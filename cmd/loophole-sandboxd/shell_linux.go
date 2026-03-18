//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gorilla/websocket"
	"golang.org/x/term"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/internal/util"
)

type shellExitError struct {
	code int
}

func (e shellExitError) Error() string {
	return fmt.Sprintf("process exited with status %d", e.code)
}

func (e shellExitError) ExitCode() int {
	return e.code
}

type shellStreamMessage struct {
	Stdin string `json:"stdin,omitempty"`
	Rows  int    `json:"rows,omitempty"`
	Cols  int    `json:"cols,omitempty"`
}

type shellStreamEvent struct {
	Stream string `json:"stream"`
	Data   string `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

func runShell(args []string) error {
	fs := flag.NewFlagSet("shell", flag.ContinueOnError)
	var socketPath string
	fs.StringVar(&socketPath, "socket-path", "", "sandboxd socket path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: loophole-sandboxd shell [--socket-path path] <sandbox-id> [command...]")
	}

	sandboxID := rest[0]
	command := rest[1:]
	if len(command) == 0 {
		command = []string{"/bin/sh", "-i"}
	}

	socketPath = firstNonEmpty(socketPath, env.DefaultDir().SandboxdSocket())
	rows, cols := termSize()

	req := processCreateRequest{
		Argv:       command,
		Background: true,
		TTY:        true,
		Rows:       rows,
		Cols:       cols,
	}
	processID, err := createShellProcess(socketPath, sandboxID, req)
	if err != nil {
		return err
	}
	return attachShell(socketPath, sandboxID, processID)
}

func createShellProcess(socketPath, sandboxID string, req processCreateRequest) (string, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: unixSocketDialContext(socketPath),
		},
	}
	request, err := http.NewRequest(http.MethodPost, "http://sandboxd/v1/sandboxes/"+sandboxID+"/processes", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer util.SafeClose(resp.Body, "close shell create response body")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("create shell session: %s", bytes.TrimSpace(body))
	}
	var record struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &record); err != nil {
		return "", err
	}
	if record.ID == "" {
		return "", fmt.Errorf("create shell session: missing process id")
	}
	return record.ID, nil
}

func attachShell(socketPath, sandboxID, processID string) error {
	dialer := websocket.Dialer{
		NetDialContext: unixSocketDialContext(socketPath),
	}
	conn, resp, err := dialer.Dial("ws://sandboxd/v1/sandboxes/"+sandboxID+"/processes/"+processID+"/stream", nil)
	if resp != nil {
		util.SafeClose(resp.Body, "close shell websocket response body")
	}
	if err != nil {
		return err
	}
	defer util.SafeClose(conn, "close shell websocket")

	restore, err := makeTerminalRaw()
	if err != nil {
		return err
	}
	defer func() {
		restore()
		_, _ = io.WriteString(os.Stdout, "\r\n")
	}()

	writeCh := make(chan shellStreamMessage, 32)
	writeErr := make(chan error, 1)
	go func() {
		for msg := range writeCh {
			if err := conn.WriteJSON(msg); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	resizeDone := make(chan struct{})
	go func() {
		defer close(resizeDone)
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGWINCH)
		defer signal.Stop(signals)
		for range signals {
			rows, cols := termSize()
			if rows > 0 && cols > 0 {
				select {
				case writeCh <- shellStreamMessage{Rows: rows, Cols: cols}:
				case <-resizeDone:
					return
				}
			}
		}
	}()

	stdinDone := make(chan struct{})
	go func() {
		defer close(stdinDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				writeCh <- shellStreamMessage{Stdin: string(buf[:n])}
			}
			if err != nil {
				close(writeCh)
				return
			}
		}
	}()

	for {
		select {
		case err := <-writeErr:
			if err != nil {
				return err
			}
		default:
		}

		var event shellStreamEvent
		if err := conn.ReadJSON(&event); err != nil {
			return err
		}
		switch event.Stream {
		case "stdout":
			if _, err := io.WriteString(os.Stdout, event.Data); err != nil {
				return err
			}
		case "stderr":
			if _, err := io.WriteString(os.Stderr, event.Data); err != nil {
				return err
			}
		case "error":
			return fmt.Errorf("shell stream: %s", event.Error)
		case "exit":
			close(writeCh)
			<-stdinDone
			<-resizeDone
			if code, err := strconv.Atoi(event.Data); err == nil && code != 0 {
				return shellExitError{code: code}
			}
			return nil
		}
	}
}

func unixSocketDialContext(socketPath string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socketPath)
	}
}

func makeTerminalRaw() (func(), error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return func() {}, nil
	}
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return nil, err
	}
	return func() {
		_ = term.Restore(int(os.Stdin.Fd()), state)
		if term.IsTerminal(int(os.Stdout.Fd())) {
			_, _ = io.WriteString(os.Stdout, "\x1b[?2004l")
		}
	}, nil
}

func termSize() (int, int) {
	fd := int(os.Stdout.Fd())
	if !term.IsTerminal(fd) {
		fd = int(os.Stdin.Fd())
	}
	if !term.IsTerminal(fd) {
		return 0, 0
	}
	cols, rows, err := term.GetSize(fd)
	if err != nil {
		return 0, 0
	}
	return rows, cols
}
