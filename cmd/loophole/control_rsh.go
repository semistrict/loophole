package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

const (
	controlRshStdout byte = 1
	controlRshStderr byte = 2
)

type controlRshOutput struct {
	Type     string `json:"type"`
	ExitCode int    `json:"exitCode,omitempty"`
	Error    string `json:"error,omitempty"`
}

type controlRshInput struct {
	Type   string `json:"type"`
	Signal string `json:"signal,omitempty"`
}

func controlRshCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "control-rsh --url <base-url> [--container <id>] <host> <command> [args...]",
		Short:              "Remote-shell helper for control-plane WebSocket exec",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL, container, cwd, rest, err := parseControlRshArgs(args)
			if err != nil {
				return err
			}
			if baseURL == "" {
				return fmt.Errorf("--url is required")
			}
			if len(rest) < 2 {
				return fmt.Errorf("usage: control-rsh --url <base-url> [--container <id>] <host> <command> [args...]")
			}

			host := rest[0]
			if container == "" {
				container = host
			}
			remoteArgv := rest[1:]

			wsURL, err := buildControlWSURL(baseURL, container, cwd, remoteArgv)
			if err != nil {
				return err
			}

			dialer := websocket.Dialer{}
			conn, _, err := dialer.DialContext(cmd.Context(), wsURL, nil)
			if err != nil {
				return fmt.Errorf("connect websocket: %w", err)
			}
			defer func() { _ = conn.Close() }()

			var wsMu sync.Mutex
			writeJSON := func(payload controlRshInput) error {
				wsMu.Lock()
				defer wsMu.Unlock()
				return conn.WriteJSON(payload)
			}
			writeBinary := func(data []byte) error {
				wsMu.Lock()
				defer wsMu.Unlock()
				return conn.WriteMessage(websocket.BinaryMessage, data)
			}

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigCh)
			go func() {
				for sig := range sigCh {
					_ = writeJSON(controlRshInput{Type: "signal", Signal: signalName(sig)})
				}
			}()

			go func() {
				buf := make([]byte, 32*1024)
				for {
					n, err := os.Stdin.Read(buf)
					if n > 0 {
						if writeErr := writeBinary(buf[:n]); writeErr != nil {
							_ = conn.Close()
							return
						}
					}
					if err != nil {
						if errorsIsEOF(err) {
							_ = writeJSON(controlRshInput{Type: "stdinClose"})
						}
						return
					}
				}
			}()

			for {
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					return err
				}

				switch msgType {
				case websocket.BinaryMessage:
					if len(data) == 0 {
						continue
					}
					switch data[0] {
					case controlRshStdout:
						if _, err := os.Stdout.Write(data[1:]); err != nil {
							return err
						}
					case controlRshStderr:
						if _, err := os.Stderr.Write(data[1:]); err != nil {
							return err
						}
					}
				case websocket.TextMessage:
					var msg controlRshOutput
					if err := json.Unmarshal(data, &msg); err != nil {
						return err
					}
					if msg.Error != "" {
						_, _ = io.WriteString(os.Stderr, msg.Error+"\n")
					}
					if msg.Type == "exit" {
						if msg.ExitCode != 0 {
							return &exitCodeError{code: msg.ExitCode}
						}
						return nil
					}
				}
			}
		},
	}

	return cmd
}

func parseControlRshArgs(args []string) (baseURL string, container string, cwd string, rest []string, err error) {
	i := 0
	for i < len(args) {
		arg := args[i]
		if arg == "--" {
			return baseURL, container, cwd, args[i+1:], nil
		}
		if !strings.HasPrefix(arg, "--") {
			return baseURL, container, cwd, args[i:], nil
		}

		key, value, hasValue := strings.Cut(arg, "=")
		if !hasValue {
			if i+1 >= len(args) {
				return "", "", "", nil, fmt.Errorf("missing value for %s", key)
			}
			value = args[i+1]
			i++
		}

		switch key {
		case "--url":
			baseURL = value
		case "--container":
			container = value
		case "--cwd":
			cwd = value
		default:
			return "", "", "", nil, fmt.Errorf("unknown flag %s", key)
		}
		i++
	}

	return baseURL, container, cwd, nil, nil
}

func buildControlWSURL(baseURL, container, cwd string, argv []string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse --url: %w", err)
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported url scheme %q", parsed.Scheme)
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/debug/control/" + url.PathEscape(container) + "/ws-exec"
	q := parsed.Query()
	for _, arg := range argv {
		q.Add("arg", arg)
	}
	if cwd != "" {
		q.Set("cwd", cwd)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
}

func signalName(sig os.Signal) string {
	switch sig {
	case os.Interrupt:
		return "SIGINT"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return sig.String()
	}
}
