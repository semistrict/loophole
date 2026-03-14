package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

type remoteSandboxRecord struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	State  string `json:"state"`
	Source struct {
		Kind   string `json:"kind"`
		Volume string `json:"volume"`
	} `json:"source"`
}

func sshCmd() *cobra.Command {
	var urlFlag string
	var volumeFlag string
	var secretFlag string
	var cwdFlag string

	cmd := &cobra.Command{
		Use:   "ssh",
		Short: "Open an interactive shell on a remote volume",
		Long:  "Creates or reuses a remote sandbox for the volume and connects a raw terminal over WebSocket.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := strings.TrimRight(urlFlag, "/")
			if baseURL == "" {
				dir := loophole.DefaultDir()
				inst, err := resolveProfile(dir)
				if err != nil {
					return err
				}
				baseURL = strings.TrimRight(inst.DaemonURL, "/")
			}
			if baseURL == "" {
				return fmt.Errorf("--url is required (or set daemon_url in profile)")
			}
			if volumeFlag == "" {
				return fmt.Errorf("--volume is required")
			}
			secret := secretFlag
			if secret == "" {
				secret = os.Getenv("CONTROL_SECRET")
			}

			fd := int(os.Stdin.Fd())
			if !term.IsTerminal(fd) {
				return fmt.Errorf("stdin is not a terminal")
			}
			cols, rows, err := term.GetSize(fd)
			if err != nil {
				return fmt.Errorf("get terminal size: %w", err)
			}

			sandbox, err := ensureRemoteSandbox(cmd.Context(), baseURL, volumeFlag, secret)
			if err != nil {
				return err
			}

			wsURL, err := buildSSHWebsocketURL(baseURL, sandbox.ID, rows, cols, cwdFlag)
			if err != nil {
				return err
			}

			headers := http.Header{}
			if secret != "" {
				headers.Set("X-Control-Secret", secret)
			}
			dialer := websocket.Dialer{}
			conn, _, err := dialer.DialContext(cmd.Context(), wsURL, headers)
			if err != nil {
				return fmt.Errorf("connect websocket: %w", err)
			}
			defer util.SafeClose(conn, "close ssh websocket")

			oldState, err := term.MakeRaw(fd)
			if err != nil {
				return fmt.Errorf("make raw: %w", err)
			}
			defer func() { _ = term.Restore(fd, oldState) }()

			var wsMu sync.Mutex
			writeJSON := func(payload any) error {
				wsMu.Lock()
				defer wsMu.Unlock()
				return conn.WriteJSON(payload)
			}
			writeBinary := func(data []byte) error {
				wsMu.Lock()
				defer wsMu.Unlock()
				return conn.WriteMessage(websocket.BinaryMessage, data)
			}

			sigwinch := make(chan os.Signal, 1)
			signal.Notify(sigwinch, syscall.SIGWINCH)
			defer signal.Stop(sigwinch)
			go func() {
				for range sigwinch {
					c, r, err := term.GetSize(fd)
					if err != nil {
						continue
					}
					_ = writeJSON(map[string]int{"cols": c, "rows": r})
				}
			}()

			go func() {
				buf := make([]byte, 32*1024)
				for {
					n, err := os.Stdin.Read(buf)
					if n > 0 {
						if writeErr := writeBinary(buf[:n]); writeErr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()

			for {
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					return nil
				}
				switch msgType {
				case websocket.BinaryMessage:
					if _, err := os.Stdout.Write(data); err != nil {
						return err
					}
				case websocket.TextMessage:
					var msg struct {
						Type     string `json:"type"`
						ExitCode int    `json:"exitCode,omitempty"`
						Error    string `json:"error,omitempty"`
					}
					if err := json.Unmarshal(data, &msg); err != nil {
						continue
					}
					if msg.Error != "" {
						_, _ = io.WriteString(os.Stderr, msg.Error+"\n")
					}
					if msg.Type == "closed" {
						if msg.ExitCode != 0 {
							return &exitCodeError{code: msg.ExitCode}
						}
						return nil
					}
				}
			}
		},
	}

	cmd.Flags().StringVar(&urlFlag, "url", "", "Remote worker base URL (e.g. https://cf-demo-4.ramon3525.workers.dev)")
	cmd.Flags().StringVar(&volumeFlag, "volume", "", "Volume name to connect to")
	cmd.Flags().StringVar(&secretFlag, "secret", "", "Shared control secret (defaults to CONTROL_SECRET env var)")
	cmd.Flags().StringVar(&cwdFlag, "cwd", "", "Initial working directory inside the sandbox")
	return cmd
}

func ensureRemoteSandbox(ctx context.Context, baseURL, volume, secret string) (remoteSandboxRecord, error) {
	sandboxes, err := listRemoteSandboxes(ctx, baseURL, secret)
	if err != nil {
		return remoteSandboxRecord{}, err
	}
	for _, sandbox := range sandboxes {
		if sandbox.Name == volume || sandbox.Source.Volume == volume {
			if sandbox.State != "" && sandbox.State != "running" {
				if err := startRemoteSandbox(ctx, baseURL, sandbox.ID, secret); err != nil {
					return remoteSandboxRecord{}, err
				}
			}
			return sandbox, nil
		}
	}
	return createRemoteSandbox(ctx, baseURL, volume, secret)
}

func listRemoteSandboxes(ctx context.Context, baseURL, secret string) ([]remoteSandboxRecord, error) {
	req, err := newRemoteJSONRequest(ctx, http.MethodGet, baseURL+"/api/sandbox", secret, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(resp.Body, "close sandbox list response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list sandboxes: %s", strings.TrimSpace(string(data)))
	}
	var sandboxes []remoteSandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&sandboxes); err != nil {
		return nil, err
	}
	return sandboxes, nil
}

func createRemoteSandbox(ctx context.Context, baseURL, volume, secret string) (remoteSandboxRecord, error) {
	req, err := newRemoteJSONRequest(ctx, http.MethodPost, baseURL+"/api/sandbox", secret, map[string]string{"name": volume})
	if err != nil {
		return remoteSandboxRecord{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return remoteSandboxRecord{}, err
	}
	defer util.SafeClose(resp.Body, "close sandbox create response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return remoteSandboxRecord{}, fmt.Errorf("create sandbox: %s", strings.TrimSpace(string(data)))
	}
	var sandbox remoteSandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&sandbox); err != nil {
		return remoteSandboxRecord{}, err
	}
	return sandbox, nil
}

func startRemoteSandbox(ctx context.Context, baseURL, sandboxID, secret string) error {
	req, err := newRemoteJSONRequest(ctx, http.MethodPost, baseURL+"/api/sandbox/"+url.PathEscape(sandboxID)+"/start", secret, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close sandbox start response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("start sandbox: %s", strings.TrimSpace(string(data)))
	}
	return nil
}

func newRemoteJSONRequest(ctx context.Context, method, requestURL, secret string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if secret != "" {
		req.Header.Set("X-Control-Secret", secret)
	}
	return req, nil
}

func buildSSHWebsocketURL(baseURL, sandboxID string, rows, cols int, cwd string) (string, error) {
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
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/toolbox/" + url.PathEscape(sandboxID) + "/toolbox/process/terminal"
	q := parsed.Query()
	if cwd != "" {
		q.Set("cwd", cwd)
	}
	if rows > 0 {
		q.Set("rows", fmt.Sprintf("%d", rows))
	}
	if cols > 0 {
		q.Set("cols", fmt.Sprintf("%d", cols))
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}
