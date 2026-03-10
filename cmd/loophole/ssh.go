package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/semistrict/loophole"
)

func sshCmd() *cobra.Command {
	var urlFlag string
	var volumeFlag string

	cmd := &cobra.Command{
		Use:   "ssh",
		Short: "Open an interactive shell on a remote volume",
		Long:  "Connects to a remote loophole daemon via WebSocket and opens a raw PTY session.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Resolve base URL from flag or profile.
			baseURL := urlFlag
			if baseURL == "" {
				dir := loophole.DefaultDir()
				inst, err := resolveProfile(dir)
				if err != nil {
					return err
				}
				baseURL = inst.DaemonURL
			}
			if baseURL == "" {
				return fmt.Errorf("--url is required (or set daemon_url in profile)")
			}
			if volumeFlag == "" {
				return fmt.Errorf("--volume is required")
			}

			// Get terminal size.
			fd := int(os.Stdin.Fd())
			if !term.IsTerminal(fd) {
				return fmt.Errorf("stdin is not a terminal")
			}
			cols, rows, err := term.GetSize(fd)
			if err != nil {
				return fmt.Errorf("get terminal size: %w", err)
			}

			// Build WebSocket URL: /v/{volume}/sandbox/shell?mode=raw&cols=N&rows=N
			wsURL := fmt.Sprintf("%s/v/%s/sandbox/shell?mode=raw&cols=%d&rows=%d",
				baseURL, volumeFlag, cols, rows)
			wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
			wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

			dialer := websocket.Dialer{}
			conn, _, err := dialer.DialContext(cmd.Context(), wsURL, nil)
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer func() { _ = conn.Close() }()

			// Put terminal in raw mode.
			oldState, err := term.MakeRaw(fd)
			if err != nil {
				return fmt.Errorf("make raw: %w", err)
			}
			defer func() { _ = term.Restore(fd, oldState) }()

			var wsMu sync.Mutex // protects concurrent WS writes

			// Handle SIGWINCH for terminal resize.
			sigwinch := make(chan os.Signal, 1)
			signal.Notify(sigwinch, syscall.SIGWINCH)
			defer signal.Stop(sigwinch)

			go func() {
				for range sigwinch {
					c, r, err := term.GetSize(fd)
					if err != nil {
						continue
					}
					data, _ := json.Marshal(map[string]int{"cols": c, "rows": r})
					wsMu.Lock()
					_ = conn.WriteMessage(websocket.TextMessage, data)
					wsMu.Unlock()
				}
			}()

			// stdin → WS (binary frames).
			done := make(chan struct{})
			go func() {
				buf := make([]byte, 32*1024)
				for {
					n, err := os.Stdin.Read(buf)
					if n > 0 {
						wsMu.Lock()
						wErr := conn.WriteMessage(websocket.BinaryMessage, buf[:n])
						wsMu.Unlock()
						if wErr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()

			// WS → stdout.
			for {
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					close(done)
					return nil // connection closed
				}
				_ = done // keep done reachable
				switch msgType {
				case websocket.BinaryMessage:
					_, _ = os.Stdout.Write(data)
				case websocket.TextMessage:
					var msg struct{ Type string }
					if json.Unmarshal(data, &msg) == nil && msg.Type == "closed" {
						close(done)
						return nil
					}
				}
			}
		},
	}

	cmd.Flags().StringVar(&urlFlag, "url", "", "Remote daemon base URL (e.g. https://cf-demo.ramon3525.workers.dev)")
	cmd.Flags().StringVar(&volumeFlag, "volume", "", "Volume name to connect to")

	return cmd
}
