package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// RemoteClient talks to a remote loophole daemon over HTTPS/WSS,
// routed through a CF VolumeActor at {baseURL}/v/{volume}/.
type RemoteClient struct {
	baseURL string // e.g. https://cf-demo.example.com/v/sandbox-1
	http    *http.Client
}

// NewRemoteClient creates a remote client for the given volume URL.
// baseURL should be like "https://cf-demo.example.com/v/sandbox-1".
func NewRemoteClient(baseURL string) *RemoteClient {
	return &RemoteClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{},
	}
}

// CreatePTY creates a PTY session on the remote daemon.
func (rc *RemoteClient) CreatePTY(ctx context.Context, id, volume string, cols, rows uint16) error {
	body, err := json.Marshal(map[string]any{
		"id":     id,
		"volume": volume,
		"cols":   cols,
		"rows":   rows,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", rc.baseURL+"/sandbox/pty", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := rc.http.Do(req)
	if err != nil {
		return fmt.Errorf("create PTY: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		var errResp struct{ Error string }
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return fmt.Errorf("%s", errResp.Error)
		}
		return fmt.Errorf("create PTY: %s", data)
	}
	return nil
}

// ConnectPTYRaw opens a raw-mode WebSocket to the PTY session.
// Binary frames = stdout, text frames = control messages (e.g. "closed").
func (rc *RemoteClient) ConnectPTYRaw(ctx context.Context, sessionID string) (*websocket.Conn, error) {
	wsURL := rc.baseURL + "/sandbox/pty/" + sessionID + "/ws?mode=raw"
	// Convert https:// to wss://, http:// to ws://
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	dialer := websocket.Dialer{}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("connect PTY raw: %w", err)
	}
	return conn, nil
}

// KillPTY kills a remote PTY session.
func (rc *RemoteClient) KillPTY(ctx context.Context, sessionID string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", rc.baseURL+"/sandbox/pty/"+sessionID, nil)
	if err != nil {
		return err
	}
	resp, err := rc.http.Do(req)
	if err != nil {
		return fmt.Errorf("kill PTY: %w", err)
	}
	_ = resp.Body.Close()
	return nil
}
