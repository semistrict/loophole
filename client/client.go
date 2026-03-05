// Package client provides a Go client for the loophole daemon HTTP/UDS API.
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/gorilla/websocket"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/streammux"
)

// Client talks to a running loophole daemon over its Unix socket.
type Client struct {
	Dir     loophole.Dir
	Inst    loophole.Instance
	Bin     string // path to loophole binary; empty = find in PATH
	Sudo    bool   // wrap daemon start with sudo
	Profile string // profile name; non-empty = pass -p to spawned daemon

	sock string
	http *http.Client
}

// New creates a client for the given instance.
func New(dir loophole.Dir, inst loophole.Instance) *Client {
	sock := dir.Socket(inst.ProfileName)
	return &Client{
		Dir:  dir,
		Inst: inst,
		Sudo: true,
		sock: sock,
		http: httpClient(sock),
	}
}

// NewFromSocket creates a client connected to a specific socket path.
func NewFromSocket(sock string) *Client {
	return &Client{
		sock: sock,
		http: httpClient(sock),
	}
}

func httpClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}
}

// EnsureDaemon starts the daemon if it isn't already running.
func (c *Client) EnsureDaemon() error {
	if isSocketAlive(c.sock) {
		return nil
	}
	return c.startDaemon()
}

// --- RPC methods ---

// Create creates a new volume and formats it.
// Size is the volume size in bytes; 0 means use the default.
// Type is the volume type ("ext4" or "juicefs"); empty defaults to "ext4".
type CreateParams struct {
	Volume   string `json:"volume"`
	Size     uint64 `json:"size,omitempty"`
	NoFormat bool   `json:"no_format,omitempty"`
	Type     string `json:"type,omitempty"`
}

func (c *Client) Create(ctx context.Context, p CreateParams) error {
	_, err := c.rpc(ctx, "POST", "/create", p)
	return err
}

// Delete removes a volume.
func (c *Client) Delete(ctx context.Context, volume string) error {
	_, err := c.rpc(ctx, "POST", "/delete", map[string]string{
		"volume": volume,
	})
	return err
}

// Mount mounts an existing volume at mountpoint.
func (c *Client) Mount(ctx context.Context, volume, mountpoint string) error {
	_, err := c.rpc(ctx, "POST", "/mount", map[string]string{
		"volume":     volume,
		"mountpoint": mountpoint,
	})
	return err
}

// Unmount unmounts the filesystem at mountpoint.
func (c *Client) Unmount(ctx context.Context, mountpoint string) error {
	_, err := c.rpc(ctx, "POST", "/unmount", map[string]string{
		"mountpoint": mountpoint,
	})
	return err
}

// Clone freezes the source, clones it, and mounts the clone.
func (c *Client) Clone(ctx context.Context, srcMountpoint, cloneName, cloneMountpoint string) error {
	_, err := c.rpc(ctx, "POST", "/clone", map[string]string{
		"mountpoint":       srcMountpoint,
		"clone":            cloneName,
		"clone_mountpoint": cloneMountpoint,
	})
	return err
}

// Snapshot creates a snapshot of the volume at mountpoint.
func (c *Client) Snapshot(ctx context.Context, mountpoint, name string) error {
	_, err := c.rpc(ctx, "POST", "/snapshot", map[string]string{
		"mountpoint": mountpoint,
		"name":       name,
	})
	return err
}

// DeviceAttach opens a volume and returns the FUSE device path.
func (c *Client) DeviceAttach(ctx context.Context, volume string) (string, error) {
	resp, err := c.rpc(ctx, "POST", "/device/attach", map[string]string{
		"volume": volume,
	})
	if err != nil {
		return "", err
	}
	var result struct{ Device string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("decode device/attach response: %w", err)
	}
	return result.Device, nil
}

// DeviceDetach closes a volume device.
func (c *Client) DeviceDetach(ctx context.Context, volume string) error {
	_, err := c.rpc(ctx, "POST", "/device/detach", map[string]string{
		"volume": volume,
	})
	return err
}

// DeviceSnapshot creates a snapshot at the device level.
func (c *Client) DeviceSnapshot(ctx context.Context, volume, snapshot string) error {
	_, err := c.rpc(ctx, "POST", "/device/snapshot", map[string]string{
		"volume":   volume,
		"snapshot": snapshot,
	})
	return err
}

// DeviceClone clones a volume and returns the device path.
func (c *Client) DeviceClone(ctx context.Context, volume, clone string) (string, error) {
	resp, err := c.rpc(ctx, "POST", "/device/clone", map[string]string{
		"volume": volume,
		"clone":  clone,
	})
	if err != nil {
		return "", err
	}
	var result struct{ Device string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("decode device/clone response: %w", err)
	}
	return result.Device, nil
}

// FileResult holds the websocket connection from a file command.
// Callers must call Demux to consume stdout/stderr and get the exit code,
// then Close to release the connection.
type FileResult struct {
	conn *websocket.Conn
	mux  *streammux.Reader
}

// Demux reads all frames, writing stdout/stderr to the given writers.
// Returns the exit code from the daemon.
func (fr *FileResult) Demux(stdout, stderr io.Writer) (int32, error) {
	return fr.mux.Demux(stdout, stderr)
}

// Close releases the underlying websocket connection.
func (fr *FileResult) Close() error {
	return fr.conn.Close()
}

// File runs a file command (cat, ls, tar, etc.) and returns a FileResult
// for streaming stdout/stderr. The argv is sent as-is to the server.
// The server may request files from the client via the bidirectional protocol.
func (c *Client) File(ctx context.Context, argv []string) (*FileResult, error) {
	dialer := websocket.Dialer{
		NetDialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", c.sock)
		},
	}

	conn, _, err := dialer.DialContext(ctx, "ws://loophole/file", nil)
	if err != nil {
		return nil, fmt.Errorf("no daemon running (socket %s): %w", c.sock, err)
	}

	// Send argv as first text message.
	argvJSON, _ := json.Marshal(argv)
	if err := conn.WriteMessage(websocket.TextMessage, argvJSON); err != nil {
		_ = conn.Close()
		return nil, err
	}

	// The connection is now bidirectional:
	// - Server sends binary messages: streammux frames (stdout/stderr/exit)
	// - Server sends text messages: read requests {"read": "path", "id": N}
	// - Client sends binary messages: file data with 4-byte uint32 id prefix

	// We need a goroutine to read all messages and route them:
	// binary → streammux pipe, text → file request handler.
	smuxR, smuxW := io.Pipe()

	go func() {
		defer func() { _ = smuxW.Close() }()
		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch msgType {
			case websocket.BinaryMessage:
				// Streammux frame from server.
				if _, err := smuxW.Write(msg); err != nil {
					return
				}
			case websocket.TextMessage:
				// Read request from server.
				var req struct {
					Read string `json:"read"`
					ID   uint32 `json:"id"`
				}
				if json.Unmarshal(msg, &req) != nil {
					continue
				}
				go c.serveFileRead(conn, req.Read, req.ID)
			}
		}
	}()

	return &FileResult{
		conn: conn,
		mux:  streammux.NewReader(smuxR),
	}, nil
}

// serveFileRead opens a file (or stdin for "-") and streams it to the server
// as binary websocket messages with a 4-byte id prefix.
func (c *Client) serveFileRead(conn *websocket.Conn, path string, id uint32) {
	var r io.ReadCloser
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			// Send EOF immediately on error.
			idBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(idBuf, id)
			_ = conn.WriteMessage(websocket.BinaryMessage, idBuf)
			return
		}
		r = f
	}
	defer func() {
		if path != "-" {
			_ = r.Close()
		}
	}()

	buf := make([]byte, 64*1024+4)
	binary.BigEndian.PutUint32(buf[:4], id)

	for {
		n, err := r.Read(buf[4:])
		if n > 0 {
			if wErr := conn.WriteMessage(websocket.BinaryMessage, buf[:4+n]); wErr != nil {
				return
			}
		}
		if err != nil {
			break
		}
	}

	// Send EOF: just the 4-byte id with no data.
	_ = conn.WriteMessage(websocket.BinaryMessage, buf[:4])
}

// ListVolumes returns all volume names from the store (not just open ones).
func (c *Client) ListVolumes(ctx context.Context) ([]string, error) {
	resp, err := c.rpc(ctx, "GET", "/volumes", nil)
	if err != nil {
		return nil, err
	}
	var result struct{ Volumes []string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result.Volumes, nil
}

// Shutdown asks the daemon to shut down gracefully.
func (c *Client) Shutdown(ctx context.Context) error {
	_, err := c.rpc(ctx, "POST", "/shutdown", nil)
	return err
}

type StatusResponse struct {
	S3      string            `json:"s3"`
	Mode    string            `json:"mode"`
	Socket  string            `json:"socket"`
	NBDSock string            `json:"nbd_sock"`
	Fuse    string            `json:"fuse"`
	Volumes []string          `json:"volumes"`
	Mounts  map[string]string `json:"mounts"`
}

// Metrics returns the raw Prometheus metrics text from the daemon.
func (c *Client) Metrics(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://loophole/metrics", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("daemon not reachable at %s: %w", c.sock, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("daemon error: %s", data)
	}
	return string(data), nil
}

// Status returns the daemon status.
func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	resp, err := c.rpc(ctx, "GET", "/status", nil)
	if err != nil {
		return nil, err
	}
	var status StatusResponse
	if err := json.Unmarshal(resp, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// --- DB methods ---

// DBCreateParams are the parameters for creating a SQLite database volume.
type DBCreateParams struct {
	Volume string `json:"volume"`
	Size   uint64 `json:"size,omitempty"`
}

// DBCreate creates a new SQLite database volume.
func (c *Client) DBCreate(ctx context.Context, p DBCreateParams) error {
	_, err := c.rpc(ctx, "POST", "/db/create", p)
	return err
}

// DBSnapshot creates a snapshot of a SQLite database volume.
func (c *Client) DBSnapshot(ctx context.Context, volume, snapshot string) error {
	_, err := c.rpc(ctx, "POST", "/db/snapshot", map[string]string{
		"volume":   volume,
		"snapshot": snapshot,
	})
	return err
}

// DBBranch creates a writable branch of a SQLite database volume.
func (c *Client) DBBranch(ctx context.Context, volume, branch string) error {
	_, err := c.rpc(ctx, "POST", "/db/branch", map[string]string{
		"volume": volume,
		"branch": branch,
	})
	return err
}

// DBFlush flushes a SQLite database volume to S3.
func (c *Client) DBFlush(ctx context.Context, volume string) error {
	_, err := c.rpc(ctx, "POST", "/db/flush", map[string]string{
		"volume": volume,
	})
	return err
}

// DBList returns all SQLite database volumes.
func (c *Client) DBList(ctx context.Context) ([]string, error) {
	resp, err := c.rpc(ctx, "GET", "/db/ls", nil)
	if err != nil {
		return nil, err
	}
	var result struct{ Volumes []string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result.Volumes, nil
}

// NBDSock returns the NBD socket path from the daemon status.
func (c *Client) NBDSock(ctx context.Context) (string, error) {
	status, err := c.Status(ctx)
	if err != nil {
		return "", err
	}
	if status.NBDSock == "" {
		return "", fmt.Errorf("daemon has no NBD server running")
	}
	return status.NBDSock, nil
}

// --- internal ---

func (c *Client) rpc(ctx context.Context, method, path string, body any) (json.RawMessage, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, "http://loophole"+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("no daemon running (socket %s)", c.sock)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp struct{ Error string }
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("%s", errResp.Error)
		}
		return nil, fmt.Errorf("daemon error: %s", data)
	}

	return data, nil
}

func isSocketAlive(path string) bool {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return false
	}
	if err := conn.Close(); err != nil {
		slog.Warn("close failed", "error", err)
	}
	return true
}

func (c *Client) startDaemon() error {
	// Wait for any previous daemon to fully exit so its cleanup doesn't
	// race with our new socket (the old daemon removes the socket file
	// on shutdown, which could delete the new daemon's socket).
	dead := false
	for range 50 {
		if !isSocketAlive(c.sock) {
			dead = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !dead {
		fmt.Fprintln(os.Stderr, "warning: previous daemon still running after 5s")
	}

	bin := c.Bin
	if bin == "" {
		var err error
		bin, err = exec.LookPath("loophole")
		if err != nil {
			return fmt.Errorf("find loophole binary: %w (is loophole in PATH?)", err)
		}
	}

	// Build the start command args.
	var args []string
	if c.Profile != "" {
		args = append(args, "-p", c.Profile, "start")
	} else {
		args = append(args, "start", c.Inst.URL())
	}

	var cmd *exec.Cmd
	if c.Sudo && os.Getuid() != 0 {
		args = append([]string{"-E", bin}, args...)
		if c.Profile == "" {
			args = append(args, "--socket-mode", fmt.Sprintf("%d", 0o666))
		}
		cmd = exec.Command("sudo", args...)
	} else {
		cmd = exec.Command(bin, args...)
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = daemonSysProcAttr()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	if err := cmd.Process.Release(); err != nil {
		slog.Warn("process release failed", "error", err)
	}

	for range 300 {
		time.Sleep(100 * time.Millisecond)
		if isSocketAlive(c.sock) {
			return nil
		}
	}
	return fmt.Errorf("daemon did not start within 30s (socket: %s, log: %s)", c.sock, c.Dir.Log(c.Inst.ProfileName))
}
