// Package client provides a Go client for the loophole daemon HTTP/UDS API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"github.com/semistrict/loophole/internal/storage"
)

// Client talks to a running loophole daemon over its Unix socket.
type Client struct {
	sock string
	http *http.Client
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

// Socket returns the Unix socket path for this client.
func (c *Client) Socket() string { return c.sock }

// Flush flushes the managed volume (FS mode).
func (c *Client) Flush(ctx context.Context) error {
	_, err := c.post(ctx, "/flush", nil)
	return err
}

// DeviceFlush flushes the managed volume (device mode).
func (c *Client) DeviceFlush(ctx context.Context) error {
	_, err := c.post(ctx, "/device/flush", nil)
	return err
}

// BreakLease clears a lease on a volume. Returns true if the holder
// released gracefully, false if it timed out and was force-cleared.
func (c *Client) BreakLease(ctx context.Context, volume string, force bool) (graceful bool, err error) {
	resp, err := c.post(ctx, "/break-lease", map[string]any{
		"volume": volume,
		"force":  force,
	})
	if err != nil {
		return false, err
	}
	var result struct{ Graceful bool }
	if err := json.Unmarshal(resp, &result); err != nil {
		return false, fmt.Errorf("decode break-lease response: %w", err)
	}
	return result.Graceful, nil
}

// Checkpoint creates a checkpoint and returns the checkpoint ID (timestamp).
func (c *Client) Checkpoint(ctx context.Context) (string, error) {
	resp, err := c.post(ctx, "/checkpoint", nil)
	if err != nil {
		return "", err
	}
	var result struct{ Checkpoint string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}
	return result.Checkpoint, nil
}

// DeviceCheckpoint creates a checkpoint at the device level.
func (c *Client) DeviceCheckpoint(ctx context.Context) (string, error) {
	resp, err := c.post(ctx, "/device/checkpoint", nil)
	if err != nil {
		return "", err
	}
	var result struct{ Checkpoint string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}
	return result.Checkpoint, nil
}

// DeviceDDWriteExisting writes a raw disk image into an already-open volume.
func (c *Client) DeviceDDWriteExisting(ctx context.Context, volume string, body io.Reader, progress io.Writer) error {
	const chunkSize = storage.BlockSize
	buf := make([]byte, chunkSize)
	var offset uint64

	for {
		n, readErr := io.ReadFull(body, buf)
		if n > 0 {
			if err := c.ddWriteBlock(ctx, volume, offset, buf[:n]); err != nil {
				return err
			}
			offset += uint64(n)
			if progress != nil && offset%(64<<20) == 0 {
				_, _ = fmt.Fprintf(progress, "  %d MiB written\n", offset>>20)
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read input: %w", readErr)
		}
	}

	if progress != nil {
		_, _ = fmt.Fprintf(progress, "  %d MiB written\n", offset>>20)
	}

	if _, err := c.post(ctx, "/device/dd/finalize?volume="+volume); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}
	return nil
}

func (c *Client) ddWriteBlock(ctx context.Context, volume string, offset uint64, data []byte) error {
	path := fmt.Sprintf("/device/dd/write?volume=%s&offset=%d", volume, offset)
	req, err := http.NewRequestWithContext(ctx, "POST", "http://loophole"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("dd write at offset %d: %w", offset, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("dd write at offset %d: status %d: %s", offset, resp.StatusCode, body)
	}
	return nil
}

// DeviceDDSize returns the volume size in bytes via the server.
func (c *Client) DeviceDDSize(ctx context.Context) (uint64, error) {
	resp, err := c.get(ctx, "/device/dd/size")
	if err != nil {
		return 0, err
	}
	var result struct{ Size uint64 }
	if err := json.Unmarshal(resp, &result); err != nil {
		return 0, err
	}
	return result.Size, nil
}

// DeviceDDRead reads a volume's raw block data in BlockSize chunks and writes
// it to dst. The volume must already be open.
func (c *Client) DeviceDDRead(ctx context.Context, dst io.Writer, progress io.Writer) error {
	size, err := c.DeviceDDSize(ctx)
	if err != nil {
		return fmt.Errorf("get volume size: %w", err)
	}

	const chunkSize = storage.BlockSize
	var offset uint64

	for offset < size {
		readSize := min(uint64(chunkSize), size-offset)

		data, err := c.ddReadBlock(ctx, offset, readSize)
		if err != nil {
			return err
		}
		if _, err := dst.Write(data); err != nil {
			return fmt.Errorf("write output at offset %d: %w", offset, err)
		}
		offset += uint64(len(data))
		if progress != nil && offset%(64<<20) == 0 {
			_, _ = fmt.Fprintf(progress, "  %d MiB / %d MiB\n", offset>>20, size>>20)
		}
	}

	if progress != nil {
		_, _ = fmt.Fprintf(progress, "  %d MiB read\n", offset>>20)
	}
	return nil
}

func (c *Client) ddReadBlock(ctx context.Context, offset, size uint64) ([]byte, error) {
	path := fmt.Sprintf("/device/dd/read?offset=%d&size=%d", offset, size)
	req, err := http.NewRequestWithContext(ctx, "GET", "http://loophole"+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dd read at offset %d: %w", offset, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("dd read at offset %d: %w", offset, err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("dd read at offset %d: status %d: %s", offset, resp.StatusCode, data)
	}
	return data, nil
}

// VolumeDebugInfo returns debug information about an open volume.
func (c *Client) VolumeDebugInfo(ctx context.Context, volume string) (storage.VolumeDebugInfo, error) {
	resp, err := c.get(ctx, "/debug/volume?volume="+volume)
	if err != nil {
		return storage.VolumeDebugInfo{}, err
	}
	var info storage.VolumeDebugInfo
	if err := json.Unmarshal(resp, &info); err != nil {
		return storage.VolumeDebugInfo{}, err
	}
	return info, nil
}

// Shutdown asks the daemon to shut down gracefully.
func (c *Client) Shutdown(ctx context.Context) error {
	_, err := c.post(ctx, "/shutdown")
	return err
}

// ShutdownWait blocks until the daemon has finished flushing and releasing leases.
func (c *Client) ShutdownWait(ctx context.Context) error {
	_, err := c.get(ctx, "/shutdown/wait")
	return err
}

// StatusResponse holds the daemon status JSON.
type StatusResponse struct {
	StoreURL   string            `json:"store_url"`
	VolsetID   string            `json:"volset_id"`
	Mode       string            `json:"mode"`
	Socket     string            `json:"socket"`
	Fuse       string            `json:"fuse"`
	Volume     string            `json:"volume"`
	Mountpoint string            `json:"mountpoint"`
	Device     string            `json:"device"`
	Cache      string            `json:"cache"`
	Log        string            `json:"log"`
	State      string            `json:"state"`
	Volumes    []string          `json:"volumes"`
	Mounts     map[string]string `json:"mounts"`
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
	resp, err := c.get(ctx, "/status")
	if err != nil {
		return nil, err
	}
	var status StatusResponse
	if err := json.Unmarshal(resp, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// --- internal ---

func (c *Client) post(ctx context.Context, path string, args ...any) (json.RawMessage, error) {
	return c.rpc(ctx, "POST", path, args...)
}

func (c *Client) get(ctx context.Context, path string, args ...any) (json.RawMessage, error) {
	return c.rpc(ctx, "GET", path, args...)
}

func (c *Client) rpc(ctx context.Context, method, path string, args ...any) (json.RawMessage, error) {
	var body any
	switch len(args) {
	case 0:
		// no body
	case 1:
		body = args[0]
	default:
		m := make(map[string]string, len(args)/2)
		for i := 0; i+1 < len(args); i += 2 {
			m[args[i].(string)] = args[i+1].(string)
		}
		body = m
	}
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
