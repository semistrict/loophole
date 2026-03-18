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

	"github.com/semistrict/loophole/storage"
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

// FlushVolume flushes a named volume.
func (c *Client) FlushVolume(ctx context.Context, volume string) error {
	_, err := c.post(ctx, "/flush", "volume", volume)
	return err
}

// --- RPC methods ---

// CreateParams is an alias for storage.CreateParams.
type CreateParams = storage.CreateParams

func (c *Client) Create(ctx context.Context, p CreateParams) error {
	_, err := c.post(ctx, "/create", p)
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

// Delete removes a volume.
func (c *Client) Delete(ctx context.Context, volume string) error {
	_, err := c.post(ctx, "/delete", "volume", volume)
	return err
}

// Mount mounts an existing volume at mountpoint.
func (c *Client) Mount(ctx context.Context, volume, mountpoint string) error {
	_, err := c.post(ctx, "/mount", "volume", volume, "mountpoint", mountpoint)
	return err
}

// Clone creates an unmounted clone of a mounted volume or checkpoint.
type CloneParams struct {
	Mountpoint string `json:"mountpoint,omitempty"`
	Volume     string `json:"volume,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"`
	Clone      string `json:"clone"`
}

func (c *Client) Clone(ctx context.Context, p CloneParams) error {
	_, err := c.post(ctx, "/clone", p)
	return err
}

// Checkpoint creates a checkpoint and returns the checkpoint ID (timestamp).
func (c *Client) Checkpoint(ctx context.Context, mountpoint string) (string, error) {
	resp, err := c.post(ctx, "/checkpoint", "mountpoint", mountpoint)
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
func (c *Client) DeviceCheckpoint(ctx context.Context, volume string) (string, error) {
	resp, err := c.post(ctx, "/device/checkpoint", "volume", volume)
	if err != nil {
		return "", err
	}
	var result struct{ Checkpoint string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}
	return result.Checkpoint, nil
}

// ListCheckpoints returns all checkpoints for a volume.
func (c *Client) ListCheckpoints(ctx context.Context, volume string) ([]storage.CheckpointInfo, error) {
	resp, err := c.get(ctx, "/checkpoints?volume="+volume)
	if err != nil {
		return nil, err
	}
	var result struct{ Checkpoints []storage.CheckpointInfo }
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result.Checkpoints, nil
}

// DeviceAttach opens a volume and returns the FUSE device path.
func (c *Client) DeviceAttach(ctx context.Context, volume string) (string, error) {
	resp, err := c.post(ctx, "/device/attach", "volume", volume)
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
	_, err := c.post(ctx, "/device/detach", "volume", volume)
	return err
}

type DeviceCloneParams struct {
	Volume     string `json:"volume"`
	Checkpoint string `json:"checkpoint,omitempty"`
	Clone      string `json:"clone"`
}

// DeviceClone creates an unattached clone of a volume or checkpoint.
func (c *Client) DeviceClone(ctx context.Context, p DeviceCloneParams) error {
	_, err := c.post(ctx, "/device/clone", p)
	return err
}

// DeviceDD imports a raw disk image into a new volume via the daemon.
// It first creates the volume with the given params (NoFormat is forced true),
// then reads from body in BlockSize chunks and sends each as a separate
// request so the server can write them directly to L2.
func (c *Client) DeviceDD(ctx context.Context, p CreateParams, body io.Reader, progress io.Writer) error {
	p.NoFormat = true
	if err := c.Create(ctx, p); err != nil {
		return fmt.Errorf("create volume: %w", err)
	}

	const chunkSize = storage.BlockSize
	buf := make([]byte, chunkSize)
	var offset uint64

	for {
		n, readErr := io.ReadFull(body, buf)
		if n > 0 {
			if err := c.ddWriteBlock(ctx, p.Volume, offset, buf[:n]); err != nil {
				return err
			}
			offset += uint64(n)
			if progress != nil && offset%(64<<20) == 0 {
				_, _ = fmt.Fprintf(progress, "  %d MiB / %d MiB\n", offset>>20, p.Size>>20)
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

	// Flush and release the volume ref so it can be opened by others.
	if _, err := c.post(ctx, "/device/dd/finalize?volume="+p.Volume); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}
	return nil
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

// DeviceDDRead reads a volume's raw block data in BlockSize chunks and writes
// it to dst. The volume must already be open. This is the read counterpart of
// DeviceDD (write). The caller must open the volume first (e.g. via Create or
// by having previously written to it).
func (c *Client) DeviceDDRead(ctx context.Context, volume string, size uint64, dst io.Writer, progress io.Writer) error {
	const chunkSize = storage.BlockSize
	var offset uint64

	for offset < size {
		readSize := min(uint64(chunkSize), size-offset)

		data, err := c.ddReadBlock(ctx, volume, offset, readSize)
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

func (c *Client) ddReadBlock(ctx context.Context, volume string, offset, size uint64) ([]byte, error) {
	path := fmt.Sprintf("/device/dd/read?volume=%s&offset=%d&size=%d", volume, offset, size)
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

// VolumeInfo returns metadata about a volume (does not need to be open).
func (c *Client) VolumeInfo(ctx context.Context, volume string) (storage.VolumeInfo, error) {
	resp, err := c.get(ctx, "/volume-info?volume="+volume)
	if err != nil {
		return storage.VolumeInfo{}, err
	}
	var info storage.VolumeInfo
	if err := json.Unmarshal(resp, &info); err != nil {
		return storage.VolumeInfo{}, err
	}
	return info, nil
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

// ListVolumes returns all volume names from the store (not just open ones).
func (c *Client) ListVolumes(ctx context.Context) ([]string, error) {
	resp, err := c.get(ctx, "/volumes")
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
	_, err := c.post(ctx, "/shutdown")
	return err
}

// ShutdownWait blocks until the daemon has finished flushing and releasing leases.
func (c *Client) ShutdownWait(ctx context.Context) error {
	_, err := c.get(ctx, "/shutdown/wait")
	return err
}

type StatusResponse struct {
	S3         string            `json:"s3"`
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
