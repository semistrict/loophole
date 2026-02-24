// Package client provides a Go client for the loophole daemon HTTP/UDS API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"time"

	"github.com/semistrict/loophole"
)

// Client talks to a running loophole daemon over its Unix socket.
type Client struct {
	sock   string
	http   *http.Client
}

// New creates a client that talks to the daemon for the given instance.
// It does NOT start the daemon — call EnsureDaemon first if needed.
func New(dir loophole.Dir, inst loophole.Instance) *Client {
	return NewFromSocket(dir.Socket(inst))
}

// NewFromSocket creates a client connected to a specific socket path.
func NewFromSocket(sock string) *Client {
	return &Client{
		sock: sock,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", sock)
				},
			},
		},
	}
}

// EnsureDaemon starts the daemon if it isn't already running.
// The loopholeBin argument is the path to the loophole binary; pass ""
// to use os.Executable().
func EnsureDaemon(dir loophole.Dir, inst loophole.Instance, loopholeBin string) error {
	sock := dir.Socket(inst)
	if isSocketAlive(sock) {
		return nil
	}
	return startDaemon(dir, inst, loopholeBin)
}

// --- RPC methods ---

// Create creates a new volume and formats it with ext4.
func (c *Client) Create(ctx context.Context, volume string) error {
	_, err := c.rpc(ctx, "POST", "/create", map[string]string{
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

// DeviceMount opens a volume and returns the FUSE device path.
func (c *Client) DeviceMount(ctx context.Context, volume string) (string, error) {
	resp, err := c.rpc(ctx, "POST", "/device/mount", map[string]string{
		"volume": volume,
	})
	if err != nil {
		return "", err
	}
	var result struct{ Device string }
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("decode device/mount response: %w", err)
	}
	return result.Device, nil
}

// DeviceUnmount closes a volume device.
func (c *Client) DeviceUnmount(ctx context.Context, volume string) error {
	_, err := c.rpc(ctx, "POST", "/device/unmount", map[string]string{
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

type StatusResponse struct {
	S3      string            `json:"s3"`
	Socket  string            `json:"socket"`
	Fuse    string            `json:"fuse"`
	Volumes []string          `json:"volumes"`
	Mounts  map[string]string `json:"mounts"`
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
		return nil, fmt.Errorf("daemon not reachable at %s: %w", c.sock, err)
	}
	defer resp.Body.Close()

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
	conn.Close()
	return true
}

func startDaemon(dir loophole.Dir, inst loophole.Instance, loopholeBin string) error {
	if loopholeBin == "" {
		var err error
		loopholeBin, err = exec.LookPath("loophole")
		if err != nil {
			return fmt.Errorf("find loophole binary: %w (is loophole in PATH?)", err)
		}
	}

	cmd := exec.Command(loopholeBin, "start", inst.S3URL())
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = daemonSysProcAttr()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	cmd.Process.Release()

	sock := dir.Socket(inst)
	for range 50 {
		time.Sleep(100 * time.Millisecond)
		if isSocketAlive(sock) {
			return nil
		}
	}
	return fmt.Errorf("daemon did not start within 5s (socket: %s, log: %s)", sock, dir.Log(inst))
}
