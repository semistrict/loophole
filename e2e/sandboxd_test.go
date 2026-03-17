//go:build linux

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/util"
)

type sandboxdClient struct {
	http *http.Client
}

func newSandboxdClient(t *testing.T, socket string) *sandboxdClient {
	t.Helper()
	return &sandboxdClient{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socket)
				},
			},
		},
	}
}

func (c *sandboxdClient) jsonRequest(t *testing.T, method, path string, body any) []byte {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, "http://sandboxd"+path, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	require.NoError(t, err)
	defer util.SafeClose(resp.Body, "close sandboxd response body")
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 400, "sandboxd request failed: %s", string(data))
	return data
}

func (c *sandboxdClient) rawRequest(t *testing.T, method, path string, body io.Reader) []byte {
	t.Helper()
	req, err := http.NewRequest(method, "http://sandboxd"+path, body)
	require.NoError(t, err)
	resp, err := c.http.Do(req)
	require.NoError(t, err)
	defer util.SafeClose(resp.Body, "close sandboxd raw response body")
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 400, "sandboxd raw request failed: %s", string(data))
	return data
}

func (c *sandboxdClient) dialWebsocket(t *testing.T, path string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", testDir.SandboxdSocket())
		},
	}
	conn, resp, err := dialer.Dial("ws://sandboxd"+path, nil)
	if resp != nil {
		util.SafeClose(resp.Body, "close sandboxd websocket response body")
	}
	require.NoError(t, err)
	return conn
}

func startSandboxd(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat(testRunscBin); err != nil {
		t.Skipf("runsc test binary not available: %v", err)
	}
	socket := testDir.SandboxdSocket()
	logPath := filepath.Join(string(testDir), "sandboxd-e2e.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)

	cmd := exec.Command(testSandboxdBin, "-p", "test")
	cmd.Env = append(os.Environ(),
		"LOOPHOLE_HOME="+string(testDir),
		"LOOPHOLE_BIN="+testBin,
		"RUNSC_BIN="+testRunscBin,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		util.SafeClose(logFile, "close sandboxd e2e log")
	}()

	client := newSandboxdClient(t, socket)

	t.Cleanup(func() {
		req, err := http.NewRequest(http.MethodGet, "http://sandboxd/v1/sandboxes", nil)
		if err == nil {
			if resp, reqErr := client.http.Do(req); reqErr == nil {
				func() {
					defer util.SafeClose(resp.Body, "close sandbox cleanup list body")
					if resp.StatusCode < 400 {
						var sandboxes []sandboxRecord
						if decodeErr := json.NewDecoder(resp.Body).Decode(&sandboxes); decodeErr == nil {
							for _, sb := range sandboxes {
								deleteReq, newErr := http.NewRequest(http.MethodDelete, "http://sandboxd/v1/sandboxes/"+sb.ID, nil)
								if newErr != nil {
									continue
								}
								if deleteResp, deleteErr := client.http.Do(deleteReq); deleteErr == nil {
									util.SafeClose(deleteResp.Body, "close sandbox cleanup delete body")
								}
							}
						}
					}
				}()
			}
		}
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	})

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socket); err == nil {
			req, err := http.NewRequest(http.MethodGet, "http://sandboxd/v1/zygotes", nil)
			require.NoError(t, err)
			if resp, err := client.http.Do(req); err == nil {
				util.SafeClose(resp.Body, "close sandboxd readiness body")
				if resp.StatusCode < 500 {
					return socket
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sandboxd socket %s did not become ready", socket)
	return ""
}

func sandboxdLogPath() string {
	return filepath.Join(string(testDir), "sandboxd-e2e.log")
}

type sandboxRecord struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	State        string `json:"state"`
	RootfsVolume string `json:"rootfs_volume"`
	Mountpoint   string `json:"mountpoint"`
	OwnerSocket  string `json:"owner_socket"`
	OwnerMode    string `json:"owner_mode"`
}

func TestE2E_Sandboxd_CreateFromZygote(t *testing.T) {
	skipE2E(t)
	b, _ := setupBusyboxVolume(t, "sandboxd-zygote")
	require.NoError(t, b.FreezeVolume(t.Context(), "sandboxd-zygote"))

	socket := startSandboxd(t)
	client := newSandboxdClient(t, socket)

	client.jsonRequest(t, http.MethodPost, "/v1/zygotes", map[string]any{
		"name":   "busybox",
		"volume": "sandboxd-zygote",
	})

	data := client.jsonRequest(t, http.MethodPost, "/v1/sandboxes", map[string]any{
		"name": "zygote-sandbox",
		"source": map[string]any{
			"kind":   "zygote",
			"zygote": "busybox",
		},
	})
	var sb sandboxRecord
	require.NoError(t, json.Unmarshal(data, &sb))
	require.Equal(t, "running", sb.State)

	execData := client.jsonRequest(t, http.MethodPost, fmt.Sprintf("/v1/sandboxes/%s/processes", sb.ID), map[string]any{
		"argv": []string{"/bin/cat", "/marker.txt"},
	})
	var execResp struct {
		ExitCode int    `json:"exit_code"`
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
	}
	require.NoError(t, json.Unmarshal(execData, &execResp))
	require.Equal(t, 0, execResp.ExitCode)
	require.Equal(t, "chrooted\n", execResp.Stdout)
	require.Empty(t, execResp.Stderr)

	client.rawRequest(t, http.MethodPut, fmt.Sprintf("/v1/sandboxes/%s/fs/write?path=/tmp/hello.txt", sb.ID), bytes.NewBufferString("hello from sandboxd\n"))
	readBack := client.rawRequest(t, http.MethodGet, fmt.Sprintf("/v1/sandboxes/%s/fs/read?path=/tmp/hello.txt", sb.ID), nil)
	require.Equal(t, "hello from sandboxd\n", string(readBack))
}

func TestE2E_Sandboxd_CreateFromClonedVolume_KeepsRootfsReadableDuringFlush(t *testing.T) {
	skipE2E(t)
	setupBusyboxVolume(t, "sandboxd-zygote-flush")
	t.Setenv("LOOPHOLE_TEST_STORAGE2_FLUSH_THRESHOLD", "32768")

	socket := startSandboxd(t)
	client := newSandboxdClient(t, socket)

	data := client.jsonRequest(t, http.MethodPost, "/v1/sandboxes", map[string]any{
		"name": "clone-flush-sandbox",
		"source": map[string]any{
			"kind":   "volume",
			"volume": "sandboxd-zygote-flush",
			"mode":   "clone",
		},
	})
	var sb sandboxRecord
	require.NoError(t, json.Unmarshal(data, &sb))
	require.Equal(t, "running", sb.State)

	target := filepath.Join(sb.Mountpoint, "bin", "busybox")
	ownerLog := filepath.Join(string(testDir), "sandboxd", "owners", sb.RootfsVolume+".log")
	deadline := time.Now().Add(8 * time.Second)
	nextRead := time.Now()
	for time.Now().Before(deadline) {
		if _, err := os.Stat(target); err != nil {
			t.Fatalf("sandbox rootfs became unreadable: stat %s: %v\nowner log:\n%s\nsandboxd log:\n%s",
				target, err, tailFile(ownerLog, 200), tailFile(sandboxdLogPath(), 200))
		}
		if time.Now().After(nextRead) {
			execData := client.jsonRequest(t, http.MethodPost, fmt.Sprintf("/v1/sandboxes/%s/processes", sb.ID), map[string]any{
				"argv": []string{"/bin/cat", "/marker.txt"},
			})
			var execResp struct {
				ExitCode int    `json:"exit_code"`
				Stdout   string `json:"stdout"`
				Stderr   string `json:"stderr"`
			}
			require.NoError(t, json.Unmarshal(execData, &execResp))
			if execResp.ExitCode != 0 || execResp.Stdout != "chrooted\n" || execResp.Stderr != "" {
				t.Fatalf("sandbox content corrupted after clone-backed flush: exit=%d stdout=%q stderr=%q\nowner log:\n%s\nsandboxd log:\n%s",
					execResp.ExitCode, execResp.Stdout, execResp.Stderr, tailFile(ownerLog, 200), tailFile(sandboxdLogPath(), 200))
			}
			nextRead = time.Now().Add(500 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestE2E_Sandboxd_TTYShell(t *testing.T) {
	skipE2E(t)
	b, _ := setupBusyboxVolume(t, "sandboxd-tty")
	require.NoError(t, b.FreezeVolume(t.Context(), "sandboxd-tty"))

	socket := startSandboxd(t)
	client := newSandboxdClient(t, socket)

	client.jsonRequest(t, http.MethodPost, "/v1/zygotes", map[string]any{
		"name":   "busybox-tty",
		"volume": "sandboxd-tty",
	})

	data := client.jsonRequest(t, http.MethodPost, "/v1/sandboxes", map[string]any{
		"name": "tty-sandbox",
		"source": map[string]any{
			"kind":   "zygote",
			"zygote": "busybox-tty",
		},
	})
	var sb sandboxRecord
	require.NoError(t, json.Unmarshal(data, &sb))
	require.Equal(t, "running", sb.State)

	procData := client.jsonRequest(t, http.MethodPost, fmt.Sprintf("/v1/sandboxes/%s/processes", sb.ID), map[string]any{
		"argv":       []string{"/bin/sh"},
		"tty":        true,
		"background": true,
		"rows":       24,
		"cols":       80,
	})
	var proc struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(procData, &proc))
	require.NotEmpty(t, proc.ID)

	conn := client.dialWebsocket(t, fmt.Sprintf("/v1/sandboxes/%s/processes/%s/stream", sb.ID, proc.ID))
	defer util.SafeClose(conn, "close tty shell websocket")
	require.NoError(t, conn.WriteJSON(map[string]any{"rows": 40, "cols": 100}))
	require.NoError(t, conn.WriteJSON(map[string]any{"stdin": "echo hello-from-tty\n"}))
	require.NoError(t, conn.WriteJSON(map[string]any{"stdin": "exit\n"}))

	var out strings.Builder
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var event struct {
			Stream string `json:"stream"`
			Data   string `json:"data"`
			Error  string `json:"error"`
		}
		require.NoError(t, conn.ReadJSON(&event))
		switch event.Stream {
		case "stdout", "stderr":
			out.WriteString(event.Data)
		case "error":
			t.Fatalf("tty shell stream error: %s", event.Error)
		case "exit":
			require.Equal(t, "0", event.Data)
			require.Contains(t, out.String(), "hello-from-tty")
			return
		}
	}
	t.Fatalf("timed out waiting for tty shell exit")
}
