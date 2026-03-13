//go:build linux

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/internal/util"
)

func findBusybox(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"busybox.static", "busybox"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	t.Skip("busybox not found in PATH")
	return ""
}

// execResult is the JSON response from /sandbox/exec.
type execResult struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// sandboxExec calls the daemon's /sandbox/exec endpoint over the unix socket.
func sandboxExec(t *testing.T, volume, cmd string) execResult {
	t.Helper()
	if volume == "" {
		t.Skip("host-only sandbox exec no longer exists without a volume owner")
	}
	sock := testDir.VolumeSocket(volume)
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}

	url := "http://loophole/sandbox/exec?cmd=" + cmd
	if volume != "" {
		url += "&volume=" + volume
	}
	resp, err := hc.Post(url, "", nil)
	require.NoError(t, err)
	defer util.SafeClose(resp.Body, "close sandbox exec response body")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "exec failed: %s", string(body))

	var result execResult
	require.NoError(t, json.Unmarshal(body, &result))
	return result
}

// setupBusyboxVolume creates a volume with a minimal busybox rootfs.
func setupBusyboxVolume(t *testing.T, name string) (*testBackend, string) {
	t.Helper()
	busybox := findBusybox(t)

	b := newBackend(t)
	mp := mountpoint(t, name)

	require.NoError(t, b.Create(t.Context(), client.CreateParams{Volume: name}))
	require.NoError(t, b.Mount(t.Context(), name, mp))

	tfs := newTestFS(t, b, mp)
	tfs.MkdirAll(t, "bin")
	tfs.MkdirAll(t, "etc")
	tfs.MkdirAll(t, "tmp")

	busyboxData, err := os.ReadFile(busybox)
	require.NoError(t, err)
	tfs.WriteFile(t, "bin/busybox", busyboxData)

	require.NoError(t, os.Chmod(mp+"/bin/busybox", 0o755))
	for _, applet := range []string{"sh", "cat", "ls", "echo", "mount", "id"} {
		require.NoError(t, os.Symlink("busybox", mp+"/bin/"+applet))
	}

	tfs.WriteFile(t, "marker.txt", []byte("chrooted\n"))

	if needsKernelExt4() {
		syncFS(t, mp)
	}

	return b, mp
}
