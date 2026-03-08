//go:build linux

package e2e

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_ShellWebSocket tests the /sandbox/shell WebSocket endpoint.
// It creates a volume with a minimal rootfs (busybox), connects via WebSocket,
// runs a command, and verifies the shell is chrooted to the volume root.
func TestE2E_ShellWebSocket(t *testing.T) {
	skipE2E(t)

	// We need a statically-linked shell. busybox-static is ideal.
	busybox := findBusybox(t)

	b := newBackend(t)
	ctx := t.Context()
	vol := "shell-ws"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: vol}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	// Build a minimal rootfs: /bin/sh -> busybox, with common applet symlinks.
	tfs := newTestFS(t, b, mp)
	tfs.MkdirAll(t, "bin")
	tfs.MkdirAll(t, "tmp")

	// Copy busybox into the volume.
	busyboxData, err := os.ReadFile(busybox)
	require.NoError(t, err)
	tfs.WriteFile(t, "bin/busybox", busyboxData)

	// Make busybox executable and create applet symlinks.
	require.NoError(t, os.Chmod(mp+"/bin/busybox", 0o755))
	for _, applet := range []string{"sh", "cat", "ls", "echo"} {
		require.NoError(t, os.Symlink("busybox", mp+"/bin/"+applet))
	}

	// Create a marker file to verify chroot works.
	tfs.WriteFile(t, "chroot-marker.txt", []byte("inside-volume\n"))

	if needsKernelExt4() {
		syncFS(t, mp)
	}

	// Connect to the daemon's shell WebSocket.
	sock := testClient.Socket()
	dialer := websocket.Dialer{
		NetDialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", sock)
		},
	}

	conn, resp, err := dialer.Dial("ws://loophole/sandbox/shell?volume="+vol, http.Header{})
	require.NoError(t, err, "WebSocket dial failed")
	defer conn.Close()
	require.Equal(t, 101, resp.StatusCode)

	// Send a command to read the marker file.
	// The shell prompt may appear first; we send our command and look for output.
	err = conn.WriteMessage(websocket.BinaryMessage, []byte("cat /chroot-marker.txt\n"))
	require.NoError(t, err)

	// Also test that / is the volume root by listing it.
	err = conn.WriteMessage(websocket.BinaryMessage, []byte("ls /chroot-marker.txt && echo CHROOT_OK\n"))
	require.NoError(t, err)

	// Tell the shell to exit.
	err = conn.WriteMessage(websocket.BinaryMessage, []byte("exit\n"))
	require.NoError(t, err)

	// Read all output until the connection closes.
	var output bytes.Buffer
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		output.Write(msg)
	}

	out := output.String()
	t.Logf("shell output:\n%s", out)

	// Verify the marker file content appeared in the output.
	require.True(t, strings.Contains(out, "inside-volume"),
		"expected shell output to contain 'inside-volume' (marker file content), got:\n%s", out)

	// Verify chroot worked — ls found the file at / and printed CHROOT_OK.
	require.True(t, strings.Contains(out, "CHROOT_OK"),
		"expected 'CHROOT_OK' in output (chroot verification), got:\n%s", out)
}

// findBusybox returns the path to a statically-linked busybox binary.
// Skips the test if not found.
func findBusybox(t *testing.T) string {
	t.Helper()
	for _, path := range []string{
		"/bin/busybox",
		"/usr/bin/busybox",
	} {
		if _, err := os.Stat(path); err == nil {
			// Verify it's statically linked (or at least executable).
			if err := exec.Command(path, "--help").Run(); err == nil {
				return path
			}
		}
	}
	// Try PATH.
	if p, err := exec.LookPath("busybox"); err == nil {
		return p
	}
	t.Skip("busybox not found; install busybox-static for shell e2e tests")
	return ""
}
