//go:build linux

package e2e

import (
	"context"
	"encoding/json"
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

// TestE2E_ShellWebSocket tests the /sandbox/shell WebSocket endpoint
// which now speaks the midterm JSON protocol (screen/update/closed messages).
func TestE2E_ShellWebSocket(t *testing.T) {
	skipE2E(t)

	busybox := findBusybox(t)

	b := newBackend(t)
	ctx := t.Context()
	vol := "shell-ws"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: vol}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	tfs := newTestFS(t, b, mp)
	tfs.MkdirAll(t, "bin")
	tfs.MkdirAll(t, "tmp")

	busyboxData, err := os.ReadFile(busybox)
	require.NoError(t, err)
	tfs.WriteFile(t, "bin/busybox", busyboxData)

	require.NoError(t, os.Chmod(mp+"/bin/busybox", 0o755))
	for _, applet := range []string{"sh", "cat", "ls", "echo"} {
		require.NoError(t, os.Symlink("busybox", mp+"/bin/"+applet))
	}

	tfs.WriteFile(t, "chroot-marker.txt", []byte("inside-volume\n"))

	if needsKernelExt4() {
		syncFS(t, mp)
	}

	// Connect to the daemon's shell WebSocket (compat endpoint).
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

	// Read the initial screen message.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, screenData, err := conn.ReadMessage()
	require.NoError(t, err)

	var screen struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(screenData, &screen))
	require.Equal(t, "screen", screen.Type)

	// Send input via the JSON protocol.
	sendInput := func(data string) {
		msg, _ := json.Marshal(map[string]string{"type": "input", "data": data})
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, msg))
	}

	sendInput("cat /chroot-marker.txt\n")
	sendInput("ls /chroot-marker.txt && echo CHROOT_OK\n")
	sendInput("exit\n")

	// Collect all row HTML from update messages until the connection closes.
	var allHTML []string
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var msg struct {
			Type string          `json:"type"`
			Rows json.RawMessage `json:"rows"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "update":
			var rows map[string]string
			if json.Unmarshal(msg.Rows, &rows) == nil {
				for _, html := range rows {
					allHTML = append(allHTML, html)
				}
			}
		case "screen":
			var rows []string
			if json.Unmarshal(msg.Rows, &rows) == nil {
				allHTML = append(allHTML, rows...)
			}
		}
	}

	// Strip HTML tags to get plain text for assertions.
	out := stripHTML(strings.Join(allHTML, "\n"))
	t.Logf("shell output:\n%s", out)

	require.Contains(t, out, "inside-volume",
		"expected shell output to contain 'inside-volume' (marker file content)")
	require.Contains(t, out, "CHROOT_OK",
		"expected 'CHROOT_OK' in output (chroot verification)")
}

// stripHTML removes HTML tags from a string, returning plain text.
func stripHTML(s string) string {
	var b strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			continue
		}
		if !inTag {
			b.WriteRune(r)
		}
	}
	return b.String()
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
			if err := exec.Command(path, "--help").Run(); err == nil {
				return path
			}
		}
	}
	if p, err := exec.LookPath("busybox"); err == nil {
		return p
	}
	t.Skip("busybox not found; install busybox-static for shell e2e tests")
	return ""
}
