//go:build linux

package e2e

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/storage"
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

// setupBusyboxVolume creates a volume with a minimal busybox rootfs.
func setupBusyboxVolume(t *testing.T, name string) (*testBackend, string) {
	t.Helper()
	busybox := findBusybox(t)

	b := newBackend(t)
	mp := mountpoint(t, name)

	require.NoError(t, b.Create(t.Context(), storage.CreateParams{Volume: name}))
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
