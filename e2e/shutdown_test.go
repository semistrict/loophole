//go:build linux

package e2e

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_UnmountWithActiveProcess verifies that unmounting completes
// even when a process holds the mountpoint open.
func TestE2E_UnmountWithActiveProcess(t *testing.T) {
	skipE2E(t)
	if !needsRealMount() {
		t.Skip("test requires real mount (FUSE/NBD)")
	}

	b := newBackend(t)
	ctx := t.Context()

	vol := "unmount-active"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: vol}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "hello.txt", []byte("hello"))

	// Start a long-running process with CWD inside the mountpoint.
	cmd := exec.Command("sleep", "3600")
	cmd.Dir = mp
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	done := make(chan error, 1)
	go func() {
		done <- b.Unmount(ctx, mp)
	}()

	select {
	case err := <-done:
		t.Logf("Unmount returned: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Unmount hung for 10s with an active process on the mountpoint")
	}
}
