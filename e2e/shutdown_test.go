//go:build linux

package e2e

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_FreezeWithActiveProcess verifies that freezing a volume completes
// even when a process holds the mountpoint open. This reproduces the CF
// container shutdown hang: closeMount calls Freeze (fsfreeze ioctl) before
// unmount, and FIFREEZE blocks indefinitely if a process has pending I/O
// on the filesystem.
func TestE2E_FreezeWithActiveProcess(t *testing.T) {
	skipE2E(t)
	skipKernelOnly(t)

	b := newBackend(t)
	ctx := t.Context()

	vol := "freeze-active"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: vol}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	// Write a file so the volume isn't empty.
	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "hello.txt", []byte("hello"))

	// Start a process that does continuous I/O inside the mountpoint.
	// This is more realistic than just holding CWD — it creates pending
	// filesystem operations that block FIFREEZE.
	cmd := exec.Command("sh", "-c", "while true; do echo tick >> "+mp+"/busy.txt; sleep 0.1; done")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	// Give the process a moment to start I/O.
	time.Sleep(500 * time.Millisecond)

	// FreezeVolume calls the same Freeze path as closeMount during shutdown.
	done := make(chan error, 1)
	go func() {
		done <- b.FreezeVolume(ctx, vol, false)
	}()

	select {
	case err := <-done:
		t.Logf("FreezeVolume returned: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("FreezeVolume hung for 10s with an active process doing I/O on the mountpoint")
	}

	// The FS is now frozen. The sh process is in D state (uninterruptible
	// sleep) with pending I/O on the frozen FS — SIGKILL can't kill it.
	// Thaw first so the process can be reaped, then kill and unmount.
	thaw := exec.Command("fsfreeze", "--unfreeze", mp)
	if err := thaw.Run(); err != nil {
		t.Logf("fsfreeze --unfreeze: %v", err)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	_ = syscall.Unmount(mp, syscall.MNT_DETACH)
}

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

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: vol}))
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
