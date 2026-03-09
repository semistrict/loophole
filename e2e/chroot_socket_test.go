//go:build linux

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// setupChrootSocketVolume creates a busybox volume, triggers prepareChrootEnv
// by running a command, and returns a client connected to the chroot socket.
func setupChrootSocketVolume(t *testing.T, name string) (*testBackend, string, *client.Client) {
	t.Helper()
	b, mp := setupBusyboxVolume(t, name)

	// Trigger prepareChrootEnv by running a command inside the chroot.
	r := sandboxExec(t, name, "echo+ready")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "ready\n", r.Stdout)

	// Connect to the host-side chroot socket.
	hostSock := filepath.Join(os.TempDir(), "loophole-chroot-"+filepath.Base(mp)+".sock")
	c := client.NewFromSocket(hostSock)

	return b, mp, c
}

func TestE2E_ChrootSocketAndBinaryExist(t *testing.T) {
	skipE2E(t)
	setupChrootSocketVolume(t, "csock-exist")

	// /.loophole should exist inside the chroot.
	r := sandboxExec(t, "csock-exist", "ls+/.loophole")
	require.Equal(t, 0, r.ExitCode)

	// /usr/bin/loophole should exist inside the chroot.
	r = sandboxExec(t, "csock-exist", "ls+/usr/bin/loophole")
	require.Equal(t, 0, r.ExitCode)
}

func TestE2E_ChrootSocketStatus(t *testing.T) {
	skipE2E(t)
	_, _, c := setupChrootSocketVolume(t, "csock-status")

	status, err := c.ChrootStatus(t.Context())
	require.NoError(t, err)
	require.Equal(t, "csock-status", status.Name)
	require.Greater(t, status.Size, uint64(0))
	require.NotEmpty(t, status.Layer.LayerID)
}

func TestE2E_ChrootSocketFlush(t *testing.T) {
	skipE2E(t)
	b, mp, c := setupChrootSocketVolume(t, "csock-flush")

	// Write data so there's something to flush.
	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "flush-test.txt", []byte("flush me\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}

	require.NoError(t, c.Flush(t.Context()))

	// Verify file still readable after flush.
	require.Equal(t, "flush me\n", string(tfs.ReadFile(t, "flush-test.txt")))
}

func TestE2E_ChrootSocketSnapshot(t *testing.T) {
	skipE2E(t)
	b, mp, c := setupChrootSocketVolume(t, "csock-snap")

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "snap-test.txt", []byte("snapshot data\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}

	ctx := t.Context()
	require.NoError(t, c.ChrootSnapshot(ctx, "csock-snap-v1"))

	// Open the snapshot volume to verify it was created and is read-only.
	snapVol, err := testDaemon.Backend().VM().OpenVolume(ctx, "csock-snap-v1")
	require.NoError(t, err, "snapshot volume should be openable")
	require.True(t, snapVol.ReadOnly(), "snapshot should be read-only")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = snapVol.ReleaseRef()
		_ = testDaemon.Backend().VM().DeleteVolume(ctx, "csock-snap-v1")
	})

	// Source volume should still be writable after snapshot.
	tfs.WriteFile(t, "after-snap.txt", []byte("still writable\n"))
	require.Equal(t, "still writable\n", string(tfs.ReadFile(t, "after-snap.txt")))
}

func TestE2E_ChrootSocketClone(t *testing.T) {
	skipE2E(t)
	b, mp, c := setupChrootSocketVolume(t, "csock-clone")

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "clone-test.txt", []byte("clone me\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}

	ctx := t.Context()
	cloneMP, err := c.ChrootClone(ctx, "csock-clone-v1")
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = testClient.Unmount(ctx, cloneMP)
		_ = testClient.Delete(ctx, "csock-clone-v1")
		_ = os.Remove(cloneMP)
	})

	// Clone should be mounted and writable.
	cloneFS, err := testDaemon.Backend().FS(cloneMP)
	require.NoError(t, err)

	// Data from the source should be present.
	data, err := cloneFS.ReadFile("clone-test.txt")
	require.NoError(t, err)
	require.Equal(t, "clone me\n", string(data))

	// Clone should be writable.
	require.NoError(t, cloneFS.WriteFile("new-file.txt", []byte("clone only\n"), 0o644))

	// Source should not see the clone's writes.
	require.False(t, tfs.Exists(t, "new-file.txt"))
}
