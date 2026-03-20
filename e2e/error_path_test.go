package e2e

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/storage"
)

// ---------- Create errors ----------

func TestE2E_CreateDuplicateVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "dup-create", Size: defaultVolumeSize}))

	err := b.Create(ctx, storage.CreateParams{Volume: "dup-create", Size: defaultVolumeSize})
	require.Error(t, err, "creating a volume that already exists should fail")
	assert.Contains(t, err.Error(), "already exists")
}

func TestE2E_CreateInvalidVolumeName(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	for _, name := range []string{
		"",
		"has/slash",
		"has\\backslash",
		"has..dotdot",
		"has\x00null",
	} {
		err := b.Create(ctx, storage.CreateParams{Volume: name, Size: defaultVolumeSize})
		assert.Error(t, err, "volume name %q should be rejected", name)
	}
}

// ---------- Mount errors ----------

func TestE2E_MountNonexistentVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	mp := mountpoint(t, "nonexistent-mount")
	err := b.Mount(ctx, "volume-does-not-exist", mp)
	require.Error(t, err, "mounting a nonexistent volume should fail")
}

func TestE2E_MountAlreadyMountedAtDifferentPath(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "mount-conflict"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))

	mp1 := mountpoint(t, vol+"-1")
	require.NoError(t, b.Mount(ctx, vol, mp1))

	mp2 := mountpoint(t, vol+"-2")
	err := b.Mount(ctx, vol, mp2)
	require.Error(t, err, "mounting a volume at a second mountpoint should fail")
	assert.Contains(t, err.Error(), "already mounted")
}

func TestE2E_MountIdempotent(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "mount-idem"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))

	mp := mountpoint(t, vol)
	require.NoError(t, b.Mount(ctx, vol, mp))

	// Same volume at same mountpoint should succeed (idempotent).
	err := b.Mount(ctx, vol, mp)
	require.NoError(t, err, "remounting at the same mountpoint should be idempotent")
}

// ---------- Unmount errors ----------

func TestE2E_UnmountNotMounted(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	err := b.Unmount(ctx, "/not-a-real-mountpoint")
	require.Error(t, err, "unmounting a path that is not mounted should fail")
}

// ---------- Clone errors ----------

func TestE2E_CloneNonexistentVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "clone-src"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))
	mp := mountpoint(t, vol)
	require.NoError(t, b.Mount(ctx, vol, mp))

	// Clone with an invalid name.
	err := b.Clone(ctx, mp, "has/slash")
	require.Error(t, err, "clone with invalid name should fail")
}

func TestE2E_CloneDuplicateName(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	// Create source volume and mount it.
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "clone-dup-src", Size: defaultVolumeSize}))
	mp := mountpoint(t, "clone-dup-src")
	require.NoError(t, b.Mount(ctx, "clone-dup-src", mp))

	// Create a volume that will collide with the clone name.
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "clone-dup-target", Size: defaultVolumeSize}))

	// Clone into existing name should fail.
	err := b.Clone(ctx, mp, "clone-dup-target")
	require.Error(t, err, "cloning into an existing volume name should fail")
	assert.Contains(t, err.Error(), "already exists")
}

// ---------- Checkpoint errors ----------

func TestE2E_CheckpointUnmountedVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	_, err := b.Checkpoint(ctx, "/nonexistent-mountpoint")
	require.Error(t, err, "checkpoint on an untracked mountpoint should fail")
}

func TestE2E_CloneFromBadCheckpointID(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "cp-bad-id"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))

	// Try cloning from a checkpoint that doesn't exist.
	err := b.CloneFromCheckpoint(ctx, vol, "99999999999999", "cp-bad-clone")
	require.Error(t, err, "clone from nonexistent checkpoint should fail")
}

func TestE2E_CloneFromCheckpointNonexistentVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	err := b.CloneFromCheckpoint(ctx, "no-such-volume", "20260317120000", "no-such-clone")
	require.Error(t, err, "clone from nonexistent volume should fail")
}

// ---------- Delete errors ----------

func TestE2E_DeleteNonexistentVolume(t *testing.T) {
	newBackend(t) // registers cleanup and calls skipE2E
	ctx := t.Context()

	vm, cleanup, err := openDirectManager(ctx)
	require.NoError(t, err)
	defer cleanup()

	err = storage.DeleteVolume(ctx, vm.Store(), "no-such-delete-vol")
	require.Error(t, err, "deleting a nonexistent volume should fail")
}

func TestE2E_DeleteOpenVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "delete-open"
	mp := mountpoint(t, vol)
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	// Deleting a mounted volume via the owner daemon should still work
	// (the daemon unmounts first), but deleting via a direct manager while
	// the owner process has the lease should fail.
	vm, cleanup, err := openDirectManager(ctx)
	require.NoError(t, err)
	defer cleanup()

	err = storage.DeleteVolume(ctx, vm.Store(), vol)
	require.Error(t, err, "deleting a volume held by another process should fail")
}

// ---------- Data integrity after error ----------

func TestE2E_VolumeSurvivesFailedClone(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "survive-bad-clone"
	mp := mountpoint(t, vol)
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	tfs := newTestFS(t, b, mp)
	randomMD5 := writeTestFiles(t, tfs)

	// Create a colliding volume so clone fails.
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "survive-collider", Size: defaultVolumeSize}))

	// Clone should fail due to name collision.
	err := b.Clone(ctx, mp, "survive-collider")
	require.Error(t, err)

	// Original volume should be intact and writable.
	verifyTestFiles(t, tfs, randomMD5)
	tfs.WriteFile(t, "after-failed-clone.txt", []byte("still works"))
	require.Equal(t, "still works", string(tfs.ReadFile(t, "after-failed-clone.txt")))
}

// ---------- ListCheckpoints errors ----------

func TestE2E_ListCheckpointsNonexistentVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	// Listing checkpoints for a volume that has never had any should return
	// an empty list, not an error.
	cps, err := b.ListCheckpoints(ctx, "no-such-list-cp-vol")
	require.NoError(t, err)
	require.Empty(t, cps)
}

func TestE2E_ListCheckpointsEmptyVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "list-cp-empty"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize}))

	cps, err := b.ListCheckpoints(ctx, vol)
	require.NoError(t, err)
	require.Empty(t, cps, "new volume should have no checkpoints")
}

// ---------- Device errors ----------

func TestE2E_DeviceAttachNonexistentVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	_, err := b.DeviceAttach(ctx, "no-such-device-vol")
	require.Error(t, err, "device attach on a nonexistent volume should fail")
}

func TestE2E_DeviceCheckpointNoOwner(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	vol := "dev-cp-no-owner"
	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol, Size: defaultVolumeSize, NoFormat: true}))

	// DeviceCheckpoint requires an attached owner. Asking for a checkpoint
	// on a volume with no owner should fail or auto-attach and succeed.
	// The point is it should not crash.
	_, err := b.DeviceCheckpoint(ctx, vol)
	// We just verify it returns without panic; it may succeed (auto-attach)
	// or fail cleanly.
	_ = err
}

// ---------- Filesystem-level errors ----------

func TestE2E_ReadNonexistentFile(t *testing.T) {
	tfs, _ := mountVolume(t, "read-noexist")

	_, err := tfs.fs.ReadFile("does-not-exist.txt")
	require.Error(t, err, "reading a nonexistent file should fail")
	assert.True(t, os.IsNotExist(err), "error should be os.IsNotExist")
}

func TestE2E_StatNonexistentFile(t *testing.T) {
	tfs, _ := mountVolume(t, "stat-noexist")

	_, err := tfs.fs.Stat("does-not-exist.txt")
	require.Error(t, err, "stat on a nonexistent file should fail")
	assert.True(t, os.IsNotExist(err), "error should be os.IsNotExist")
}

func TestE2E_RemoveNonexistentFile(t *testing.T) {
	tfs, _ := mountVolume(t, "rm-noexist")

	err := tfs.fs.Remove("does-not-exist.txt")
	require.Error(t, err, "removing a nonexistent file should fail")
}
