package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_FreezeFlushesFilesystemCache verifies that freezing a volume
// flushes the filesystem cache (e.g. lwext4 write-back cache) before
// persisting data to S3. Without this, cached metadata blocks (block
// bitmaps, group descriptors) may never reach the volume and subsequent
// mounts will see ext4 corruption.
func TestE2E_FreezeFlushesFilesystemCache(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	volName := "frz-cache"
	mp := mountpoint(t, volName)

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: volName}))
	require.NoError(t, b.Mount(ctx, volName, mp))

	// Write test files through the mounted filesystem (lwext4 with
	// write-back caching, or kernel ext4). This leaves dirty metadata
	// in the filesystem cache.
	tfs := newTestFS(t, b, mp)
	randomMD5 := writeTestFiles(t, tfs)

	// Freeze the volume. This flushes the filesystem cache, unmounts
	// the filesystem, and persists the storage layer to S3.
	require.NoError(t, b.FreezeVolume(ctx, volName, false))

	// Re-mount the frozen volume read-only and verify all files survived.
	mp2 := mountpoint(t, volName+"-ro")
	require.NoError(t, b.Mount(ctx, volName, mp2))
	b.mountedMPs = append(b.mountedMPs, mp2)

	verifyTestFiles(t, newTestFS(t, b, mp2), randomMD5)
}

// TestE2E_FreezeAndClonePreservesData creates a volume, writes data,
// freezes it, then clones from the frozen volume and verifies data
// integrity on the clone. This catches the case where the freeze
// fails to flush the filesystem cache and clones inherit corrupt data.
func TestE2E_FreezeAndClonePreservesData(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	volName := "frz-cln-parent"
	mp := mountpoint(t, volName)

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: volName}))
	require.NoError(t, b.Mount(ctx, volName, mp))

	tfs := newTestFS(t, b, mp)
	randomMD5 := writeTestFiles(t, tfs)

	// Freeze — flushes filesystem cache, unmounts, persists to S3.
	require.NoError(t, b.FreezeVolume(ctx, volName, false))

	// Mount the frozen volume read-only.
	frozenMP := mountpoint(t, volName+"-frozen")
	require.NoError(t, b.Mount(ctx, volName, frozenMP))
	b.mountedMPs = append(b.mountedMPs, frozenMP)

	// Clone from the frozen volume.
	cloneName := "frz-cln-child"
	cloneMP := mountpoint(t, cloneName)
	require.NoError(t, b.Clone(ctx, frozenMP, cloneName, cloneMP))

	// Verify data on clone.
	verifyTestFiles(t, newTestFS(t, b, cloneMP), randomMD5)
}
