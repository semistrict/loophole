package storage

import (
	"testing"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGarbageCollect(t *testing.T) {
	// Disable grace period so freshly-created orphans are eligible for deletion.
	old := gcGracePeriod
	gcGracePeriod = 0
	t.Cleanup(func() { gcGracePeriod = old })

	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	m := newTestManager(t, store, testConfig)

	// Create a volume and write some data so a layer exists.
	v, err := m.NewVolume(CreateParams{Volume: "test-vol", Size: 1 << 30})
	require.NoError(t, err)

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	require.NoError(t, v.Write(data, 0))
	require.NoError(t, v.Flush())

	// Close the volume so we can delete it.
	require.NoError(t, v.ReleaseRef())

	// Delete the volume ref (leaves the layer orphaned).
	require.NoError(t, DeleteVolume(ctx, m.Store(), "test-vol"))

	// Dry-run should find 1 orphan but not delete anything.
	result, err := GarbageCollect(ctx, store, true, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 1, result.OrphanedLayers)
	assert.Equal(t, 0, result.DeletedObjects)

	// Real run should delete the orphan.
	result, err = GarbageCollect(ctx, store, false, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 1, result.OrphanedLayers)
	assert.Greater(t, result.DeletedObjects, 0)

	// Running again should find no orphans.
	result, err = GarbageCollect(ctx, store, false, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 0, result.OrphanedLayers)
	assert.Equal(t, 0, result.DeletedObjects)
}

func TestGarbageCollectPreservesReachable(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	m := newTestManager(t, store, testConfig)

	// Create a volume — its layer should be reachable.
	v, err := m.NewVolume(CreateParams{Volume: "keep-vol", Size: 1 << 30})
	require.NoError(t, err)

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = 0xAB
	}
	require.NoError(t, v.Write(data, 0))
	require.NoError(t, v.Flush())

	// GC should find 0 orphans and preserve the reachable layer.
	result, err := GarbageCollect(ctx, store, false, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, result.ReachableLayers)
	assert.Equal(t, 0, result.OrphanedLayers)

	// Verify the volume is still readable.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, data, buf)
}

func TestGarbageCollectWithCheckpoints(t *testing.T) {
	old := gcGracePeriod
	gcGracePeriod = 0
	t.Cleanup(func() { gcGracePeriod = old })

	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	m := newTestManager(t, store, testConfig)

	// Create a volume, checkpoint it, then delete the volume.
	v, err := m.NewVolume(CreateParams{Volume: "cp-vol", Size: 1 << 30})
	require.NoError(t, err)

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = 0xCD
	}
	require.NoError(t, v.Write(data, 0))
	require.NoError(t, v.Flush())

	// Create a checkpoint (this creates a frozen layer + new writable layer).
	require.NoError(t, checkpointAndClone(t, v, "cp-clone"))

	// Close and delete the clone but keep the original volume.
	m2 := &Manager{BlobStore: store, CacheDir: m.CacheDir, config: m.config, fs: m.fs}
	t.Cleanup(func() { _ = m2.Close() })
	require.NoError(t, DeleteVolume(ctx, m2.Store(), "cp-clone"))

	// GC should preserve the original volume's layers (including the checkpoint
	// ancestry) and delete both orphaned layers created by this flow:
	// the deleted clone's layer and the detached pre-checkpoint writable layer.
	result, err := GarbageCollect(ctx, store, false, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, result.OrphanedLayers)
	assert.Greater(t, result.ReachableLayers, 0)

	// Original volume should still work.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, data, buf)
}
