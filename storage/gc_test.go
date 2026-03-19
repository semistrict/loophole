package storage

import (
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGarbageCollect(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
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
	require.NoError(t, m.DeleteVolume(ctx, "test-vol"))

	// Dry-run should find 1 orphan but not delete anything.
	result, err := GarbageCollect(ctx, store, true)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 1, result.OrphanedLayers)
	assert.Equal(t, 0, result.DeletedObjects)

	// Real run should delete the orphan.
	result, err = GarbageCollect(ctx, store, false)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 1, result.OrphanedLayers)
	assert.Greater(t, result.DeletedObjects, 0)

	// Running again should find no orphans.
	result, err = GarbageCollect(ctx, store, false)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ReachableLayers)
	assert.Equal(t, 0, result.OrphanedLayers)
	assert.Equal(t, 0, result.DeletedObjects)
}

func TestGarbageCollectPreservesReachable(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
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
	result, err := GarbageCollect(ctx, store, false)
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
	ctx := t.Context()
	store := objstore.NewMemStore()
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
	require.NoError(t, v.Clone("cp-clone"))

	// Close and delete the clone but keep the original volume.
	m2 := NewManager(store, m.cacheDir, m.config, m.fs, m.diskCache)
	t.Cleanup(func() { _ = m2.Close() })
	require.NoError(t, m2.DeleteVolume(ctx, "cp-clone"))

	// GC should delete the clone's orphaned layer but preserve the original
	// volume's layers (including the frozen parent referenced via blockRanges).
	result, err := GarbageCollect(ctx, store, false)
	require.NoError(t, err)
	assert.Equal(t, 1, result.OrphanedLayers)
	assert.Greater(t, result.ReachableLayers, 0)

	// Original volume should still work.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, data, buf)
}
