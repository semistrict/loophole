package loophole

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStaleCacheAfterExternalWrite reproduces the cache staleness bug:
//  1. VM1 writes data, flushes to S3, closes.
//  2. VM1 restarts (same cache dir), reads the block — populates immutable cache.
//  3. VM1 closes.
//  4. VM2 (different cache) opens the volume, overwrites the same block, flushes, closes.
//  5. VM1 restarts again (same cache dir) — reads stale data from immutable cache.
func TestStaleCacheAfterExternalWrite(t *testing.T) {
	store := NewMemStore()
	cacheDir := t.TempDir() // shared cache dir for VM1 across restarts

	// --- Step 1: VM1 writes "old data" and flushes ---
	require.NoError(t, FormatSystem(t.Context(), store, 64))

	vm1a := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	require.NoError(t, vm1a.Connect(t.Context()))

	vol, err := vm1a.NewVolume(t.Context(), "shared-vol", 0)
	require.NoError(t, err)

	oldData := make([]byte, 64)
	copy(oldData, "old data from vm1")
	require.NoError(t, vol.Write(t.Context(), oldData, 0))
	require.NoError(t, vol.Flush(t.Context()))
	require.NoError(t, vm1a.Close(t.Context()))

	// --- Step 2: VM1 restarts, reads the block (populates immutable cache) ---
	vm1b := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	require.NoError(t, vm1b.Connect(t.Context()))

	vol, err = vm1b.OpenVolume(t.Context(), "shared-vol")
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = vol.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, oldData, buf, "sanity: should read old data")

	require.NoError(t, vm1b.Close(t.Context()))

	// --- Step 3: VM2 overwrites the same block with new data ---
	vm2 := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm2.Connect(t.Context()))

	vol2, err := vm2.OpenVolume(t.Context(), "shared-vol")
	require.NoError(t, err)

	newData := make([]byte, 64)
	copy(newData, "new data from vm2")
	require.NoError(t, vol2.Write(t.Context(), newData, 0))
	require.NoError(t, vol2.Flush(t.Context()))
	require.NoError(t, vm2.Close(t.Context()))

	// --- Step 4: VM1 restarts again — should see new data but gets stale cache ---
	vm1c := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	require.NoError(t, vm1c.Connect(t.Context()))
	defer func() {
		require.NoError(t, vm1c.Close(t.Context()))
	}()

	vol3, err := vm1c.OpenVolume(t.Context(), "shared-vol")
	require.NoError(t, err)

	buf = make([]byte, 64)
	_, err = vol3.Read(t.Context(), buf, 0)
	require.NoError(t, err)

	assert.Equal(t, newData, buf, "VM1 should see VM2's updated data, not stale cache")
}

// TestFreezeAdoptsMutableToImmutableCache verifies that when a layer is
// frozen, its mutable cache files are moved to the immutable cache so
// subsequent reads (and new VMs sharing the cache dir) get cache hits
// without re-fetching from S3.
func TestFreezeAdoptsMutableToImmutableCache(t *testing.T) {
	store := NewMemStore()
	cacheDir := t.TempDir()

	require.NoError(t, FormatSystem(t.Context(), store, 64))

	vm := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	require.NoError(t, vm.Connect(t.Context()))

	vol, err := vm.NewVolume(t.Context(), "testvol", 0)
	require.NoError(t, err)

	data := make([]byte, 64)
	copy(data, "freeze me")
	require.NoError(t, vol.Write(t.Context(), data, 0))

	// Before freeze: mutable dir should have the block, immutable should not.
	lv := vol.(*legacyVolume)
	layerID := lv.layer.id
	mutableLayerDir := lv.layer.mutableDir
	immutableLayerDir := filepath.Join(cacheDir, "immutable", layerID)

	_, err = os.Stat(mutableLayerDir)
	require.NoError(t, err, "mutable dir should exist before freeze")
	_, err = os.Stat(immutableLayerDir)
	assert.True(t, os.IsNotExist(err), "immutable dir should not exist before freeze")

	// Freeze (via snapshot to keep the volume writable).
	require.NoError(t, vol.Snapshot(t.Context(), "snap"))

	// After freeze: mutable dir should be gone, immutable should have it.
	_, err = os.Stat(mutableLayerDir)
	assert.True(t, os.IsNotExist(err), "mutable dir should be gone after freeze")
	_, err = os.Stat(immutableLayerDir)
	require.NoError(t, err, "immutable dir should exist after freeze")

	require.NoError(t, vm.Close(t.Context()))

	// New VM with same cache dir should read from immutable cache (no S3 needed).
	vm2 := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	require.NoError(t, vm2.Connect(t.Context()))
	defer func() { require.NoError(t, vm2.Close(t.Context())) }()

	snap, err := vm2.OpenVolume(t.Context(), "snap")
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = snap.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)
}
