package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func newTestPersistentPageCache(t *testing.T) (*PageCache, *mockStore) {
	t.Helper()
	store := newMockStore(64*PageSize, 0)
	cache, err := newPageCacheWithStore(store)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})
	store.freeFn = func() int64 { return 64*PageSize - cache.usedBytes }
	return cache, store
}

func TestWritableLayerDoesNotUsePersistentPageCache(t *testing.T) {
	cache, backing := newTestPersistentPageCache(t)
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	m := NewManager(objstore.NewMemStore(), t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	v, err := m.NewVolume(CreateParams{
		Volume:   "vol",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	v.layer.blockCache.clear()

	buf := make([]byte, PageSize)
	_, err = v.Read(t.Context(), buf, 5*PageSize)
	require.NoError(t, err)

	count, err := backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestImmutableSourcePagesSharePersistentCacheAcrossClone(t *testing.T) {
	cache, backing := newTestPersistentPageCache(t)
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	cacheDir := t.TempDir()
	m := NewManager(objstore.NewMemStore(), cacheDir, cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	v, err := m.NewVolume(CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	require.NoError(t, v.Write(page, 0))
	require.NoError(t, v.Flush())

	// Capture the layer ID that owns the L1/L2 blocks before clone
	// (clone switches the parent to a new layer).
	parent := v
	originalLayerID := parent.layer.id

	require.NoError(t, v.Clone("child"))

	parent.layer.blockCache.clear()

	child, err := m.OpenVolume("child")
	require.NoError(t, err)
	childVol := child
	childVol.layer.blockCache.clear()

	buf := make([]byte, PageSize)
	_, err = v.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	count, err := backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	count, err = backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify the shared logical source key was used.
	// The blocks were written under the original layer ID (before clone).
	key := cacheKey{LayerID: originalLayerID, PageIdx: 0}
	ref, ok, err := backing.LookupPage(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.GreaterOrEqual(t, ref.Slot, 0)
}

func TestChildOverrideDoesNotPopulatePersistentCacheForWritablePage(t *testing.T) {
	cache, backing := newTestPersistentPageCache(t)
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	m := NewManager(objstore.NewMemStore(), t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	parent, err := m.NewVolume(CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, parent.Write(page, 0))
	require.NoError(t, parent.Flush())
	require.NoError(t, parent.Clone("child"))

	child, err := m.OpenVolume("child")
	require.NoError(t, err)
	childVol := child

	buf := make([]byte, PageSize)
	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	count, err := backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	override := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, child.Write(override, 0))
	require.NoError(t, child.Flush())
	childVol.layer.blockCache.clear()

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, override, buf)

	count, err = backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
