package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func TestWritableLayerDoesNotUsePersistentPageCache(t *testing.T) {
	cache := newMemPageCache()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	m := &Manager{ObjectStore: objstore.NewMemStore(), config: cfg, diskCache: cache}
	t.Cleanup(func() { _ = m.Close() })

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

	require.Equal(t, 0, cache.Count())
}

func TestImmutableSourcePagesSharePersistentCacheAcrossClone(t *testing.T) {
	cache := newMemPageCache()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	store := objstore.NewMemStore()
	m := &Manager{ObjectStore: store, config: cfg, diskCache: cache}
	t.Cleanup(func() { _ = m.Close() })

	v, err := m.NewVolume(CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	require.NoError(t, v.Write(page, 0))
	require.NoError(t, v.Flush())

	require.NoError(t, v.Clone("child"))

	v.layer.blockCache.clear()

	// Open child on a separate manager (same store, same cache).
	m2 := &Manager{ObjectStore: store, config: cfg, diskCache: cache}
	t.Cleanup(func() { _ = m2.Close() })

	child, err := m2.OpenVolume("child")
	require.NoError(t, err)
	child.layer.blockCache.clear()

	buf := make([]byte, PageSize)
	_, err = v.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, 1, cache.Count())

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	// Both volumes share the same page in the cache.
	require.Equal(t, 1, cache.Count())
}

func TestChildOverrideDoesNotPopulatePersistentCacheForWritablePage(t *testing.T) {
	cache := newMemPageCache()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	store := objstore.NewMemStore()
	m := &Manager{ObjectStore: store, config: cfg, diskCache: cache}
	t.Cleanup(func() { _ = m.Close() })

	parent, err := m.NewVolume(CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, parent.Write(page, 0))
	require.NoError(t, parent.Flush())
	require.NoError(t, parent.Clone("child"))

	// Open child on a separate manager (same store).
	m2 := &Manager{ObjectStore: store, config: cfg, diskCache: cache}
	t.Cleanup(func() { _ = m2.Close() })

	child, err := m2.OpenVolume("child")
	require.NoError(t, err)

	buf := make([]byte, PageSize)
	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, 1, cache.Count())

	override := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, child.Write(override, 0))
	require.NoError(t, child.Flush())
	child.layer.blockCache.clear()

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, override, buf)

	require.Equal(t, 1, cache.Count())
}
