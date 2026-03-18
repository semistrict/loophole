package storage2

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole"
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

	m := NewVolumeManager(loophole.NewMemStore(), t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	v, err := m.NewVolume(loophole.CreateParams{
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
	v.(*volume).layer.blockCache.clear()

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
	m := NewVolumeManager(loophole.NewMemStore(), cacheDir, cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	v, err := m.NewVolume(loophole.CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	require.NoError(t, v.Write(page, 0))
	require.NoError(t, v.Flush())

	// Capture the layer ID that owns the L1/L2 blocks before clone
	// (clone switches the parent to a new layer).
	parent := v.(*volume)
	originalLayerID := parent.layer.id

	require.NoError(t, v.Clone("child"))

	parent.layer.blockCache.clear()

	child, err := m.OpenVolume("child")
	require.NoError(t, err)
	childVol := child.(*volume)
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

func TestFrozenLayerPageCacheSurvivesBlockCacheEviction(t *testing.T) {
	cache, backing := newTestPersistentPageCache(t)
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	store := loophole.NewMemStore()
	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)

	buf := make([]byte, PageSize)
	_, err = frozenVol.Read(t.Context(), buf, 5*PageSize)
	require.NoError(t, err)

	key := cacheKey{LayerID: frozen.layer.id, PageIdx: 5}
	refBefore, ok, err := backing.LookupPage(key)
	require.NoError(t, err)
	require.True(t, ok)

	count, err := backing.CountPages()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	frozen.layer.blockCache.clear()

	_, err = frozenVol.Read(t.Context(), buf, 5*PageSize)
	require.NoError(t, err)
	count, err = backing.CountPages()
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 1)

	refAfter, ok, err := backing.LookupPage(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, refBefore.Slot, refAfter.Slot)
	require.GreaterOrEqual(t, refAfter.Generation, refBefore.Generation)
}

func TestChildOverrideDoesNotPopulatePersistentCacheForWritablePage(t *testing.T) {
	cache, backing := newTestPersistentPageCache(t)
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	m := NewVolumeManager(loophole.NewMemStore(), t.TempDir(), cfg, nil, cache)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	parent, err := m.NewVolume(loophole.CreateParams{Volume: "root", Size: 1024 * 1024, NoFormat: true})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, parent.Write(page, 0))
	require.NoError(t, parent.Flush())
	require.NoError(t, parent.Clone("child"))

	child, err := m.OpenVolume("child")
	require.NoError(t, err)
	childVol := child.(*volume)

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

func TestRepro_SharedPersistentPageCacheCorruptsImmutableReadAcrossManagers(t *testing.T) {
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	store := loophole.NewMemStore()
	cacheRoot := t.TempDir()
	creator := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() { _ = creator.Close(t.Context()) })

	v, err := creator.NewVolume(loophole.CreateParams{Volume: "zygote", Size: 4 * PageSize, NoFormat: true})
	require.NoError(t, err)

	pageA := bytes.Repeat([]byte{0xAA}, PageSize)
	pageB := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, v.Write(pageA, 0))
	require.NoError(t, v.Write(pageB, PageSize))
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	cache1, err := NewPageCache(filepath.Join(cacheRoot, "diskcache"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache1.Close()) })
	cache2, err := NewPageCache(filepath.Join(cacheRoot, "diskcache"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache2.Close()) })

	reader1 := NewVolumeManager(store, t.TempDir(), cfg, nil, cache1)
	t.Cleanup(func() { _ = reader1.Close(t.Context()) })
	reader2 := NewVolumeManager(store, t.TempDir(), cfg, nil, cache2)
	t.Cleanup(func() { _ = reader2.Close(t.Context()) })

	frozen1, err := reader1.OpenVolume("zygote")
	require.NoError(t, err)
	frozen2, err := reader2.OpenVolume("zygote")
	require.NoError(t, err)

	ly1 := frozen1.(*frozenVolume).layer
	ly2 := frozen2.(*frozenVolume).layer

	bufA := make([]byte, PageSize)
	_, err = frozen1.Read(t.Context(), bufA, 0)
	require.NoError(t, err)
	require.Equal(t, pageA, bufA)

	ly1.blockCache.clear()
	ly2.blockCache.clear()

	bufB := make([]byte, PageSize)
	_, err = frozen2.Read(t.Context(), bufB, PageSize)
	require.NoError(t, err)
	require.Equal(t, pageB, bufB)

	ly1.blockCache.clear()

	bufAgain := make([]byte, PageSize)
	_, err = frozen1.Read(t.Context(), bufAgain, 0)
	require.NoError(t, err)
	require.Equal(t, pageA, bufAgain)
}
