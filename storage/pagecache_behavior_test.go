package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/cached/cachedserver"
	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

// startInProcessCache starts an in-process page cache server and returns
// a PageCache client. The server is stopped on test cleanup.
func startInProcessCache(t *testing.T) *cached.PageCache {
	t.Helper()
	cachedserver.InitInMemory(256)
	ln := cachedserver.NewPipeListener()
	require.NoError(t, cachedserver.StartServerWithListener("", ln))
	t.Cleanup(func() {
		ln.Close()
		cachedserver.Shutdown()
	})
	sp := safepoint.New()
	client := ln.Dial(sp)
	t.Cleanup(func() { client.Close() })
	return client
}

func TestWritableLayerDoesNotUsePersistentPageCache(t *testing.T) {
	cache := startInProcessCache(t)
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

	count, err := cache.Count()
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestImmutableSourcePagesSharePersistentCacheAcrossClone(t *testing.T) {
	cache := startInProcessCache(t)
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
	count, err := cache.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	count, err = cache.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestChildOverrideDoesNotPopulatePersistentCacheForWritablePage(t *testing.T) {
	cache := startInProcessCache(t)
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
	count, err := cache.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	override := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, child.Write(override, 0))
	require.NoError(t, child.Flush())
	child.layer.blockCache.clear()

	_, err = child.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, override, buf)

	count, err = cache.Count()
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
