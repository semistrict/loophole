package storage

import (
	"bytes"
	"os"
	"sync"
	"testing"
)

func pk(tl string, idx PageIdx) cacheKey {
	return cacheKey{LayerID: tl, PageIdx: idx}
}

// shortTempDir creates a temp dir with a short path to avoid
// exceeding macOS's 104-byte Unix socket path limit.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newTestPageCache(t *testing.T) *PageCache {
	t.Helper()
	dir := shortTempDir(t)
	cache, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func TestDiskCachePageRoundTrip(t *testing.T) {
	cache := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	cache.PutPage(pk("tl", 1), page)

	got := cache.GetPage(pk("tl", 1))
	if !bytes.Equal(got, page) {
		t.Fatalf("page mismatch")
	}

	cache.DeletePage(pk("tl", 1))
	if got := cache.GetPage(pk("tl", 1)); got != nil {
		t.Fatalf("expected page miss after delete")
	}
}

func TestGetPageRoundTrip(t *testing.T) {
	cache := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xCD}, PageSize)
	cache.PutPage(pk("p", 1), page)

	data := cache.GetPage(pk("p", 1))
	if data == nil {
		t.Fatal("expected cache hit")
	}
	if !bytes.Equal(data, page) {
		t.Fatal("cached data mismatch")
	}

	if got := cache.GetPage(pk("missing", 0)); got != nil {
		t.Fatal("expected nil for cache miss")
	}
}

func TestGetPageReturnsCopy(t *testing.T) {
	cache := newTestPageCache(t)

	page := bytes.Repeat([]byte{0x11}, PageSize)
	cache.PutPage(pk("t", 1), page)

	data := cache.GetPage(pk("t", 1))
	if data == nil {
		t.Fatal("expected hit")
	}

	// Mutate the returned copy — should not affect the cache.
	data[0] = 0xFF

	got := cache.GetPage(pk("t", 1))
	if got == nil {
		t.Fatal("expected hit on second read")
	}
	if !bytes.Equal(got, page) {
		t.Fatal("mutation of copy should not affect cached data")
	}
}

func TestDiskCacheConcurrentAccess(t *testing.T) {
	cache := newTestPageCache(t)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := pk("t", PageIdx(i))
			page := bytes.Repeat([]byte{byte(i)}, PageSize)
			cache.PutPage(key, page)
			got := cache.GetPage(key)
			if !bytes.Equal(got, page) {
				t.Errorf("page %d mismatch", i)
			}
		}(i)
	}
	wg.Wait()
}

func TestPageCachePersistence(t *testing.T) {
	dir := shortTempDir(t)

	cache, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	cache.PutPage(pk("tl", 1), page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — data should still be there (persisted via SQLite).
	cache2, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	got := cache2.GetPage(pk("tl", 1))
	if !bytes.Equal(got, page) {
		t.Fatalf("page not persisted across restart")
	}
}

func TestPageCacheMultiClient(t *testing.T) {
	dir := shortTempDir(t)

	cache1, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache1.Close() })

	page := bytes.Repeat([]byte{0xCD}, PageSize)
	cache1.PutPage(pk("shared", 1), page)

	// Second client connects to the same daemon.
	cache2, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Close cache2 before cache1 (cache1 owns the daemon).
	t.Cleanup(func() { _ = cache2.Close() })

	got := cache2.GetPage(pk("shared", 1))
	if !bytes.Equal(got, page) {
		t.Fatalf("second client should see first client's page")
	}
}

func TestPageCacheCloseIsIdempotent(t *testing.T) {
	cache := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xEE}, PageSize)
	cache.PutPage(pk("t", 1), page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	// Writes/reads after close are silently ignored.
	cache.PutPage(pk("t", 2), page)
	if got := cache.GetPage(pk("t", 1)); got != nil {
		t.Fatalf("expected nil after close")
	}

	// Double close should not panic.
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheGetPageRef(t *testing.T) {
	cache := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xDD}, PageSize)
	cache.PutPage(pk("ref", 1), page)

	data, unpin := cache.GetPageRef(pk("ref", 1))
	if data == nil {
		t.Fatal("expected hit")
	}
	defer unpin()

	if !bytes.Equal(data, page) {
		t.Fatal("ref data mismatch")
	}

	// Miss should return (nil, nil).
	data, unpin = cache.GetPageRef(pk("missing", 0))
	if data != nil || unpin != nil {
		t.Fatal("expected nil for cache miss")
	}
}

func TestPageCacheCountPages(t *testing.T) {
	cache := newTestPageCache(t)

	for i := range 5 {
		cache.PutPage(pk("t", PageIdx(i)), bytes.Repeat([]byte{byte(i)}, PageSize))
	}

	count, err := cache.CountPages()
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("expected 5 pages, got %d", count)
	}

	cache.DeletePage(pk("t", 2))
	count, err = cache.CountPages()
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("expected 4 pages after delete, got %d", count)
	}
}
