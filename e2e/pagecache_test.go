package e2e

import (
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/semistrict/loophole/internal/cached"
	"github.com/semistrict/loophole/internal/safepoint"
)

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

func newTestPageCache(t *testing.T) (*cached.PageCache, *safepoint.Safepoint) {
	t.Helper()
	dir := shortTempDir(t)
	sp := safepoint.New()
	cache, err := cached.NewPageCache(dir, sp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache, sp
}

func TestDiskCachePageRoundTrip(t *testing.T) {
	cache, _ := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xAB}, cached.SlotSize)
	cache.PutPage("tl", 1, page)

	got := cache.GetPage("tl", 1)
	if !bytes.Equal(got, page) {
		t.Fatalf("page mismatch")
	}

	cache.InvalidatePage("tl", 1)
	if got := cache.GetPage("tl", 1); got != nil {
		t.Fatalf("expected page miss after delete")
	}
}

func TestGetPageRoundTrip(t *testing.T) {
	cache, _ := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xCD}, cached.SlotSize)
	cache.PutPage("p", 1, page)

	data := cache.GetPage("p", 1)
	if data == nil {
		t.Fatal("expected cache hit")
	}
	if !bytes.Equal(data, page) {
		t.Fatal("cached data mismatch")
	}

	if got := cache.GetPage("missing", 0); got != nil {
		t.Fatal("expected nil for cache miss")
	}
}

func TestGetPageReturnsCopy(t *testing.T) {
	cache, _ := newTestPageCache(t)

	page := bytes.Repeat([]byte{0x11}, cached.SlotSize)
	cache.PutPage("t", 1, page)

	data := cache.GetPage("t", 1)
	if data == nil {
		t.Fatal("expected hit")
	}

	// Mutate the returned copy — should not affect the cache.
	data[0] = 0xFF

	got := cache.GetPage("t", 1)
	if got == nil {
		t.Fatal("expected hit on second read")
	}
	if !bytes.Equal(got, page) {
		t.Fatal("mutation of copy should not affect cached data")
	}
}

func TestDiskCacheConcurrentAccess(t *testing.T) {
	cache, _ := newTestPageCache(t)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			page := bytes.Repeat([]byte{byte(i)}, cached.SlotSize)
			cache.PutPage("t", uint64(i), page)
			got := cache.GetPage("t", uint64(i))
			if !bytes.Equal(got, page) {
				t.Errorf("page %d mismatch", i)
			}
		}(i)
	}
	wg.Wait()
}

func TestPageCachePersistence(t *testing.T) {
	dir := shortTempDir(t)
	sp := safepoint.New()

	cache, err := cached.NewPageCache(dir, sp)
	if err != nil {
		t.Fatal(err)
	}

	page := bytes.Repeat([]byte{0xAB}, cached.SlotSize)
	cache.PutPage("tl", 1, page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	cache2, err := cached.NewPageCache(dir, sp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	got := cache2.GetPage("tl", 1)
	if !bytes.Equal(got, page) {
		t.Fatalf("page not persisted across restart")
	}
}

func TestPageCacheMultiClient(t *testing.T) {
	dir := shortTempDir(t)
	sp := safepoint.New()

	cache1, err := cached.NewPageCache(dir, sp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache1.Close() })

	page := bytes.Repeat([]byte{0xCD}, cached.SlotSize)
	cache1.PutPage("shared", 1, page)

	cache2, err := cached.NewPageCache(dir, sp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	got := cache2.GetPage("shared", 1)
	if !bytes.Equal(got, page) {
		t.Fatalf("second client should see first client's page")
	}
}

func TestPageCacheCloseIsIdempotent(t *testing.T) {
	cache, _ := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xEE}, cached.SlotSize)
	cache.PutPage("t", 1, page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	cache.PutPage("t", 2, page)
	if got := cache.GetPage("t", 1); got != nil {
		t.Fatalf("expected nil after close")
	}

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageCacheGetPageRef(t *testing.T) {
	cache, sp := newTestPageCache(t)

	page := bytes.Repeat([]byte{0xDD}, cached.SlotSize)
	cache.PutPage("ref", 1, page)

	g := sp.Enter()
	data := cache.GetPageRef(g, "ref", 1)
	if data == nil {
		g.Exit()
		t.Fatal("expected hit")
	}

	if !bytes.Equal(data, page) {
		g.Exit()
		t.Fatal("ref data mismatch")
	}
	g.Exit()

	// Miss should return nil.
	g = sp.Enter()
	data = cache.GetPageRef(g, "missing", 0)
	g.Exit()
	if data != nil {
		t.Fatal("expected nil for cache miss")
	}
}

func TestPageCacheCountPages(t *testing.T) {
	cache, _ := newTestPageCache(t)

	for i := range 5 {
		cache.PutPage("t", uint64(i), bytes.Repeat([]byte{byte(i)}, cached.SlotSize))
	}

	count, err := cache.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("expected 5 pages, got %d", count)
	}

	cache.InvalidatePage("t", 2)
	count, err = cache.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("expected 4 pages after delete, got %d", count)
	}
}
