package diskcache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func pk(tl string, addr uint64) CacheKey {
	return CacheKey{Timeline: tl, PageAddr: addr}
}

type mockStore struct {
	mu        sync.Mutex
	slots     map[int][]byte
	blobs     map[string][]byte
	freeSpace int64
	reserve   int64
	freeFn    func() int64
}

func newMockStore(freeSpace, reserve int64) *mockStore {
	return &mockStore{
		slots:     make(map[int][]byte),
		blobs:     make(map[string][]byte),
		freeSpace: freeSpace,
		reserve:   reserve,
	}
}

func (s *mockStore) AllocArena(maxSlots int) error { return nil }

func (s *mockStore) ReadSlot(slot int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.slots[slot]
	if !ok {
		return nil, fmt.Errorf("slot %d not found", slot)
	}
	return bytes.Clone(data), nil
}

func (s *mockStore) ReadSlotPinned(slot int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.slots[slot]
	if !ok {
		return nil, fmt.Errorf("slot %d not found", slot)
	}
	return data, nil
}

func (s *mockStore) WriteSlot(slot int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slots[slot] = bytes.Clone(data)
	return nil
}

func (s *mockStore) ReadBlob(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.blobs[key]
	if !ok {
		return nil, fmt.Errorf("blob %q not found", key)
	}
	return bytes.Clone(data), nil
}

func (s *mockStore) WriteBlob(key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[key] = bytes.Clone(data)
	return nil
}

func (s *mockStore) DeleteBlob(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, key)
	return nil
}

func (s *mockStore) FreeSpace() int64 {
	if s.freeFn != nil {
		return s.freeFn()
	}
	return s.freeSpace
}
func (s *mockStore) MinReserve() int64 { return s.reserve }
func (s *mockStore) Close() error      { return nil }

func TestDiskCachePageRoundTrip(t *testing.T) {
	cache, err := NewWithStore(newMockStore(8*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

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

func TestDiskCacheBlobRoundTrip(t *testing.T) {
	cache, err := NewWithStore(newMockStore(16*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	blob := bytes.Repeat([]byte("x"), 3*PageSize)
	cache.PutBlob("blob", blob)

	got := cache.GetBlob("blob")
	if !bytes.Equal(got, blob) {
		t.Fatalf("blob mismatch")
	}
}

func TestDiskCacheBlobTooLargeIgnored(t *testing.T) {
	cache, err := NewWithStore(newMockStore(32*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	cache.PutBlob("huge", bytes.Repeat([]byte("z"), MaxBlobSize+1))
	if got := cache.GetBlob("huge"); got != nil {
		t.Fatalf("expected oversize blob miss")
	}
}

func TestDiskCacheWeightedEvictionPrefersLargeColdBlob(t *testing.T) {
	store := newMockStore(8*PageSize, 0)
	cache, err := NewWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	store.freeFn = func() int64 { return 8*PageSize - cache.usedBytes }

	hot := bytes.Repeat([]byte{0x11}, PageSize)
	cold := bytes.Repeat([]byte{0x22}, PageSize)
	newPage := bytes.Repeat([]byte{0x44}, PageSize)
	large := bytes.Repeat([]byte{0x33}, 4*PageSize)

	cache.PutPage(pk("t", 1), hot)
	_ = cache.GetPage(pk("t", 1))
	_ = cache.GetPage(pk("t", 1))
	cache.PutBlob("large", large)
	cache.PutPage(pk("t", 2), cold)
	cache.PutPage(pk("t", 3), newPage)

	if got := cache.GetBlob("large"); got != nil {
		t.Fatalf("expected large blob to be evicted")
	}
	if got := cache.GetPage(pk("t", 1)); !bytes.Equal(got, hot) {
		t.Fatalf("expected hot page to stay cached")
	}
	if got := cache.GetPage(pk("t", 2)); !bytes.Equal(got, cold) {
		t.Fatalf("expected cold page to be cached")
	}
	if got := cache.GetPage(pk("t", 3)); !bytes.Equal(got, newPage) {
		t.Fatalf("expected new page to be cached")
	}
}

func TestDiskCacheBudgetShrinkEvictsUntilWithinBudget(t *testing.T) {
	store := newMockStore(8*PageSize, 0)
	cache, err := NewWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	store.freeFn = func() int64 { return 8*PageSize - cache.usedBytes }

	hot := bytes.Repeat([]byte{0xAA}, PageSize)
	cold1 := bytes.Repeat([]byte{0x01}, PageSize)
	cold2 := bytes.Repeat([]byte{0x02}, PageSize)
	cold3 := bytes.Repeat([]byte{0x03}, PageSize)

	cache.PutPage(pk("t", 0), hot)
	cache.PutPage(pk("t", 1), cold1)
	cache.PutPage(pk("t", 2), cold2)
	cache.PutPage(pk("t", 3), cold3)
	_ = cache.GetPage(pk("t", 0))

	store.reserve = 2 * PageSize
	store.freeFn = func() int64 { return PageSize }

	cache.mu.Lock()
	cache.budget = cache.computeBudgetLocked()
	cache.evictUntilWithinBudgetLocked()
	cache.mu.Unlock()

	if cache.budget >= 4*PageSize {
		t.Fatalf("budget = %d, want shrink below current usage", cache.budget)
	}
	if cache.usedBytes > cache.budget {
		t.Fatalf("used bytes = %d, want <= budget %d", cache.usedBytes, cache.budget)
	}

	evicted := 0
	for _, key := range []CacheKey{pk("t", 1), pk("t", 2), pk("t", 3)} {
		if got := cache.GetPage(key); got == nil {
			evicted++
		}
	}
	if evicted == 0 {
		t.Fatalf("expected at least one cold page eviction after budget shrink")
	}
}

func TestDiskCacheCloseStopsBudgetLoop(t *testing.T) {
	cache, err := NewWithStore(newMockStore(8*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}

	cache.PutBlob("blob", []byte("hello"))

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cache.doneCh:
	default:
		t.Fatal("budget loop did not stop")
	}

	cache.PutBlob("after-close", []byte("ignored"))
	if got := cache.GetBlob("blob"); got != nil {
		t.Fatalf("expected cache to be empty after close")
	}
	if got := cache.GetBlob("after-close"); got != nil {
		t.Fatalf("expected writes after close to be ignored")
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetPagePinnedRoundTrip(t *testing.T) {
	cache, err := NewWithStore(newMockStore(8*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	page := bytes.Repeat([]byte{0xCD}, PageSize)
	cache.PutPage(pk("p", 1), page)

	data, release := cache.GetPagePinned(pk("p", 1))
	if data == nil {
		t.Fatal("expected pinned hit")
	}
	if !bytes.Equal(data, page) {
		t.Fatal("pinned data mismatch")
	}
	release()

	// Miss returns nil, nil.
	data, release = cache.GetPagePinned(pk("missing", 0))
	if data != nil || release != nil {
		t.Fatal("expected nil for cache miss")
	}
}

func TestGetPagePinnedEvictionSkipsPinned(t *testing.T) {
	store := newMockStore(8*PageSize, 0)
	cache, err := NewWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	store.freeFn = func() int64 { return 8*PageSize - cache.usedBytes }

	// Fill with 2 pages.
	pinnedPage := bytes.Repeat([]byte{0x11}, PageSize)
	otherPage := bytes.Repeat([]byte{0x22}, PageSize)
	cache.PutPage(pk("t", 1), pinnedPage)
	cache.PutPage(pk("t", 2), otherPage)

	// Pin one entry.
	data, release := cache.GetPagePinned(pk("t", 1))
	if data == nil {
		t.Fatal("expected pinned hit")
	}

	// Shrink budget to force eviction — only room for 1 page.
	store.reserve = 6 * PageSize
	store.freeFn = func() int64 { return 6*PageSize - cache.usedBytes }
	cache.mu.Lock()
	cache.budget = cache.computeBudgetLocked()
	cache.evictUntilWithinBudgetLocked()
	cache.mu.Unlock()

	// The non-pinned page should be evicted, pinned should survive.
	if got := cache.GetPage(pk("t", 2)); got != nil {
		t.Fatal("expected non-pinned page to be evicted")
	}
	if got := cache.GetPage(pk("t", 1)); !bytes.Equal(got, pinnedPage) {
		t.Fatal("expected pinned page to survive eviction")
	}

	// Release the pin — now it can be evicted.
	release()

	cache.mu.Lock()
	cache.budget = 0
	cache.evictUntilWithinBudgetLocked()
	cache.mu.Unlock()

	if got := cache.GetPage(pk("t", 1)); got != nil {
		t.Fatal("expected page to be evictable after release")
	}
}

func TestDiskCacheConcurrentAccess(t *testing.T) {
	cache, err := NewWithStore(newMockStore(256*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := pk("t", uint64(i))
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
