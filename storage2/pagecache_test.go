package storage2

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
)

func pk(tl string, idx PageIdx) cacheKey {
	return cacheKey{LayerID: tl, PageIdx: idx}
}

type mockPage struct {
	slot    int
	credits int
}

type mockStore struct {
	slots     map[int][]byte
	pages     map[cacheKey]*mockPage
	maxSlots  int
	freeSpace int64
	reserve   int64
	freeFn    func() int64
}

func newMockStore(freeSpace, reserve int64) *mockStore {
	return &mockStore{
		slots:     make(map[int][]byte),
		pages:     make(map[cacheKey]*mockPage),
		freeSpace: freeSpace,
		reserve:   reserve,
	}
}

func (s *mockStore) AllocArena(maxSlots int) error {
	s.maxSlots = maxSlots
	return nil
}

func (s *mockStore) ArenaSlots() int { return s.maxSlots }

func (s *mockStore) ReadSlot(slot int) ([]byte, error) {
	data, ok := s.slots[slot]
	if !ok {
		return nil, fmt.Errorf("slot %d not found", slot)
	}
	return bytes.Clone(data), nil
}

func (s *mockStore) WriteSlot(slot int, data []byte) error {
	s.slots[slot] = bytes.Clone(data)
	return nil
}

func (s *mockStore) LookupPage(key cacheKey) (int, bool, error) {
	p, ok := s.pages[key]
	if !ok {
		return 0, false, nil
	}
	return p.slot, true, nil
}

func (s *mockStore) InsertPage(key cacheKey, slot int) error {
	s.pages[key] = &mockPage{slot: slot, credits: 1}
	return nil
}

func (s *mockStore) DeletePage(key cacheKey) (int, error) {
	p, ok := s.pages[key]
	if !ok {
		return -1, fmt.Errorf("page not found")
	}
	delete(s.pages, key)
	return p.slot, nil
}

func (s *mockStore) BumpCredits(keys []cacheKey) error {
	counts := make(map[cacheKey]int, len(keys))
	for _, k := range keys {
		counts[k]++
	}
	for k, n := range counts {
		if p, ok := s.pages[k]; ok {
			p.credits += n
		}
	}
	return nil
}

func (s *mockStore) AgeCredits() error {
	for _, p := range s.pages {
		if p.credits > 0 {
			p.credits--
		}
	}
	return nil
}

func (s *mockStore) EvictLow(count int) ([]int, error) {
	type kv struct {
		key     cacheKey
		credits int
		slot    int
	}
	var items []kv
	for k, p := range s.pages {
		items = append(items, kv{key: k, credits: p.credits, slot: p.slot})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].credits != items[j].credits {
			return items[i].credits < items[j].credits
		}
		if items[i].key.LayerID != items[j].key.LayerID {
			return items[i].key.LayerID < items[j].key.LayerID
		}
		return items[i].key.PageIdx < items[j].key.PageIdx
	})
	if count > len(items) {
		count = len(items)
	}
	var freed []int
	for i := 0; i < count; i++ {
		delete(s.pages, items[i].key)
		freed = append(freed, items[i].slot)
	}
	return freed, nil
}

func (s *mockStore) CountPages() (int, error) {
	return len(s.pages), nil
}

func (s *mockStore) UsedSlots() ([]int, error) {
	var slots []int
	for _, p := range s.pages {
		slots = append(slots, p.slot)
	}
	return slots, nil
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
	cache, err := newPageCacheWithStore(newMockStore(8*PageSize, 0))
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

func TestPageCacheEvictionPrefersCold(t *testing.T) {
	store := newMockStore(32*PageSize, 0)
	cache, err := newPageCacheWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	store.freeFn = func() int64 { return 32*PageSize - cache.usedBytes }

	hot := bytes.Repeat([]byte{0x11}, PageSize)
	cold := bytes.Repeat([]byte{0x22}, PageSize)

	cache.PutPage(pk("t", 1), hot)
	// Bump hot page credits.
	for range 10 {
		_ = cache.GetPage(pk("t", 1))
	}
	cache.PutPage(pk("t", 2), cold)

	// Age credits so cold (credits=1→0) is clearly below fill pages (credits=1).
	cache.mu.Lock()
	cache.flushCreditsLocked()
	_ = cache.store.AgeCredits()
	cache.mu.Unlock()

	// Fill remaining slots to force eviction.
	for i := PageIdx(10); i < 30; i++ {
		cache.PutPage(pk("t", i), bytes.Repeat([]byte{byte(i)}, PageSize))
	}

	if got := cache.GetPage(pk("t", 1)); !bytes.Equal(got, hot) {
		t.Fatalf("expected hot page to stay cached")
	}
	if got := cache.GetPage(pk("t", 2)); got != nil {
		t.Fatalf("expected cold page to be evicted")
	}
}

func TestPageCacheBudgetShrinkEvictsUntilWithinBudget(t *testing.T) {
	store := newMockStore(8*PageSize, 0)
	cache, err := newPageCacheWithStore(store)
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
	for _, key := range []cacheKey{pk("t", 1), pk("t", 2), pk("t", 3)} {
		if got := cache.GetPage(key); got == nil {
			evicted++
		}
	}
	if evicted == 0 {
		t.Fatalf("expected at least one cold page eviction after budget shrink")
	}
}

func TestPageCacheCloseStopsBudgetLoop(t *testing.T) {
	cache, err := newPageCacheWithStore(newMockStore(8*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}

	page := bytes.Repeat([]byte{0xEE}, PageSize)
	cache.PutPage(pk("t", 1), page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cache.doneCh:
	default:
		t.Fatal("budget loop did not stop")
	}

	cache.PutPage(pk("t", 2), page)
	if got := cache.GetPage(pk("t", 1)); got != nil {
		t.Fatalf("expected cache to be empty after close")
	}
	if got := cache.GetPage(pk("t", 2)); got != nil {
		t.Fatalf("expected writes after close to be ignored")
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetPagePinnedRoundTrip(t *testing.T) {
	cache, err := newPageCacheWithStore(newMockStore(8*PageSize, 0))
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

func TestGetPagePinnedReturnsCopy(t *testing.T) {
	store := newMockStore(8*PageSize, 0)
	cache, err := newPageCacheWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	page := bytes.Repeat([]byte{0x11}, PageSize)
	cache.PutPage(pk("t", 1), page)

	// Get a "pinned" copy.
	data, release := cache.GetPagePinned(pk("t", 1))
	if data == nil {
		t.Fatal("expected hit")
	}
	release()

	// Evict the original page from cache.
	cache.mu.Lock()
	cache.budget = 0
	cache.evictUntilWithinBudgetLocked()
	cache.mu.Unlock()

	// The copy should still hold valid data even though the cache page was evicted.
	if !bytes.Equal(data, page) {
		t.Fatal("copy should survive eviction of original")
	}
}

func TestDiskCacheConcurrentAccess(t *testing.T) {
	cache, err := newPageCacheWithStore(newMockStore(256*PageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

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
	dir := t.TempDir()

	cache, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	page := bytes.Repeat([]byte{0xAB}, PageSize)
	cache.PutPage(pk("tl", 1), page)

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — data should still be there.
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

func TestPageCacheMultiInstance(t *testing.T) {
	dir := t.TempDir()

	cache1, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache1.Close() })

	page := bytes.Repeat([]byte{0xCD}, PageSize)
	cache1.PutPage(pk("shared", 1), page)

	// Second instance sharing the same directory.
	cache2, err := NewPageCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	got := cache2.GetPage(pk("shared", 1))
	if !bytes.Equal(got, page) {
		t.Fatalf("second instance should see first instance's page")
	}
}
