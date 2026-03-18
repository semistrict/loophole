package storage

import (
	"log/slog"
	"runtime"
	"sync"
	"time"
)

const (
	fallbackBudget = 256 << 20
	budgetInterval = 30 * time.Second
)

// cacheKey identifies a cached logical page for an immutable source layer.
// Writable layers do not use the persistent page cache.
type cacheKey struct {
	LayerID string
	PageIdx PageIdx
}

type cacheSlotRef struct {
	Slot       int
	Generation uint64
}

// cacheStore is the storage backend for PageCache.
type cacheStore interface {
	// Arena
	ReadSlot(ref cacheSlotRef) ([]byte, error)
	PrepareSlot(slot int, data []byte) (cacheSlotRef, error)
	AllocArena(maxSlots int) error
	ArenaSlots() int

	// Index
	LookupPage(key cacheKey) (ref cacheSlotRef, ok bool, err error)
	SetPage(key cacheKey, ref cacheSlotRef) error
	DeletePage(key cacheKey) (ref cacheSlotRef, err error)
	BumpCredits(keys []cacheKey) error
	AgeCredits() error
	EvictLow(count int) (freedSlots []int, err error)
	CountPages() (int, error)
	UsedSlots() ([]int, error)
	LockMutation() error
	UnlockMutation() error

	// Disk
	FreeSpace() int64
	MinReserve() int64
	Close() error
}

// PageCache is a daemon-wide shared on-disk cache for fixed-size pages.
// The index is stored in SQLite (WAL mode) so multiple processes can share
// the cache and pages persist across restarts.
type PageCache struct {
	mu         sync.Mutex
	store      cacheStore
	usedBytes  int64
	budget     int64
	budgetAt   time.Time // when budget was last computed
	arenaSlots int
	freeSlots  []int
	accessBuf  []cacheKey
	closed     bool
	stopCh     chan struct{}
	doneCh     chan struct{}
}

func NewPageCache(dir string) (*PageCache, error) {
	store, err := newDefaultStore(dir)
	if err != nil {
		return nil, err
	}
	cache, err := newPageCacheWithStore(store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return cache, nil
}

func newPageCacheWithStore(store cacheStore) (*PageCache, error) {
	c := &PageCache{
		store:  store,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	c.budget = c.computeBudgetLocked()
	c.budgetAt = time.Now()
	c.arenaSlots = c.computeArenaSlotsLocked()
	if err := c.store.AllocArena(c.arenaSlots); err != nil {
		return nil, err
	}
	c.arenaSlots = c.store.ArenaSlots()

	if err := c.rebuildFreeSlotsLocked(); err != nil {
		return nil, err
	}

	go c.runBudgetLoop()
	return c, nil
}

func (c *PageCache) rebuildFreeSlotsLocked() error {
	usedSlots, err := c.store.UsedSlots()
	if err != nil {
		return err
	}
	used := make(map[int]struct{}, len(usedSlots))
	for _, s := range usedSlots {
		used[s] = struct{}{}
	}
	c.freeSlots = c.freeSlots[:0]
	for i := c.arenaSlots - 1; i >= 0; i-- {
		if _, ok := used[i]; !ok {
			c.freeSlots = append(c.freeSlots, i)
		}
	}
	c.usedBytes = int64(len(usedSlots)) * PageSize
	return nil
}

func (c *PageCache) GetPage(key cacheKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}

	ref, ok, err := c.store.LookupPage(key)
	if !ok || err != nil {
		return nil
	}
	c.accessBuf = append(c.accessBuf, key)
	data, err := c.store.ReadSlot(ref)
	if err != nil || len(data) != PageSize {
		return nil
	}
	return data
}

func (c *PageCache) PutPage(key cacheKey, data []byte) {
	if len(data) != PageSize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	if err := c.store.LockMutation(); err != nil {
		return
	}
	defer func() {
		_ = c.store.UnlockMutation()
	}()

	// Another process may have populated this key while we were waiting.
	if existing, ok, _ := c.store.LookupPage(key); ok {
		ref, err := c.store.PrepareSlot(existing.Slot, data)
		if err != nil {
			return
		}
		if err := c.store.SetPage(key, ref); err != nil {
			return
		}
		c.accessBuf = append(c.accessBuf, key)
		return
	}
	if err := c.rebuildFreeSlotsLocked(); err != nil {
		return
	}
	if !c.ensureCapacityLocked(PageSize) {
		return
	}
	slot, ok := c.allocSlotLocked()
	if !ok {
		return
	}
	ref, err := c.store.PrepareSlot(slot, data)
	if err != nil {
		c.freeSlots = append(c.freeSlots, slot)
		return
	}
	if err := c.store.SetPage(key, ref); err != nil {
		// UNIQUE violation (another process) or other error — give back the slot.
		c.freeSlots = append(c.freeSlots, ref.Slot)
		return
	}
	c.usedBytes += PageSize
}

func (c *PageCache) DeletePage(key cacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.LockMutation(); err != nil {
		return
	}
	defer func() {
		_ = c.store.UnlockMutation()
	}()

	ref, err := c.store.DeletePage(key)
	if err != nil {
		return
	}
	c.usedBytes -= PageSize
	c.freeSlots = append(c.freeSlots, ref.Slot)
}

func (c *PageCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.stopCh)
	<-c.doneCh

	c.mu.Lock()
	c.freeSlots = nil
	c.accessBuf = nil
	c.mu.Unlock()

	return c.store.Close()
}

func (c *PageCache) runBudgetLoop() {
	ticker := time.NewTicker(budgetInterval)
	defer ticker.Stop()
	defer close(c.doneCh)

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.flushCreditsLocked()
			_ = c.store.AgeCredits()
			_ = c.rebuildFreeSlotsLocked()

			oldBudget := c.budget
			oldUsed := c.usedBytes
			oldCount, _ := c.store.CountPages()
			c.budget = c.computeBudgetLocked()
			c.budgetAt = time.Now()
			if err := c.store.LockMutation(); err == nil {
				c.evictUntilWithinBudgetLocked()
				_ = c.store.UnlockMutation()
			}
			newCount, _ := c.store.CountPages()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			slog.Debug("pagecache: budget adjusted",
				"budget_mb", c.budget>>20,
				"old_budget_mb", oldBudget>>20,
				"used_mb", c.usedBytes>>20,
				"entries", newCount,
				"evicted", oldCount-newCount,
				"freed_mb", (oldUsed-c.usedBytes)>>20,
				"free_space_mb", c.store.FreeSpace()>>20,
				"heap_mb", m.HeapAlloc>>20,
				"sys_mb", m.Sys>>20,
				"goroutines", runtime.NumGoroutine(),
			)
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

const maxBudget = 2 * 1024 * 1024 * 1024 // 2GB

func (c *PageCache) computeBudgetLocked() int64 {
	freeSpace := c.store.FreeSpace()
	if freeSpace <= 0 {
		return fallbackBudget
	}
	available := freeSpace + c.usedBytes
	budget := available * 3 / 4
	reserveBudget := available - c.store.MinReserve()
	if reserveBudget < budget {
		budget = reserveBudget
	}
	if budget < 0 {
		return 0
	}
	if budget > maxBudget {
		budget = maxBudget
	}
	return budget
}

func (c *PageCache) computeArenaSlotsLocked() int {
	slots := int((c.budget / PageSize) / 2)
	if slots < 1 {
		return 1
	}
	return slots
}

func (c *PageCache) ensureCapacityLocked(delta int64) bool {
	if delta <= 0 {
		return true
	}
	// Throttle Statfs calls: only recompute budget if stale (>1s).
	if now := time.Now(); now.Sub(c.budgetAt) > time.Second {
		c.budget = c.computeBudgetLocked()
		c.budgetAt = now
	}
	if delta > c.budget {
		return false
	}
	c.evictLocked(delta)
	return c.usedBytes+delta <= c.budget
}

func (c *PageCache) evictUntilWithinBudgetLocked() {
	c.evictLocked(0)
}

func (c *PageCache) flushCreditsLocked() {
	if len(c.accessBuf) > 0 {
		_ = c.store.BumpCredits(c.accessBuf)
		c.accessBuf = c.accessBuf[:0]
	}
}

func (c *PageCache) evictLocked(delta int64) {
	c.flushCreditsLocked()
	for c.usedBytes+delta > c.budget {
		count := int((c.usedBytes + delta - c.budget) / PageSize)
		if count < 1 {
			count = 1
		}
		freed, err := c.store.EvictLow(count)
		if err != nil || len(freed) == 0 {
			return
		}
		c.freeSlots = append(c.freeSlots, freed...)
		c.usedBytes -= int64(len(freed)) * PageSize
	}
}

func (c *PageCache) allocSlotLocked() (int, bool) {
	if n := len(c.freeSlots); n > 0 {
		slot := c.freeSlots[n-1]
		c.freeSlots = c.freeSlots[:n-1]
		return slot, true
	}
	// No free slots — flush credits and evict one page.
	c.flushCreditsLocked()
	freed, err := c.store.EvictLow(1)
	if err != nil || len(freed) == 0 {
		return 0, false
	}
	c.usedBytes -= int64(len(freed)) * PageSize
	slot := freed[0]
	if len(freed) > 1 {
		c.freeSlots = append(c.freeSlots, freed[1:]...)
	}
	return slot, true
}
