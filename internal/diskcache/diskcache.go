package diskcache

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PageSize       = 4096
	MaxBlobSize    = 16 << 20
	fallbackBudget = 256 << 20
	budgetInterval = 30 * time.Second
)

type entryKind uint8

const (
	entryKindPage entryKind = iota + 1
	entryKindBlob
)

// CacheKey identifies an entry in the DiskCache.
// For pages: Timeline and PageAddr are set, BlobKey is empty.
// For blobs: BlobKey is set, Timeline is empty and PageAddr is 0.
type CacheKey struct {
	Timeline string
	PageAddr uint64
	BlobKey  string
}

type entry struct {
	key       CacheKey
	kind      entryKind
	size      int64
	arenaSlot int
	credits   int32
	ringIdx   int
	pinCount  atomic.Int32
}

// DiskCache is a daemon-wide shared on-disk cache for fixed-size pages and blobs.
// The in-memory index is process-local, so cache contents are discarded on startup.
type DiskCache struct {
	mu        sync.Mutex
	store     Store
	usedBytes int64
	budget    int64

	arenaSlots int
	freeSlots  []int

	entries    map[CacheKey]*entry
	ring       []*entry
	tombstones int
	clockHand  int
	closed     bool
	stopCh     chan struct{}
	doneCh     chan struct{}
}

func New(dir string) (*DiskCache, error) {
	store, err := newDefaultStore(dir)
	if err != nil {
		return nil, err
	}
	cache, err := NewWithStore(store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return cache, nil
}

func NewWithStore(store Store) (*DiskCache, error) {
	c := &DiskCache{
		store:   store,
		entries: make(map[CacheKey]*entry),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	c.budget = c.computeBudgetLocked()
	c.arenaSlots = c.computeArenaSlotsLocked()
	if err := c.store.AllocArena(c.arenaSlots); err != nil {
		return nil, err
	}
	c.freeSlots = make([]int, c.arenaSlots)
	for i := range c.arenaSlots {
		c.freeSlots[i] = c.arenaSlots - 1 - i
	}

	go c.runBudgetLoop()
	return c, nil
}

func (c *DiskCache) GetPage(key CacheKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.entries[key]
	if e == nil || e.kind != entryKindPage {
		return nil
	}
	c.bumpCreditsLocked(e)
	data, err := c.store.ReadSlot(e.arenaSlot)
	if err != nil || len(data) != PageSize {
		return nil
	}
	return data
}

// GetPagePinned returns a pinned zero-copy slice into the arena mmap.
// The returned release function MUST be called when the caller is done with the data.
// Returns (nil, nil) on cache miss.
func (c *DiskCache) GetPagePinned(key CacheKey) ([]byte, func()) {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil || e.kind != entryKindPage {
		c.mu.Unlock()
		return nil, nil
	}
	c.bumpCreditsLocked(e)
	e.pinCount.Add(1)
	slot := e.arenaSlot
	c.mu.Unlock()

	data, err := c.store.ReadSlotPinned(slot)
	if err != nil {
		e.pinCount.Add(-1)
		return nil, nil
	}
	return data, func() { e.pinCount.Add(-1) }
}

func (c *DiskCache) PutPage(key CacheKey, data []byte) {
	if len(data) != PageSize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	if e := c.entries[key]; e != nil && e.kind == entryKindPage {
		if err := c.store.WriteSlot(e.arenaSlot, data); err != nil {
			return
		}
		c.bumpCreditsLocked(e)
		return
	}

	if !c.ensureCapacityLocked(int64(len(data))) {
		return
	}
	slot, ok := c.ensurePageSlotLocked()
	if !ok {
		return
	}
	if err := c.store.WriteSlot(slot, data); err != nil {
		c.freeSlots = append(c.freeSlots, slot)
		return
	}

	c.replaceEntryLocked(key, &entry{
		key:       key,
		kind:      entryKindPage,
		size:      int64(len(data)),
		arenaSlot: slot,
		credits:   1,
		ringIdx:   -1,
	})
}

func (c *DiskCache) DeletePage(key CacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.entries[key]
	if e == nil || e.kind != entryKindPage {
		return
	}
	c.removeEntryLocked(e)
}

func (c *DiskCache) GetBlob(key string) []byte {
	ck := CacheKey{BlobKey: key}
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.entries[ck]
	if e == nil || e.kind != entryKindBlob {
		return nil
	}
	c.bumpCreditsLocked(e)
	data, err := c.store.ReadBlob(key)
	if err != nil {
		return nil
	}
	return data
}

func (c *DiskCache) PutBlob(key string, data []byte) {
	if len(data) == 0 || len(data) > MaxBlobSize {
		return
	}

	ck := CacheKey{BlobKey: key}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	if e := c.entries[ck]; e != nil && e.kind == entryKindBlob {
		if !c.ensureCapacityLocked(int64(len(data)) - e.size) {
			return
		}
		if err := c.store.WriteBlob(key, data); err != nil {
			return
		}
		c.usedBytes += int64(len(data)) - e.size
		e.size = int64(len(data))
		c.bumpCreditsLocked(e)
		return
	}

	if existing := c.entries[ck]; existing != nil {
		if !c.ensureCapacityLocked(int64(len(data)) - existing.size) {
			return
		}
	} else if !c.ensureCapacityLocked(int64(len(data))) {
		return
	}
	if err := c.store.WriteBlob(key, data); err != nil {
		return
	}

	c.replaceEntryLocked(ck, &entry{
		key:     ck,
		kind:    entryKindBlob,
		size:    int64(len(data)),
		credits: 1,
		ringIdx: -1,
	})
}

func (c *DiskCache) Close() error {
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
	c.entries = nil
	c.ring = nil
	c.freeSlots = nil
	c.mu.Unlock()

	return c.store.Close()
}

func (c *DiskCache) runBudgetLoop() {
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
			c.budget = c.computeBudgetLocked()
			c.evictUntilWithinBudgetLocked()
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

const maxBudget = 2 * 1024 * 1024 * 1024 // 2GB

func (c *DiskCache) computeBudgetLocked() int64 {
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

func (c *DiskCache) computeArenaSlotsLocked() int {
	slots := int((c.budget / PageSize) / 2)
	if slots < 1 {
		return 1
	}
	return slots
}

func (c *DiskCache) ensureCapacityLocked(delta int64) bool {
	if delta <= 0 {
		return true
	}
	c.budget = c.computeBudgetLocked()
	if delta > c.budget {
		return false
	}
	c.evictLocked(delta)
	return c.usedBytes+delta <= c.budget
}

func (c *DiskCache) evictUntilWithinBudgetLocked() {
	c.evictLocked(0)
}

func (c *DiskCache) evictLocked(delta int64) {
	for c.usedBytes+delta > c.budget {
		if len(c.entries) == 0 || len(c.ring) == 0 {
			return
		}
		if !c.evictOneLocked() {
			return
		}
	}
}

func (c *DiskCache) evictOneLocked() bool {
	scanned := 0
	for len(c.ring) > 0 && scanned < len(c.ring)*2 {
		if c.clockHand >= len(c.ring) {
			c.clockHand = 0
		}
		e := c.ring[c.clockHand]
		c.clockHand++
		scanned++
		if e == nil {
			continue
		}

		if e.pinCount.Load() > 0 {
			continue
		}

		if e.credits <= 0 {
			c.removeEntryLocked(e)
			return true
		}

		cost := e.size / PageSize
		if cost < 1 {
			cost = 1
		}
		if cost > math.MaxInt32 {
			cost = math.MaxInt32
		}
		e.credits -= int32(cost)
	}

	for _, e := range c.ring {
		if e == nil {
			continue
		}
		if e.pinCount.Load() > 0 {
			continue
		}
		c.removeEntryLocked(e)
		return true
	}
	return false
}

func (c *DiskCache) takeFreeSlotLocked() (int, bool) {
	n := len(c.freeSlots)
	if n == 0 {
		return 0, false
	}
	slot := c.freeSlots[n-1]
	c.freeSlots = c.freeSlots[:n-1]
	return slot, true
}

func (c *DiskCache) ensurePageSlotLocked() (int, bool) {
	if slot, ok := c.takeFreeSlotLocked(); ok {
		return slot, true
	}
	for len(c.entries) > 0 {
		if !c.evictOneLocked() {
			break
		}
		if slot, ok := c.takeFreeSlotLocked(); ok {
			return slot, true
		}
	}
	return 0, false
}

func (c *DiskCache) replaceEntryLocked(key CacheKey, newEntry *entry) {
	if old := c.entries[key]; old != nil {
		c.removeEntryLocked(old)
	}
	newEntry.ringIdx = len(c.ring)
	c.ring = append(c.ring, newEntry)
	c.entries[key] = newEntry
	c.usedBytes += newEntry.size
}

func (c *DiskCache) removeEntryLocked(e *entry) {
	if current := c.entries[e.key]; current != e {
		return
	}
	delete(c.entries, e.key)
	c.usedBytes -= e.size
	if e.kind == entryKindPage {
		c.freeSlots = append(c.freeSlots, e.arenaSlot)
	} else {
		_ = c.store.DeleteBlob(e.key.BlobKey)
	}
	if e.ringIdx >= 0 && e.ringIdx < len(c.ring) && c.ring[e.ringIdx] == e {
		c.ring[e.ringIdx] = nil
		c.tombstones++
	}
	if len(c.ring) > 0 && c.tombstones*4 > len(c.ring) {
		c.compactRingLocked()
	}
}

func (c *DiskCache) compactRingLocked() {
	if len(c.ring) == 0 {
		c.clockHand = 0
		c.tombstones = 0
		return
	}
	newRing := make([]*entry, 0, len(c.ring)-c.tombstones)
	var current *entry
	currentIdx := c.clockHand
	if currentIdx >= len(c.ring) {
		currentIdx = 0
	}
	if len(c.ring) > 0 {
		current = c.ring[currentIdx]
	}
	for _, e := range c.ring {
		if e == nil {
			continue
		}
		e.ringIdx = len(newRing)
		newRing = append(newRing, e)
	}
	c.ring = newRing
	c.tombstones = 0
	c.clockHand = 0
	if current != nil {
		if idx := current.ringIdx; idx >= 0 && idx < len(c.ring) {
			c.clockHand = idx
		}
	}
}

func (c *DiskCache) bumpCreditsLocked(e *entry) {
	if e.credits < math.MaxInt32 {
		e.credits++
	}
}
