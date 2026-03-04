package lsm

import (
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// pageCacheKey identifies a cached page.
type pageCacheKey struct {
	timelineID string
	pageAddr   uint64
}

// PageCache is a fixed-size page cache backed by a memory-mapped file.
// Data lives off-heap (zero GC pressure). The backing file is deleted and
// recreated on every startup — the cache is write-through from S3, so stale
// data after a crash is unsafe.
//
// Eviction uses the CLOCK algorithm (approximate LRU). Each slot has a
// referenced bit that is set on access. The clock hand sweeps slots: if
// referenced, the bit is cleared and the hand advances; if not referenced,
// that slot is evicted.
type PageCache struct {
	mu         sync.Mutex
	maxPages   int
	data       []byte               // mmap'd region (maxPages * PageSize)
	file       *os.File             // backing file (kept open)
	path       string               // backing file path
	index      map[pageCacheKey]int // key → slot index
	slots      []pageCacheKey       // slot → key (reverse index for eviction)
	referenced []bool               // CLOCK referenced bit per slot
	clockHand  int                  // current CLOCK sweep position
	freeSlots  []int                // stack of free slot indices
}

// NewPageCache creates a page cache backed by a memory-mapped file at path.
// Any existing file at path is deleted first.
func NewPageCache(path string, maxBytes int64) (*PageCache, error) {
	maxPages := int(maxBytes / PageSize)
	if maxPages < 1 {
		maxPages = 1
	}

	// Ensure parent directory exists and delete any stale file from a previous run.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	_ = os.Remove(path)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	size := int64(maxPages) * PageSize
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}

	// Initialize all slots as free (in reverse order so slot 0 is popped first).
	freeSlots := make([]int, maxPages)
	for i := range maxPages {
		freeSlots[i] = maxPages - 1 - i
	}

	return &PageCache{
		maxPages:   maxPages,
		data:       data,
		file:       f,
		path:       path,
		index:      make(map[pageCacheKey]int, maxPages),
		slots:      make([]pageCacheKey, maxPages),
		referenced: make([]bool, maxPages),
		freeSlots:  freeSlots,
	}, nil
}

// Get returns the cached page data, or nil if not cached.
func (pc *PageCache) Get(timelineID string, pageAddr uint64) []byte {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	key := pageCacheKey{timelineID, pageAddr}
	slot, ok := pc.index[key]
	if !ok {
		return nil
	}
	pc.referenced[slot] = true
	out := make([]byte, PageSize)
	copy(out, pc.data[slot*PageSize:])
	return out
}

// Put inserts or updates a page in the cache, using CLOCK eviction if full.
func (pc *PageCache) Put(timelineID string, pageAddr uint64, data []byte) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	key := pageCacheKey{timelineID, pageAddr}

	// Update in place if already cached.
	if slot, ok := pc.index[key]; ok {
		copy(pc.data[slot*PageSize:], data)
		pc.referenced[slot] = true
		return
	}

	// Get a free slot, or evict using CLOCK.
	var slot int
	if n := len(pc.freeSlots); n > 0 {
		slot = pc.freeSlots[n-1]
		pc.freeSlots = pc.freeSlots[:n-1]
	} else {
		slot = pc.clockEvict()
	}

	copy(pc.data[slot*PageSize:], data)
	pc.index[key] = slot
	pc.slots[slot] = key
	pc.referenced[slot] = true
}

// clockEvict finds a slot to evict using the CLOCK algorithm.
// Caller must hold pc.mu.
func (pc *PageCache) clockEvict() int {
	for {
		slot := pc.clockHand
		pc.clockHand = (pc.clockHand + 1) % pc.maxPages
		if pc.referenced[slot] {
			pc.referenced[slot] = false
			continue
		}
		evictedKey := pc.slots[slot]
		delete(pc.index, evictedKey)
		return slot
	}
}

// Delete removes a page from the cache if present.
func (pc *PageCache) Delete(timelineID string, pageAddr uint64) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	key := pageCacheKey{timelineID, pageAddr}
	slot, ok := pc.index[key]
	if !ok {
		return
	}
	delete(pc.index, key)
	pc.slots[slot] = pageCacheKey{}
	pc.referenced[slot] = false
	pc.freeSlots = append(pc.freeSlots, slot)
}

// Close unmaps the memory, closes the file, and removes the backing file.
func (pc *PageCache) Close() error {
	if err := unix.Munmap(pc.data); err != nil {
		return err
	}
	pc.data = nil
	if err := pc.file.Close(); err != nil {
		return err
	}
	return os.Remove(pc.path)
}
