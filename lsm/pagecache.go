package lsm

import (
	"math/rand/v2"
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
type PageCache struct {
	mu        sync.Mutex
	maxPages  int
	data      []byte               // mmap'd region (maxPages * PageSize)
	file      *os.File             // backing file (kept open)
	path      string               // backing file path
	index     map[pageCacheKey]int // key → slot index
	slots     []pageCacheKey       // slot → key (reverse index for eviction)
	freeSlots []int                // stack of free slot indices
	rng       *rand.Rand           // for random eviction
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
		maxPages:  maxPages,
		data:      data,
		file:      f,
		path:      path,
		index:     make(map[pageCacheKey]int, maxPages),
		slots:     make([]pageCacheKey, maxPages),
		freeSlots: freeSlots,
		rng:       rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
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
	out := make([]byte, PageSize)
	copy(out, pc.data[slot*PageSize:])
	return out
}

// Put inserts or updates a page in the cache, randomly evicting if full.
func (pc *PageCache) Put(timelineID string, pageAddr uint64, data []byte) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	key := pageCacheKey{timelineID, pageAddr}

	// Update in place if already cached.
	if slot, ok := pc.index[key]; ok {
		copy(pc.data[slot*PageSize:], data)
		return
	}

	// Get a free slot, or randomly evict one.
	var slot int
	if n := len(pc.freeSlots); n > 0 {
		slot = pc.freeSlots[n-1]
		pc.freeSlots = pc.freeSlots[:n-1]
	} else {
		slot = pc.rng.IntN(pc.maxPages)
		evictedKey := pc.slots[slot]
		delete(pc.index, evictedKey)
	}

	copy(pc.data[slot*PageSize:], data)
	pc.index[key] = slot
	pc.slots[slot] = key
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
