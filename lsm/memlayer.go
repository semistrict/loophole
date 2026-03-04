package lsm

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

var errMemLayerCleanedUp = errors.New("memlayer already cleaned up")

// MemLayer is the in-memory write buffer backed by a memory-mapped file.
// Pages are stored in fixed-size slots within the mmap region, and an
// in-memory index maps page addresses to slots. Overwrites to the same
// page reuse the existing slot (in-place update), avoiding the append-only
// amplification of the previous file-based design.
type MemLayer struct {
	mu       sync.RWMutex
	data     []byte   // mmap'd region (maxPages * PageSize)
	file     *os.File // backing file (kept open for mmap lifetime)
	path     string
	index    map[uint64]memEntry // pageAddr → latest entry
	nextSlot int                 // bump allocator for slots
	maxPages int
	startSeq uint64
	endSeq   uint64 // set when frozen
	size     atomic.Int64
	frozen   bool
}

// memEntry is an index entry for a single page in the memLayer.
type memEntry struct {
	seq       uint64
	slot      int  // index into mmap slab
	tombstone bool // true = PunchHole, read as zeros
}

// newMemLayer creates a new memLayer backed by a memory-mapped file.
// maxPages is the maximum number of unique pages that can be stored.
func newMemLayer(dir string, startSeq uint64, maxPages int) (*MemLayer, error) {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	path := filepath.Join(dir, fmt.Sprintf("%016x-%s.ephemeral", startSeq, hex.EncodeToString(rnd[:])))

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
		return nil, fmt.Errorf("mmap memlayer: %w", err)
	}

	return &MemLayer{
		data:     data,
		file:     f,
		path:     path,
		index:    make(map[uint64]memEntry),
		maxPages: maxPages,
		startSeq: startSeq,
	}, nil
}

// put writes a page into the mmap region and updates the index.
// If the page already exists, its slot is reused (in-place overwrite).
func (ml *MemLayer) put(pageAddr, seq uint64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(data))
	}

	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.frozen {
		return fmt.Errorf("memlayer is frozen")
	}

	var slot int
	if existing, ok := ml.index[pageAddr]; ok && !existing.tombstone {
		// Reuse existing slot — in-place overwrite, no size growth.
		slot = existing.slot
	} else {
		// Allocate a new slot.
		if ml.nextSlot >= ml.maxPages {
			return fmt.Errorf("memlayer full: %d/%d slots used", ml.nextSlot, ml.maxPages)
		}
		slot = ml.nextSlot
		ml.nextSlot++
		ml.size.Add(PageSize)
	}

	off := slot * PageSize
	copy(ml.data[off:off+PageSize], data)

	ml.index[pageAddr] = memEntry{seq: seq, slot: slot}
	return nil
}

// putTombstone records a tombstone (PunchHole) for a page.
func (ml *MemLayer) putTombstone(pageAddr, seq uint64) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.frozen {
		return fmt.Errorf("memlayer is frozen")
	}

	ml.index[pageAddr] = memEntry{seq: seq, tombstone: true}
	return nil
}

// isEmpty returns true if the memLayer has no entries (no writes or tombstones).
func (ml *MemLayer) isEmpty() bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.index) == 0
}

// get returns the latest entry for a page, if any.
func (ml *MemLayer) get(pageAddr uint64) (memEntry, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	e, ok := ml.index[pageAddr]
	return e, ok
}

// readData returns a copy of the page data for an entry from the mmap region.
// The copy is made under the read lock to prevent torn reads from concurrent put() calls
// that reuse the same slot for in-place overwrites.
func (ml *MemLayer) readData(e memEntry) ([]byte, error) {
	if e.tombstone {
		return zeroPage[:], nil
	}

	ml.mu.RLock()
	defer ml.mu.RUnlock()

	if ml.data == nil {
		return nil, errMemLayerCleanedUp
	}

	off := e.slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, ml.data[off:off+PageSize])
	return buf, nil
}

// freeze marks the memLayer as read-only and records endSeq.
func (ml *MemLayer) freeze(endSeq uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.frozen = true
	ml.endSeq = endSeq
}

// sortedEntry is a memEntry with its pageAddr, used for serialization.
type sortedEntry struct {
	pageAddr uint64
	memEntry
}

// entries returns all index entries sorted by (pageAddr, seq).
func (ml *MemLayer) entries() []sortedEntry {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	out := make([]sortedEntry, 0, len(ml.index))
	for addr, e := range ml.index {
		out = append(out, sortedEntry{pageAddr: addr, memEntry: e})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].pageAddr != out[j].pageAddr {
			return out[i].pageAddr < out[j].pageAddr
		}
		return out[i].seq < out[j].seq
	})
	return out
}

// cleanup unmaps the mmap region, closes the backing file, and removes it.
func (ml *MemLayer) cleanup() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	_ = unix.Munmap(ml.data)
	ml.data = nil
	_ = ml.file.Close()
	_ = os.Remove(ml.path)
}
