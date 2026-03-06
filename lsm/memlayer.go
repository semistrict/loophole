//go:build !js

package lsm

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// MemLayer is the in-memory write buffer backed by a memory-mapped file on
// local disk. Pages are stored in fixed-size slots within the mapping, and
// an in-memory index maps page addresses to slots. Overwrites to the same
// page reuse the existing slot (in-place update).
//
// All I/O goes through the mmap. Reads return slices directly into the
// mapping (zero-copy). The mu RWMutex protects the index map and frozen
// flag. The pins counter protects the mmap lifetime — cleanup() spins
// until all pins are released before munmapping.
type MemLayer struct {
	mu       sync.RWMutex
	pins     atomic.Int32 // outstanding pinned reads into the mmap
	file     *os.File     // backing file (kept open for the mmap)
	path     string
	mmap     []byte              // memory-mapped region
	index    map[uint64]memEntry // pageAddr → latest entry
	nextSlot int                 // bump allocator for slots
	maxPages int
	startSeq uint64
	endSeq   uint64 // set when frozen
	size     atomic.Int64
	frozen   bool
	closed   atomic.Bool // set when cleanup() is called
}

// newMemLayer creates a new memLayer backed by a memory-mapped file on local disk.
// maxPages is the maximum number of unique pages that can be stored.
func newMemLayer(dir string, startSeq uint64, maxPages int) (*MemLayer, error) {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	path := filepath.Join(dir, fmt.Sprintf("%016x-%s.ephemeral", startSeq, hex.EncodeToString(rnd[:])))

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	sz := int64(maxPages) * PageSize
	if err := f.Truncate(sz); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}

	mapping, err := unix.Mmap(int(f.Fd()), 0, int(sz), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("mmap memlayer: %w", err)
	}

	return &MemLayer{
		file:     f,
		path:     path,
		mmap:     mapping,
		index:    make(map[uint64]memEntry),
		maxPages: maxPages,
		startSeq: startSeq,
	}, nil
}

// put writes a page into the mmap and updates the index.
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
		slot = existing.slot
	} else {
		if ml.nextSlot >= ml.maxPages {
			return ErrMemLayerFull
		}
		slot = ml.nextSlot
		ml.nextSlot++
		ml.size.Add(PageSize)
	}

	off := slot * PageSize
	copy(ml.mmap[off:off+PageSize], data)

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

// readData returns a copy of the page data for an entry from the mmap.
func (ml *MemLayer) readData(e memEntry) ([]byte, error) {
	if e.tombstone {
		return zeroPage[:], nil
	}

	// Pin first, then check closed — prevents a TOCTOU race where
	// cleanup() could munmap between a closed check and the pin.
	ml.pins.Add(1)
	defer ml.pins.Add(-1)

	if ml.closed.Load() {
		return nil, errMemLayerCleanedUp
	}

	off := e.slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, ml.mmap[off:off+PageSize])
	return buf, nil
}

// readDataPinned returns a pinned slice into the mmap along with a release
// function. The pins counter is incremented to prevent cleanup() from
// munmapping the memory while the caller uses the slice.
// Callers MUST call the release function when done with the data.
func (ml *MemLayer) readDataPinned(e memEntry) ([]byte, func(), error) {
	if e.tombstone {
		return zeroPage[:], func() {}, nil
	}

	// Pin first, then check closed — prevents a TOCTOU race where
	// cleanup() could munmap between a closed check and the pin.
	ml.pins.Add(1)

	if ml.closed.Load() {
		ml.pins.Add(-1)
		return nil, nil, errMemLayerCleanedUp
	}

	off := e.slot * PageSize
	return ml.mmap[off : off+PageSize], func() { ml.pins.Add(-1) }, nil
}

// freeze marks the memLayer as read-only and records endSeq.
func (ml *MemLayer) freeze(endSeq uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.frozen = true
	ml.endSeq = endSeq
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

// cleanup unmaps the memory, closes the backing file, and removes it.
// It marks the layer as closed, then waits for all pinned reads to
// release before munmapping.
func (ml *MemLayer) cleanup() {
	ml.closed.Store(true)

	// Wait for all pinned readers to release (with bounded spin).
	for i := 0; ml.pins.Load() > 0; i++ {
		runtime.Gosched()
		if i > 1_000_000 {
			panic(fmt.Sprintf("memlayer cleanup: %d pins still held after spin limit", ml.pins.Load()))
		}
	}

	if ml.mmap != nil {
		_ = unix.Munmap(ml.mmap)
		ml.mmap = nil
	}
	_ = ml.file.Close()
	_ = os.Remove(ml.path)
}
