//go:build !js

package storage2

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

// errMemtableFull is returned when the memtable is full.
var errMemtableFull = fmt.Errorf("memtable full")

// memEntry is one entry in the memtable's index.
type memEntry struct {
	slot      int
	tombstone bool
}

// sortedEntry pairs a page address with its memEntry for iteration.
type sortedEntry struct {
	pageIdx PageIdx
	memEntry
}

// memtable is the in-memory write buffer backed by a memory-mapped file.
// Pages are stored in fixed-size slots within the mapping, and an in-memory
// index maps page addresses to slots.
type memtable struct {
	mu       sync.RWMutex
	pins     atomic.Int32
	file     *os.File
	path     string
	mmap     []byte
	index    map[PageIdx]memEntry
	nextSlot int
	maxPages int
	startSeq uint64
	endSeq   uint64
	size     atomic.Int64
	frozen   bool
	closed   atomic.Bool
}

var errmemtableCleanedUp = fmt.Errorf("memtable cleaned up")

// newMemtable creates a new memtable backed by a memory-mapped file.
func newMemtable(dir string, startSeq uint64, maxPages int) (*memtable, error) {
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
		return nil, fmt.Errorf("mmap memtable: %w", err)
	}

	return &memtable{
		file:     f,
		path:     path,
		mmap:     mapping,
		index:    make(map[PageIdx]memEntry),
		maxPages: maxPages,
		startSeq: startSeq,
	}, nil
}

func (mt *memtable) put(pageIdx PageIdx, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(data))
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.frozen {
		return fmt.Errorf("memtable is frozen")
	}

	var slot int
	if existing, ok := mt.index[pageIdx]; ok && !existing.tombstone {
		slot = existing.slot
	} else {
		if mt.nextSlot >= mt.maxPages {
			return errMemtableFull
		}
		slot = mt.nextSlot
		mt.nextSlot++
		mt.size.Add(PageSize)
	}

	off := slot * PageSize
	copy(mt.mmap[off:off+PageSize], data)

	mt.index[pageIdx] = memEntry{slot: slot}
	return nil
}

func (mt *memtable) putTombstone(pageIdx PageIdx) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.frozen {
		return fmt.Errorf("memtable is frozen")
	}

	mt.index[pageIdx] = memEntry{tombstone: true}
	return nil
}

func (mt *memtable) isEmpty() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.index) == 0
}

func (mt *memtable) get(pageIdx PageIdx) (memEntry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	e, ok := mt.index[pageIdx]
	return e, ok
}

func (mt *memtable) readData(e memEntry) ([]byte, error) {
	if e.tombstone {
		return zeroPage[:], nil
	}

	mt.pins.Add(1)
	defer mt.pins.Add(-1)

	if mt.closed.Load() {
		return nil, errmemtableCleanedUp
	}

	off := e.slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, mt.mmap[off:off+PageSize])
	return buf, nil
}

// readDataPinned returns a zero-copy slice into the mmap and a release
// function that must be called when the caller is done with the data.
// The pin prevents cleanup (munmap) until released.
func (mt *memtable) readDataPinned(e memEntry) ([]byte, func(), error) {
	if e.tombstone {
		return zeroPage[:], func() {}, nil
	}

	mt.pins.Add(1)
	if mt.closed.Load() {
		mt.pins.Add(-1)
		return nil, nil, errmemtableCleanedUp
	}

	off := e.slot * PageSize
	return mt.mmap[off : off+PageSize], func() { mt.pins.Add(-1) }, nil
}

func (mt *memtable) freeze(endSeq uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.frozen = true
	mt.endSeq = endSeq
}

func (mt *memtable) entries() []sortedEntry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	out := make([]sortedEntry, 0, len(mt.index))
	for addr, e := range mt.index {
		out = append(out, sortedEntry{pageIdx: addr, memEntry: e})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].pageIdx < out[j].pageIdx
	})
	return out
}

func (mt *memtable) cleanup() {
	mt.closed.Store(true)

	for i := 0; mt.pins.Load() > 0; i++ {
		runtime.Gosched()
		if i > 1_000_000 {
			panic(fmt.Sprintf("memtable cleanup: %d pins still held after spin limit", mt.pins.Load()))
		}
	}

	if mt.mmap != nil {
		_ = unix.Munmap(mt.mmap)
		mt.mmap = nil
	}
	if mt.file != nil {
		_ = mt.file.Close()
		_ = os.Remove(mt.path)
	}
}
