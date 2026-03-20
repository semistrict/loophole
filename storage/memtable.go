package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/semistrict/loophole/internal/safepoint"
	"golang.org/x/sys/unix"
)

// errMemtableFull is returned when the memtable is full.
var errMemtableFull = fmt.Errorf("memtable full")

// sortedEntry pairs a page address with its mmap slot for iteration.
type sortedEntry struct {
	pageIdx PageIdx
	slot    int
}

// memtable is the in-memory write buffer backed by a memory-mapped file.
// Pages are stored in fixed-size slots within the mapping, and an in-memory
// index maps page addresses to slots.
type memtable struct {
	mu       sync.RWMutex
	file     *os.File
	path     string
	mmap     []byte
	index    map[PageIdx]int // pageIdx → mmap slot
	nextSlot int
	maxPages int
	startSeq uint64
	endSeq   uint64
	size     atomic.Int64
	frozen   bool
	closed   bool // protected by mu
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
		index:    make(map[PageIdx]int),
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
	if existing, ok := mt.index[pageIdx]; ok {
		slot = existing
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

	mt.index[pageIdx] = slot
	return nil
}

func (mt *memtable) isEmpty() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.index) == 0
}

func (mt *memtable) get(pageIdx PageIdx) (int, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	slot, ok := mt.index[pageIdx]
	return slot, ok
}

func (mt *memtable) readData(slot int) ([]byte, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if mt.closed {
		return nil, errmemtableCleanedUp
	}

	off := slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, mt.mmap[off:off+PageSize])
	return buf, nil
}

// readDataRef returns a slice pointing directly into the mmap without copying.
// The caller must hold a safepoint Guard for the duration of the returned
// slice's use. The Guard prevents cleanup() from unmapping the memory.
func (mt *memtable) readDataRef(g safepoint.Guard, slot int) ([]byte, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if mt.closed {
		return nil, errmemtableCleanedUp
	}

	off := slot * PageSize
	ref := mt.mmap[off : off+PageSize]
	g.Register(ref)
	return ref, nil
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
	for addr, slot := range mt.index {
		out = append(out, sortedEntry{pageIdx: addr, slot: slot})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].pageIdx < out[j].pageIdx
	})
	return out
}

// cleanup unmaps and removes the backing file. Waits for any in-flight
// readData calls to complete before unmapping.
func (mt *memtable) cleanup() {
	mt.mu.Lock()
	mt.closed = true
	mmap := mt.mmap
	mt.mmap = nil
	mt.mu.Unlock()

	if mmap != nil {
		_ = unix.Munmap(mmap)
	}
	if mt.file != nil {
		_ = mt.file.Close()
		_ = os.Remove(mt.path)
	}
}
