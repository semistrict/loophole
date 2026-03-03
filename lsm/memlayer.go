package lsm

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// MemLayer is the in-memory write buffer backed by an append-only local file.
// Writes append 4KB pages to the file and update an in-memory index.
// Only the latest entry per page is kept — intermediate versions are discarded.
type MemLayer struct {
	mu       sync.RWMutex
	file     File
	path     string
	fs       LocalFS
	index    map[uint64]memEntry // pageAddr → latest entry
	startSeq uint64
	endSeq   uint64 // set when frozen
	size     atomic.Int64
	frozen   bool
}

// memEntry is an index entry for a single page in the memLayer.
type memEntry struct {
	seq       uint64
	offset    int64 // byte offset in the ephemeral file
	tombstone bool  // true = PunchHole, read as zeros
}

// newMemLayer creates a new memLayer with a fresh ephemeral file.
// The filename includes a random suffix to prevent collisions when the same
// timeline is opened multiple times (e.g., parent + child's ancestor reference).
func newMemLayer(fs LocalFS, dir string, startSeq uint64) (*MemLayer, error) {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	path := filepath.Join(dir, fmt.Sprintf("%016x-%s.ephemeral", startSeq, hex.EncodeToString(rnd[:])))
	f, err := fs.Create(path)
	if err != nil {
		return nil, err
	}
	return &MemLayer{
		file:     f,
		path:     path,
		fs:       fs,
		index:    make(map[uint64]memEntry),
		startSeq: startSeq,
	}, nil
}

// put appends a 4KB page to the ephemeral file and updates the index.
func (ml *MemLayer) put(pageAddr, seq uint64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(data))
	}

	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.frozen {
		return fmt.Errorf("memlayer is frozen")
	}

	offset, err := ml.file.Seek(0, 2) // seek to end
	if err != nil {
		return err
	}
	if _, err := ml.file.Write(data); err != nil {
		return err
	}

	ml.index[pageAddr] = memEntry{seq: seq, offset: offset}
	ml.size.Add(PageSize)
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

// readData reads the page data for an entry from the ephemeral file.
func (ml *MemLayer) readData(e memEntry) ([]byte, error) {
	if e.tombstone {
		return zeroPage[:], nil
	}

	buf := make([]byte, PageSize)
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	_, err := ml.file.ReadAt(buf, e.offset)
	if err != nil {
		return nil, fmt.Errorf("read ephemeral at %d: %w", e.offset, err)
	}
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

// cleanup removes the ephemeral file.
func (ml *MemLayer) cleanup() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	_ = ml.file.Close()
	_ = ml.fs.Remove(ml.path)
}
