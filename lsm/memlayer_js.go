//go:build js

package lsm

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// MemLayer is the in-memory write buffer for WASM builds.
// Unlike the native version, this stores pages in a []byte slab
// rather than a file on disk.
type MemLayer struct {
	mu       sync.RWMutex
	slab     []byte
	index    map[uint64]memEntry // pageAddr → latest entry
	nextSlot int                 // bump allocator for slots
	maxPages int
	startSeq uint64
	endSeq   uint64 // set when frozen
	size     atomic.Int64
	frozen   bool
	closed   bool
}

func newMemLayer(_ string, startSeq uint64, maxPages int) (*MemLayer, error) {
	return &MemLayer{
		slab:     make([]byte, int64(maxPages)*PageSize),
		index:    make(map[uint64]memEntry),
		maxPages: maxPages,
		startSeq: startSeq,
	}, nil
}

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
	copy(ml.slab[off:off+PageSize], data)

	ml.index[pageAddr] = memEntry{seq: seq, slot: slot}
	return nil
}

func (ml *MemLayer) putTombstone(pageAddr, seq uint64) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.frozen {
		return fmt.Errorf("memlayer is frozen")
	}

	ml.index[pageAddr] = memEntry{seq: seq, tombstone: true}
	return nil
}

func (ml *MemLayer) isEmpty() bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.index) == 0
}

func (ml *MemLayer) get(pageAddr uint64) (memEntry, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	e, ok := ml.index[pageAddr]
	return e, ok
}

func (ml *MemLayer) readData(e memEntry) ([]byte, error) {
	if e.tombstone {
		return zeroPage[:], nil
	}

	ml.mu.RLock()
	defer ml.mu.RUnlock()

	if ml.closed {
		return nil, errMemLayerCleanedUp
	}

	off := e.slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, ml.slab[off:off+PageSize])
	return buf, nil
}

func (ml *MemLayer) readDataPinned(e memEntry) ([]byte, func(), error) {
	data, err := ml.readData(e)
	if err != nil {
		return nil, nil, err
	}
	return data, func() {}, nil
}

func (ml *MemLayer) freeze(endSeq uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.frozen = true
	ml.endSeq = endSeq
}

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

func (ml *MemLayer) cleanup() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.closed = true
	ml.slab = nil
}
