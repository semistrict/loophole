//go:build !js

package storage2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/metrics"
)

// layerMeta is the S3-persisted metadata for a layer.
type layerMeta struct {
	CreatedAt string `json:"created_at"`
}

// layer is a writable storage layer backed by a tiered LSM.
// Data flows: memtable → frozen memtables → L0 → L1 → L2 → zeros.
type layer struct {
	id         string
	store      loophole.ObjectStore // global root (for reading blobs by full key)
	layerStore loophole.ObjectStore // rooted at layers/<id>/ (for index.json, meta.json)

	config    Config
	diskCache *PageCache
	cacheDir  string

	mu       sync.RWMutex
	index    layerIndex
	memtable *memtable

	nextSeq atomic.Uint64

	frozenMu     sync.RWMutex
	frozenTables []*memtable
	flushMu      sync.Mutex // serializes flushFrozenTables
	compactMu    sync.Mutex // serializes compaction

	// L0 cache: parsed L0 files keyed by S3 key (bounded).
	l0Cache  boundedCache[*parsedL0]
	l0Flight singleflight[*parsedL0]

	// Block header cache: parsed block headers keyed by S3 key (bounded).
	blockCache  boundedCache[*parsedBlock]
	blockFlight singleflight[*parsedBlock]

	// L1/L2 range maps (rebuilt from index on load).
	l1Map *blockRangeMap
	l2Map *blockRangeMap

	// pageLocks serializes read-modify-write cycles for partial page writes.
	pageLocks [256]sync.Mutex

	// Periodic flush goroutine.
	flushStop   chan struct{}
	flushDone   chan struct{}
	flushNotify chan struct{}
	writeNotify chan struct{} // poked on every write; triggers early flush if stale
	flushCancel context.CancelFunc
	lastFlushAt atomic.Int64 // UnixMilli of last successful flush
	closeOnce   sync.Once

	debugLog func(string)
}

func (ly *layer) pageLock(idx PageIdx) *sync.Mutex {
	return &ly.pageLocks[uint64(idx)%uint64(len(ly.pageLocks))]
}

// layerSnapshot captures the layer state for a consistent read.
type layerSnapshot struct {
	memtable *memtable
	frozen   []*memtable
	l0       []l0Entry
	l1       *blockRangeMap
	l2       *blockRangeMap
}

// openLayer loads a layer from S3 and initializes its local state.
// store is the global root (not scoped to a layer prefix).
func openLayer(ctx context.Context, store loophole.ObjectStore, id string, config Config, diskCache *PageCache, cacheDir string) (*layer, error) {
	config.setDefaults()

	layerPrefix := "layers/" + id
	ly := &layer{
		id:         id,
		store:      store,
		layerStore: store.At(layerPrefix),
		config:     config,
		diskCache:  diskCache,
		cacheDir:   cacheDir,
	}
	ly.l0Cache.init(config.MaxCacheEntries)
	ly.blockCache.init(config.MaxCacheEntries)

	// Load index.json.
	if err := ly.loadIndex(ctx); err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}

	// Create memtable.
	memDir := filepath.Join(cacheDir, "mem")
	if err := ensureDir(memDir); err != nil {
		return nil, fmt.Errorf("create mem dir: %w", err)
	}
	mt, err := newMemtable(memDir, ly.nextSeq.Load(), config.maxMemtablePages())
	if err != nil {
		return nil, fmt.Errorf("create memtable: %w", err)
	}
	ly.memtable = mt

	return ly, nil
}

func (ly *layer) loadIndex(ctx context.Context) error {
	idx, _, err := loophole.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
	if err != nil {
		// No index.json yet — start fresh.
		ly.index = layerIndex{NextSeq: 1}
		ly.nextSeq.Store(1)
		ly.l1Map = newBlockRangeMap(nil)
		ly.l2Map = newBlockRangeMap(nil)
		return nil
	}

	ly.index = idx
	ly.nextSeq.Store(idx.NextSeq)
	ly.l1Map = newBlockRangeMap(idx.L1)
	ly.l2Map = newBlockRangeMap(idx.L2)
	return nil
}

// refresh re-reads index.json from S3 to pick up changes written by
// another writer. Used for read-only "follow" mode.
func (ly *layer) refresh(ctx context.Context) error {
	idx, _, err := loophole.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
	if err != nil {
		return fmt.Errorf("refresh index: %w", err)
	}

	l1Map := newBlockRangeMap(idx.L1)
	l2Map := newBlockRangeMap(idx.L2)

	ly.mu.Lock()
	ly.index = idx
	if idx.NextSeq > ly.nextSeq.Load() {
		ly.nextSeq.Store(idx.NextSeq)
	}
	ly.l1Map = l1Map
	ly.l2Map = l2Map
	ly.mu.Unlock()

	// Clear parsed caches — stale entries may reference replaced blobs.
	ly.l0Cache.clear()
	ly.blockCache.clear()

	return nil
}

func (ly *layer) saveIndex(ctx context.Context) error {
	ly.mu.RLock()
	idx := ly.index
	idx.NextSeq = ly.nextSeq.Load()
	idx.L1 = ly.l1Map.Ranges()
	idx.L2 = ly.l2Map.Ranges()
	ly.mu.RUnlock()

	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	return ly.layerStore.PutReader(ctx, "index.json", bytes.NewReader(data))
}

// snapshotLayers captures current layer state under a read lock.
func (ly *layer) snapshotLayers() layerSnapshot {
	ly.mu.RLock()
	snap := layerSnapshot{
		memtable: ly.memtable,
		l0:       ly.index.L0,
		l1:       ly.l1Map,
		l2:       ly.l2Map,
	}
	ly.mu.RUnlock()

	ly.frozenMu.RLock()
	snap.frozen = ly.frozenTables
	ly.frozenMu.RUnlock()

	return snap
}

// Read reads data from the layer into buf at the given byte offset.
func (ly *layer) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	snap := ly.snapshotLayers()

	total := 0
	for total < len(buf) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		pageIdx, pageOff := PageIdxOf(offset + uint64(total))

		data, err := ly.readPageWith(ctx, &snap, pageIdx)
		if err != nil {
			return total, err
		}

		n := copy(buf[total:], data[pageOff:])
		total += n
	}
	return total, nil
}

// readPagePinned returns a zero-copy slice for a single page-aligned read.
// The returned release function must be called when the caller is done.
// Falls back to allocating for S3-sourced data.
func (ly *layer) readPagePinned(ctx context.Context, snap *layerSnapshot, pageIdx PageIdx) ([]byte, func(), error) {
	// 1. Active memtable — zero-copy.
	staleSnapshot := false
	if entry, ok := snap.memtable.get(pageIdx); ok {
		data, release, err := snap.memtable.readDataPinned(entry)
		if err == nil {
			return data, release, nil
		}
		staleSnapshot = true
	}

	// 2. Frozen memtables — zero-copy.
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if entry, ok := snap.frozen[i].get(pageIdx); ok {
			data, release, err := snap.frozen[i].readDataPinned(entry)
			if err == nil {
				return data, release, nil
			}
			staleSnapshot = true
			break
		}
	}

	// 3. Page cache — zero-copy.
	if ly.diskCache != nil {
		if data, release := ly.diskCache.GetPagePinned(ly.pageCacheKey(pageIdx)); data != nil {
			return data, release, nil
		}
	}

	// 4+. L0/L1/L2/zeros — allocating path (data comes from S3 or is zeros).
	_ = staleSnapshot // readPageWith handles stale snapshot refresh internally
	data, err := ly.readPageWith(ctx, snap, pageIdx)
	if err != nil {
		return nil, nil, err
	}
	return data, func() {}, nil
}

// readPageWith reads a single page using a pre-captured layer snapshot.
func (ly *layer) readPageWith(ctx context.Context, snap *layerSnapshot, pageIdx PageIdx) ([]byte, error) {
	// 1. Active memtable.
	staleSnapshot := false
	if entry, ok := snap.memtable.get(pageIdx); ok {
		if entry.tombstone {
			return zeroPage[:], nil
		}
		data, err := snap.memtable.readData(entry)
		if err == nil {
			return data, nil
		}
		// Memtable was cleaned up — its data was flushed to L0.
		staleSnapshot = true
	}

	// 2. Frozen memtables (newest first).
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if entry, ok := snap.frozen[i].get(pageIdx); ok {
			if entry.tombstone {
				return zeroPage[:], nil
			}
			data, err := snap.frozen[i].readData(entry)
			if err == nil {
				return data, nil
			}
			// Frozen memtable was cleaned up — its data was flushed to L0
			// but our snapshot's L0 list is stale. Refresh it.
			staleSnapshot = true
			break
		}
	}

	// 3. Page cache (frozen layer data is immutable).
	if ly.diskCache != nil {
		if cached := ly.diskCache.GetPage(ly.pageCacheKey(pageIdx)); cached != nil {
			return cached, nil
		}
	}

	// If a frozen memtable was cleaned up, refresh the L0 list from the layer
	// since the flush guarantees the L0 entry exists before cleanup.
	l0 := snap.l0
	if staleSnapshot {
		ly.mu.RLock()
		l0 = ly.index.L0
		ly.mu.RUnlock()
	}

	// 4. L0 files (newest first — last in slice is newest).
	for i := len(l0) - 1; i >= 0; i-- {
		l0e := &l0[i]
		if l0HasTombstone(l0e, pageIdx) {
			return zeroPage[:], nil
		}
		if !l0HasPage(l0e, pageIdx) {
			continue
		}
		data, err := ly.readFromL0(ctx, l0e, pageIdx)
		if err != nil {
			return nil, err
		}
		ly.cachePage(pageIdx, data)
		return data, nil
	}

	// 5. L1 (sparse blocks).
	if layer := snap.l1.Find(pageIdx.Block()); layer != "" {
		data, found, err := ly.readFromBlock(ctx, "l1", layer, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(pageIdx, data)
			return data, nil
		}
		// Page not in this sparse block — fall through to L2.
	}

	// 6. L2 (dense blocks).
	if layer := snap.l2.Find(pageIdx.Block()); layer != "" {
		data, found, err := ly.readFromBlock(ctx, "l2", layer, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(pageIdx, data)
			return data, nil
		}
	}

	// 7. Page never written — return zeros.
	return zeroPage[:], nil
}

func (ly *layer) pageCacheKey(pageIdx PageIdx) cacheKey {
	return cacheKey{LayerID: ly.id, PageIdx: pageIdx}
}

func (ly *layer) cachePage(pageIdx PageIdx, data []byte) {
	if ly.diskCache != nil {
		ly.diskCache.PutPage(ly.pageCacheKey(pageIdx), data)
	}
}

func (ly *layer) deleteCachedPage(pageIdx PageIdx) {
	if ly.diskCache != nil {
		ly.diskCache.DeletePage(ly.pageCacheKey(pageIdx))
	}
}

// readFromL0 reads a page from an L0 file, downloading and caching the
// parsed L0 blob if needed.
func (ly *layer) readFromL0(ctx context.Context, l0e *l0Entry, pageIdx PageIdx) ([]byte, error) {
	p, err := ly.getParsedL0(ctx, l0e)
	if err != nil {
		return nil, err
	}
	data, found, err := p.findPage(ctx, pageIdx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("L0 page index said page %d exists in %s but findPage returned false", pageIdx, l0e.Key)
	}
	return data, nil
}

func (ly *layer) getParsedL0(ctx context.Context, l0e *l0Entry) (*parsedL0, error) {
	if cached, ok := ly.l0Cache.get(l0e.Key); ok {
		return cached, nil
	}

	return ly.l0Flight.do(l0e.Key, func() (*parsedL0, error) {
		// Double-check after winning the singleflight race.
		if cached, ok := ly.l0Cache.get(l0e.Key); ok {
			return cached, nil
		}

		body, _, err := ly.store.Get(ctx, l0e.Key)
		if err != nil {
			return nil, fmt.Errorf("get L0 %s: %w", l0e.Key, err)
		}
		data, err := readAll(body)
		if err != nil {
			return nil, fmt.Errorf("read L0 %s: %w", l0e.Key, err)
		}

		p, err := parseL0(data)
		if err != nil {
			return nil, fmt.Errorf("parse L0 %s: %w", l0e.Key, err)
		}
		p.store = ly.store
		p.key = l0e.Key

		ly.l0Cache.put(l0e.Key, p)
		return p, nil
	})
}

// readFromBlock reads a page from an L1 or L2 block.
func (ly *layer) readFromBlock(ctx context.Context, level, layerID string, pageIdx PageIdx) ([]byte, bool, error) {
	blockAddr := pageIdx.Block()
	key := fmt.Sprintf("layers/%s/%s/%016x", layerID, level, blockAddr)

	pb, err := ly.getParsedBlock(ctx, key)
	if err != nil {
		return nil, false, err
	}
	return pb.findPage(ctx, pageIdx)
}

func (ly *layer) getParsedBlock(ctx context.Context, key string) (*parsedBlock, error) {
	if cached, ok := ly.blockCache.get(key); ok {
		return cached, nil
	}

	return ly.blockFlight.do(key, func() (*parsedBlock, error) {
		// Double-check after winning the singleflight race.
		if cached, ok := ly.blockCache.get(key); ok {
			return cached, nil
		}

		body, _, err := ly.store.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("get block %s: %w", key, err)
		}
		data, err := readAll(body)
		if err != nil {
			return nil, fmt.Errorf("read block %s: %w", key, err)
		}

		pb, err := parseBlock(data)
		if err != nil {
			return nil, fmt.Errorf("parse block %s: %w", key, err)
		}

		ly.blockCache.put(key, pb)
		return pb, nil
	})
}

// Write writes data to the layer at the given byte offset.
func (ly *layer) Write(ctx context.Context, data []byte, offset uint64) error {
	written := 0
	for written < len(data) {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageIdx, pageOff := PageIdxOf(offset + uint64(written))
		chunk := min(uint64(len(data)-written), PageSize-pageOff)

		if err := ly.writePage(ctx, pageIdx, pageOff, data[written:written+int(chunk)]); err != nil {
			return err
		}
		written += int(chunk)
	}
	return nil
}

func (ly *layer) writePage(ctx context.Context, pageIdx PageIdx, pageOff uint64, chunk []byte) error {
	pageLock := ly.pageLock(pageIdx)
	pageLock.Lock()

	var page [PageSize]byte

	// If partial page write, read-modify-write.
	if uint64(len(chunk)) < PageSize {
		snap := ly.snapshotLayers()
		existing, err := ly.readPageWith(ctx, &snap, pageIdx)
		if err != nil {
			pageLock.Unlock()
			return err
		}
		copy(page[:], existing)
	}

	copy(page[pageOff:], chunk)

	seq := ly.nextSeq.Add(1) - 1
	ly.mu.RLock()
	mt := ly.memtable
	err := mt.put(pageIdx, seq, page[:])
	ly.mu.RUnlock()

	pageLock.Unlock()

	// Backpressure: if the memtable is full, freeze and flush.
	for errors.Is(err, errMemtableFull) {
		if err := ly.maybeFreezeAndFlush(ctx); err != nil {
			return err
		}
		ly.mu.RLock()
		mt = ly.memtable
		err = mt.put(pageIdx, seq, page[:])
		ly.mu.RUnlock()
	}
	if err != nil {
		return err
	}

	// Notify flush loop.
	if ly.writeNotify != nil {
		select {
		case ly.writeNotify <- struct{}{}:
		default:
		}
	}

	// Backpressure: if frozen tables are at capacity, flush them now.
	ly.frozenMu.RLock()
	nfrozen := len(ly.frozenTables)
	ly.frozenMu.RUnlock()
	if nfrozen >= ly.config.MaxFrozenTables {
		if err := ly.flushFrozenTables(ctx); err != nil {
			return err
		}
	}

	// Check if memtable needs freezing.
	if mt.size.Load() >= ly.config.FlushThreshold {
		if err := ly.maybeFreezeAndFlush(ctx); err != nil {
			return err
		}
	}

	return nil
}

// PunchHole records tombstones for all pages fully covered by the range.
func (ly *layer) PunchHole(ctx context.Context, offset, length uint64) error {
	end := offset + length

	firstFullPage := PageIdx((offset + PageSize - 1) / PageSize)
	lastFullPage := PageIdx(end / PageSize)

	// Partial page at start.
	if startOff := offset % PageSize; startOff != 0 {
		pageIdx, _ := PageIdxOf(offset)
		nextPageByte := (pageIdx + 1).ByteOffset()
		clearEnd := uint64(PageSize)
		if end < nextPageByte {
			clearEnd = end - pageIdx.ByteOffset()
		}
		zeros := make([]byte, clearEnd-startOff)
		if err := ly.Write(ctx, zeros, offset); err != nil {
			return err
		}
		if end <= nextPageByte {
			return nil
		}
	}

	// Partial page at end.
	if endOff := end % PageSize; endOff != 0 {
		pageIdx, _ := PageIdxOf(end)
		zeros := make([]byte, endOff)
		if err := ly.Write(ctx, zeros, pageIdx.ByteOffset()); err != nil {
			return err
		}
	}

	// Tombstone fully-covered interior pages.
	for pageIdx := firstFullPage; pageIdx < lastFullPage; pageIdx++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageLock := ly.pageLock(pageIdx)
		pageLock.Lock()

		seq := ly.nextSeq.Add(1) - 1
		ly.mu.RLock()
		err := ly.memtable.putTombstone(pageIdx, seq)
		ly.mu.RUnlock()

		pageLock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes all pending data to S3.
func (ly *layer) Flush(ctx context.Context) error {
	if err := ly.freezememtable(); err != nil {
		return err
	}
	return ly.flushFrozenTables(ctx)
}

func (ly *layer) freezememtable() error {
	ly.mu.Lock()
	defer ly.mu.Unlock()

	if ly.memtable.isEmpty() {
		return nil
	}

	memDir := filepath.Join(ly.cacheDir, "mem")
	if err := ensureDir(memDir); err != nil {
		return fmt.Errorf("ensure mem dir: %w", err)
	}
	mt, err := newMemtable(memDir, ly.nextSeq.Load(), ly.config.maxMemtablePages())
	if err != nil {
		return fmt.Errorf("create new memtable: %w", err)
	}

	old := ly.memtable
	old.freeze(ly.nextSeq.Load())

	ly.frozenMu.Lock()
	ly.frozenTables = append(ly.frozenTables, old)
	ly.frozenMu.Unlock()

	ly.memtable = mt
	return nil
}

func (ly *layer) maybeFreezeAndFlush(ctx context.Context) error {
	if err := ly.freezememtable(); err != nil {
		return err
	}
	if ly.flushNotify != nil {
		select {
		case ly.flushNotify <- struct{}{}:
		default:
		}
		return nil
	}
	return ly.flushFrozenTables(ctx)
}

func (ly *layer) flushFrozenTables(ctx context.Context) error {
	ly.flushMu.Lock()
	defer ly.flushMu.Unlock()
	return ly.flushFrozenTablesLocked(ctx, 5)
}

func (ly *layer) flushFrozenTablesLocked(ctx context.Context, maxRetries int) error {
	for {
		ly.frozenMu.RLock()
		if len(ly.frozenTables) == 0 {
			ly.frozenMu.RUnlock()
			return nil
		}
		mt := ly.frozenTables[0]
		ly.frozenMu.RUnlock()

		if err := ly.flushMemtable(ctx, mt, maxRetries); err != nil {
			return err
		}

		ly.frozenMu.Lock()
		ly.frozenTables = ly.frozenTables[1:]
		ly.frozenMu.Unlock()

		mt.cleanup()
	}
}

func (ly *layer) flushMemtable(ctx context.Context, mt *memtable, maxRetries int) error {
	entries := mt.entries()
	if len(entries) == 0 {
		return nil
	}

	data, l0entry, err := buildL0(mt, ly.id, entries)
	if err != nil {
		return fmt.Errorf("build L0: %w", err)
	}

	// Upload with retry (no backoff — retries are immediate to avoid
	// blocking the synctest bubble with timer waits while holding flushMu).
	for attempt := range maxRetries {
		err = ly.store.PutReader(ctx, l0entry.Key, bytes.NewReader(data))
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("upload L0: %w", err)
		}
		if attempt == maxRetries-1 {
			return fmt.Errorf("upload L0 (after %d attempts): %w", maxRetries, err)
		}
		slog.Warn("retrying L0 upload", "key", l0entry.Key, "attempt", attempt+1, "error", err)
	}

	metrics.FlushBytes.Add(float64(len(data)))

	// Pre-cache the parsed L0.
	p, _ := parseL0(data)
	if p != nil {
		p.store = ly.store
		p.key = l0entry.Key
		ly.l0Cache.put(l0entry.Key, p)
	}

	// Update page cache with flushed data so L0 reads hit the cache.
	for _, e := range entries {
		if e.tombstone {
			ly.deleteCachedPage(e.pageIdx)
		} else if data, err := mt.readData(e.memEntry); err == nil {
			ly.cachePage(e.pageIdx, data)
		}
	}

	ly.mu.Lock()
	ly.index.L0 = append(ly.index.L0, l0entry)
	ly.mu.Unlock()

	ly.log("FLUSH layer=%s l0=%s pages=%d tombstones=%d",
		ly.id[:min(8, len(ly.id))], l0entry.Key, len(l0entry.Pages), len(l0entry.Tombstones))

	// Checkpoint index.json.
	if err := ly.saveIndex(ctx); err != nil {
		ly.mu.Lock()
		ly.index.L0 = ly.index.L0[:len(ly.index.L0)-1]
		ly.mu.Unlock()
		return fmt.Errorf("save index: %w", err)
	}

	return nil
}

// Snapshot freezes this layer and creates a child layer that inherits all
// data via a copy of this layer's index.json.
func (ly *layer) Snapshot(ctx context.Context, childID string) error {
	// 1. Flush everything.
	if err := ly.Flush(ctx); err != nil {
		return fmt.Errorf("flush before snapshot: %w", err)
	}

	childStore := ly.store.At("layers/" + childID)

	// 2. Copy our index.json into the child — the child inherits everything.
	ly.mu.RLock()
	idx := ly.index
	idx.NextSeq = ly.nextSeq.Load()
	idx.L1 = ly.l1Map.Ranges()
	idx.L2 = ly.l2Map.Ranges()
	ly.mu.RUnlock()

	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index for child: %w", err)
	}
	if err := childStore.PutIfNotExists(ctx, "index.json", idxData); err != nil {
		return fmt.Errorf("create child index.json: %w", err)
	}

	// 3. Write child meta.json.
	meta := layerMeta{
		CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := childStore.PutIfNotExists(ctx, "meta.json", metaData); err != nil {
		return fmt.Errorf("create child meta.json: %w", err)
	}

	return nil
}

// Close shuts down the layer's background goroutines and cleans up resources.
func (ly *layer) Close() {
	ly.closeOnce.Do(func() {
		if ly.flushCancel != nil {
			ly.flushCancel()
		}
		if ly.flushStop != nil {
			close(ly.flushStop)
			<-ly.flushDone
		}

		ly.mu.Lock()
		if ly.memtable != nil {
			ly.memtable.cleanup()
		}
		ly.mu.Unlock()

		ly.frozenMu.Lock()
		for _, mt := range ly.frozenTables {
			mt.cleanup()
		}
		ly.frozenTables = nil
		ly.frozenMu.Unlock()
	})
}

// flushWriteDelay is how long to wait after a write-triggered flush
// before actually flushing, to batch nearby writes.
const flushWriteDelay = 2 * time.Second

// startPeriodicFlush starts the background flush goroutine.
func (ly *layer) startPeriodicFlush(parentCtx context.Context) {
	if ly.config.FlushInterval < 0 {
		return
	}

	ctx, cancel := context.WithCancel(parentCtx)
	ly.flushCancel = cancel
	ly.flushStop = make(chan struct{})
	ly.flushDone = make(chan struct{})
	ly.flushNotify = make(chan struct{}, 1)
	ly.writeNotify = make(chan struct{}, 1)
	ly.lastFlushAt.Store(time.Now().UnixMilli())

	go ly.periodicFlushLoop(ctx)
}

func (ly *layer) periodicFlushLoop(ctx context.Context) {
	defer close(ly.flushDone)
	timer := time.NewTimer(ly.config.FlushInterval)
	defer timer.Stop()

	for {
		select {
		case <-ly.flushStop:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-ly.flushNotify:
		case <-ly.writeNotify:
			// A write arrived. If the last flush was longer than FlushInterval
			// ago, schedule a flush after a short delay to batch nearby writes.
			sinceFlush := time.Since(time.UnixMilli(ly.lastFlushAt.Load()))
			if sinceFlush < ly.config.FlushInterval {
				continue // not stale, let the regular timer handle it
			}
			// Drain any extra write notifications.
			select {
			case <-ly.writeNotify:
			default:
			}
			// Wait a short delay to batch writes, but still listen for stop.
			delay := time.NewTimer(flushWriteDelay)
			select {
			case <-ly.flushStop:
				delay.Stop()
				return
			case <-delay.C:
			}
		}

		// Drain any pending notifications so we don't loop unnecessarily.
		select {
		case <-ly.flushNotify:
		default:
		}
		select {
		case <-ly.writeNotify:
		default:
		}

		ly.doPeriodicFlush(ctx)
		if ctx.Err() != nil {
			return
		}
		timer.Reset(ly.config.FlushInterval)
	}
}

func (ly *layer) doPeriodicFlush(ctx context.Context) {
	// Flush frozen tables. TryLock avoids blocking if another goroutine
	// (e.g. an explicit Flush call) already holds flushMu. Under synctest,
	// a blocked goroutine prevents time from advancing, which deadlocks
	// retry timers.
	if ly.flushMu.TryLock() {
		err := ly.flushFrozenTablesLocked(ctx, 5)
		ly.flushMu.Unlock()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("periodic flush failed", "layer", ly.id, "error", err)
		}
	}

	// Also freeze+flush active memtable if non-empty.
	ly.mu.RLock()
	mt := ly.memtable
	ly.mu.RUnlock()
	if !mt.isEmpty() {
		if err := ly.freezememtable(); err == nil {
			if ly.flushMu.TryLock() {
				err := ly.flushFrozenTablesLocked(ctx, 5)
				ly.flushMu.Unlock()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("periodic flush failed", "layer", ly.id, "error", err)
				}
			}
		}
	}
	ly.lastFlushAt.Store(time.Now().UnixMilli())
}

func (ly *layer) log(format string, args ...any) {
	if ly.debugLog != nil {
		ly.debugLog(fmt.Sprintf(format, args...))
	}
}

// readAll reads the body and closes it.
func readAll(body interface {
	Read([]byte) (int, error)
	Close() error
}) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ensureDir creates a directory if it doesn't exist.
func ensureDir(path string) error {
	return ensureMemDir(path)
}

// CompactL0 merges L0 entries into L1 blocks.
func (ly *layer) CompactL0(ctx context.Context) error {
	ly.compactMu.Lock()
	defer ly.compactMu.Unlock()

	ly.mu.RLock()
	l0entries := make([]l0Entry, len(ly.index.L0))
	copy(l0entries, ly.index.L0)
	ly.mu.RUnlock()

	if totalL0Pages(l0entries) < L0CompactTrigger {
		return nil
	}

	// Group all L0 page addresses by block address.
	type pageSource struct {
		l0Idx   int
		pageIdx PageIdx
	}
	blockGroups := make(map[BlockIdx][]pageSource)
	for i, l0e := range l0entries {
		for _, pa := range l0e.Pages {
			blockGroups[pa.Block()] = append(blockGroups[pa.Block()], pageSource{l0Idx: i, pageIdx: pa})
		}
		for _, pa := range l0e.Tombstones {
			blockGroups[pa.Block()] = append(blockGroups[pa.Block()], pageSource{l0Idx: i, pageIdx: pa})
		}
	}

	// For each block address, read existing L1 block (if any), overlay L0 pages.
	for blockAddr, sources := range blockGroups {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Collect pages for this block. Newer L0 entries override older ones.
		// L0 entries are ordered oldest-first, so later entries win.
		pageMap := make(map[uint16][]byte) // page offset → data

		// Load existing L1 block.
		if layer := ly.l1Map.Find(blockAddr); layer != "" {
			key := fmt.Sprintf("layers/%s/l1/%016x", layer, blockAddr)
			pb, err := ly.getParsedBlock(ctx, key)
			if err != nil {
				return fmt.Errorf("read existing L1 block %d: %w", blockAddr, err)
			}
			pages, err := pb.readAllPages(ctx, blockAddr)
			if err != nil {
				return fmt.Errorf("read L1 block pages %d: %w", blockAddr, err)
			}
			for off, data := range pages {
				pageMap[off] = data
			}
		}

		// Overlay L0 pages (in order, so newer wins).
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].l0Idx < sources[j].l0Idx
		})
		for _, src := range sources {
			pageOffset := uint16(src.pageIdx % BlockPages)

			// Check if this is a tombstone.
			isTombstone := false
			for _, t := range l0entries[src.l0Idx].Tombstones {
				if t == src.pageIdx {
					isTombstone = true
					break
				}
			}

			if isTombstone {
				// Tombstone writes zeros into L1, masking any inherited L2 data.
				pageMap[pageOffset] = zeroPage[:]
			} else {
				// Read the actual page data from the L0 file.
				p, err := ly.getParsedL0(ctx, &l0entries[src.l0Idx])
				if err != nil {
					return fmt.Errorf("read L0 for compact: %w", err)
				}
				data, found, err := p.findPage(ctx, src.pageIdx)
				if err != nil {
					return fmt.Errorf("find page in L0: %w", err)
				}
				if found {
					pageMap[pageOffset] = data
				}
			}
		}

		// Build and upload new L1 block.
		var pages []blockPage
		for off, data := range pageMap {
			pages = append(pages, blockPage{offset: off, data: data})
		}

		blockData, err := buildBlock(blockAddr, pages)
		if err != nil {
			return fmt.Errorf("build L1 block %d: %w", blockAddr, err)
		}

		key := fmt.Sprintf("layers/%s/l1/%016x", ly.id, blockAddr)
		if err := ly.store.PutReader(ctx, key, bytes.NewReader(blockData)); err != nil {
			return fmt.Errorf("upload L1 block %d: %w", blockAddr, err)
		}

		// Update L1 range map.
		ly.mu.Lock()
		ly.l1Map = ly.l1Map.Set(blockAddr, ly.id)
		ly.mu.Unlock()

		// Check if this L1 block should be promoted to L2.
		if len(pages) >= L1PromoteThreshold {
			if err := ly.promoteL1toL2(ctx, blockAddr, pages); err != nil {
				return fmt.Errorf("promote L1→L2 block %d: %w", blockAddr, err)
			}
		}

		// Cache the new block.
		pb, _ := parseBlock(blockData)
		if pb != nil {
			ly.blockCache.put(key, pb)
		}
	}

	// Clear L0 entries from index.
	ly.mu.Lock()
	ly.index.L0 = nil
	ly.mu.Unlock()

	// Save updated index.
	return ly.saveIndex(ctx)
}

// promoteL1toL2 merges an L1 block into L2.
func (ly *layer) promoteL1toL2(ctx context.Context, blockAddr BlockIdx, l1Pages []blockPage) error {
	// Read existing L2 block if any.
	pageMap := make(map[uint16][]byte)
	if layer := ly.l2Map.Find(blockAddr); layer != "" {
		key := fmt.Sprintf("layers/%s/l2/%016x", layer, blockAddr)
		pb, err := ly.getParsedBlock(ctx, key)
		if err != nil {
			return fmt.Errorf("read existing L2 block: %w", err)
		}
		pages, err := pb.readAllPages(ctx, blockAddr)
		if err != nil {
			return fmt.Errorf("read L2 block pages: %w", err)
		}
		for off, data := range pages {
			pageMap[off] = data
		}
	}

	// Overlay L1 pages onto L2.
	for _, p := range l1Pages {
		pageMap[p.offset] = p.data
	}

	// Build new L2 block.
	var pages []blockPage
	for off, data := range pageMap {
		pages = append(pages, blockPage{offset: off, data: data})
	}

	blockData, err := buildBlock(blockAddr, pages)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("layers/%s/l2/%016x", ly.id, blockAddr)
	if err := ly.store.PutReader(ctx, key, bytes.NewReader(blockData)); err != nil {
		return err
	}

	// Update maps: add L2 entry, remove L1 entry.
	ly.mu.Lock()
	ly.l2Map = ly.l2Map.Set(blockAddr, ly.id)
	ly.l1Map = ly.l1Map.Remove(blockAddr)
	ly.mu.Unlock()

	// Cache new block.
	pb, _ := parseBlock(blockData)
	if pb != nil {
		ly.blockCache.put(key, pb)
	}

	return nil
}
