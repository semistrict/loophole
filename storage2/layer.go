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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/metrics"
	"golang.org/x/sync/errgroup"
)

// layer is a writable storage layer backed by a tiered L0/L1/L2 structure.
// Data flows: memtable → frozen memtables → L0 → L1 → L2 → zeros.
type layer struct {
	id         string
	store      loophole.ObjectStore // global root (for reading blobs by full key)
	layerStore loophole.ObjectStore // rooted at layers/<id>/ (for index.json)

	config    Config
	diskCache *PageCache
	cacheDir  string
	lease     *loophole.LeaseManager

	// writeLeaseSeq is the monotonically increasing sequence number
	// assigned when the write lease was acquired. All files written
	// during this lease session embed this value in their key names.
	writeLeaseSeq uint64

	mu       sync.RWMutex
	index    layerIndex
	memtable *memtable

	nextSeq atomic.Uint64

	frozenMu     sync.RWMutex
	frozenTables []*memtable
	flushMu      sync.Mutex // serializes flushFrozenTables
	compactMu    sync.Mutex // serializes compaction
	indexMu      sync.Mutex // serializes index.json publication within one process

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
	layoutGen uint64
	memtable  *memtable
	frozen    []*memtable
	l0        []l0Entry
	l1        *blockRangeMap
	l2        *blockRangeMap
}

type cachedL0Page struct {
	pageIdx PageIdx
	data    []byte
}

type compactionLockFile struct {
	Token string `json:"token"`
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

// openFrozenLayer loads a frozen (immutable) layer. Reads index.json but
// creates no memtable.
func openFrozenLayer(ctx context.Context, store loophole.ObjectStore, id string, config Config, diskCache *PageCache) (*layer, error) {
	idx, _, err := loophole.ReadJSON[layerIndex](ctx, store.At("layers/"+id), "index.json")
	if err != nil {
		return nil, fmt.Errorf("read frozen layer index: %w", err)
	}
	return initFrozenLayerFromIndex(store, id, config, diskCache, idx), nil
}

// initLayerFromIndex creates a mutable layer from a pre-loaded index.
// No S3 reads — the caller already has the index. Used by Clone to avoid
// re-reading the child index that was just written.
func initLayerFromIndex(store loophole.ObjectStore, id string, config Config, diskCache *PageCache, cacheDir string, idx layerIndex) (*layer, error) {
	config.setDefaults()
	ly := &layer{
		id:         id,
		store:      store,
		layerStore: store.At("layers/" + id),
		config:     config,
		diskCache:  diskCache,
		cacheDir:   cacheDir,
	}
	ly.l0Cache.init(config.MaxCacheEntries)
	ly.blockCache.init(config.MaxCacheEntries)

	ly.index = idx
	ly.nextSeq.Store(idx.NextSeq)
	ly.l1Map = newBlockRangeMap(idx.L1)
	ly.l2Map = newBlockRangeMap(idx.L2)

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

// initFrozenLayerFromIndex creates a frozen (immutable) layer from a pre-loaded
// index. No S3 reads required — the caller already has the index.
func initFrozenLayerFromIndex(store loophole.ObjectStore, id string, config Config, diskCache *PageCache, idx layerIndex) *layer {
	config.setDefaults()
	ly := &layer{
		id:         id,
		store:      store,
		layerStore: store.At("layers/" + id),
		config:     config,
		diskCache:  diskCache,
	}
	ly.l0Cache.init(config.MaxCacheEntries)
	ly.blockCache.init(config.MaxCacheEntries)

	ly.index = idx
	ly.nextSeq.Store(idx.NextSeq)
	ly.l1Map = newBlockRangeMap(idx.L1)
	ly.l2Map = newBlockRangeMap(idx.L2)
	return ly
}

func (ly *layer) loadIndex(ctx context.Context) error {
	idx, _, err := loophole.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
	if err != nil {
		// No index.json yet — start fresh.
		ly.index = layerIndex{NextSeq: 1, LayoutGen: 1}
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
	ly.indexMu.Lock()
	defer ly.indexMu.Unlock()

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
		layoutGen: ly.index.LayoutGen,
		memtable:  ly.memtable,
		l0:        ly.index.L0,
		l1:        ly.l1Map,
		l2:        ly.l2Map,
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
	total, err := ly.readWithSnapshot(ctx, buf, offset, &snap)
	if err == nil {
		return total, nil
	}
	if !ly.shouldRetryAfterLayoutError(err) {
		return total, err
	}
	refreshed, refreshErr := ly.waitRefreshForLayoutChange(ctx, snap.layoutGen)
	if refreshErr != nil || !refreshed {
		return total, err
	}
	return ly.Read(ctx, buf, offset)
}

func (ly *layer) readWithSnapshot(ctx context.Context, buf []byte, offset uint64, snap *layerSnapshot) (int, error) {
	total := 0
	for total < len(buf) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		pageIdx, pageOff := PageIdxOf(offset + uint64(total))

		data, err := ly.readPageWith(ctx, snap, pageIdx)
		if err != nil {
			return total, err
		}

		n := copy(buf[total:], data[pageOff:])
		total += n
	}
	return total, nil
}

// readPageWith reads a single page using a pre-captured layer snapshot.
func (ly *layer) readPageWith(ctx context.Context, snap *layerSnapshot, pageIdx PageIdx) ([]byte, error) {
	// 1. Active memtable (nil for frozen layers).
	staleSnapshot := false
	if snap.memtable != nil {
		if slot, ok := snap.memtable.get(pageIdx); ok {
			data, err := snap.memtable.readData(slot)
			if err == nil {
				metrics.PageReadSource.WithLabelValues("memtable").Inc()
				return data, nil
			}
			// Memtable was cleaned up — its data was flushed to L0.
			staleSnapshot = true
		}
	}

	// 2. Frozen memtables (newest first).
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if slot, ok := snap.frozen[i].get(pageIdx); ok {
			data, err := snap.frozen[i].readData(slot)
			if err == nil {
				metrics.PageReadSource.WithLabelValues("frozen").Inc()
				return data, nil
			}
			// Frozen memtable was cleaned up — its data was flushed to L0
			// but our snapshot's L0 list is stale. Refresh it.
			staleSnapshot = true
			break
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
		if !l0HasPage(l0e, pageIdx) {
			continue
		}
		sourceLayerID := layerIDFromBlobKey(l0e.Key)
		if data := ly.cachedPage(sourceLayerID, pageIdx); data != nil {
			return data, nil
		}
		data, err := ly.readFromL0(ctx, l0e, pageIdx)
		if err != nil {
			return nil, err
		}
		ly.cachePage(sourceLayerID, pageIdx, data)
		metrics.PageReadSource.WithLabelValues("l0").Inc()
		return data, nil
	}

	// 5. L1 (sparse blocks).
	if layer, seq := snap.l1.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadSource.WithLabelValues("l1").Inc()
			return data, nil
		}
		// Page not in this sparse block — fall through to L2.
	}

	// 6. L2 (dense blocks).
	if layer, seq := snap.l2.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadSource.WithLabelValues("l2").Inc()
			return data, nil
		}
	}

	// 7. Page never written — return zeros.
	metrics.PageReadSource.WithLabelValues("zero").Inc()
	return zeroPage[:], nil
}

func (ly *layer) shouldUsePersistentPageCache(sourceLayerID string) bool {
	if ly.diskCache == nil {
		return false
	}
	return ly.memtable == nil || sourceLayerID != ly.id
}

func (ly *layer) cachedPage(sourceLayerID string, pageIdx PageIdx) []byte {
	if !ly.shouldUsePersistentPageCache(sourceLayerID) {
		return nil
	}
	key := cacheKey{LayerID: sourceLayerID, PageIdx: pageIdx}
	if data := ly.diskCache.GetPage(key); data != nil {
		metrics.CacheHits.Inc()
		metrics.PageReadSource.WithLabelValues("cache").Inc()
		return data
	}
	metrics.CacheMisses.Inc()
	return nil
}

func (ly *layer) cachePage(sourceLayerID string, pageIdx PageIdx, data []byte) {
	if ly.shouldUsePersistentPageCache(sourceLayerID) {
		ly.diskCache.PutPage(cacheKey{LayerID: sourceLayerID, PageIdx: pageIdx}, data)
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
func (ly *layer) readFromBlock(ctx context.Context, level, layerID string, writeLeaseSeq uint64, pageIdx PageIdx) ([]byte, bool, error) {
	blockAddr := pageIdx.Block()
	key := blockKey(layerID, level, writeLeaseSeq, blockAddr)

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
func (ly *layer) Write(data []byte, offset uint64) error {
	written := 0
	for written < len(data) {
		remaining := len(data) - written

		// Fast path: if this is a full block-aligned write on a fresh layer,
		// write directly to L2 (skipping memtable → L0 → L1 → L2 pipeline).
		if remaining >= BlockSize && offset%BlockSize == 0 && ly.canDirectL2() {
			if err := ly.writeBlockDirectL2(data[written:written+BlockSize], offset); err != nil {
				return err
			}
			written += BlockSize
			offset += BlockSize
			continue
		}

		pageIdx, pageOff := PageIdxOf(offset)
		chunk := min(uint64(remaining), PageSize-pageOff)

		if err := ly.writePage(pageIdx, pageOff, data[written:written+int(chunk)]); err != nil {
			return err
		}
		written += int(chunk)
		offset += chunk
	}
	return nil
}

// canDirectL2 returns true if the layer is fresh enough for direct L2 writes
// (no L0 entries, no L1 ranges, empty memtable).
func (ly *layer) canDirectL2() bool {
	ly.mu.RLock()
	defer ly.mu.RUnlock()
	return len(ly.index.L0) == 0 && ly.l1Map.Len() == 0 && ly.memtable.size.Load() == 0
}

// writeBlockDirectL2 writes a full BlockSize chunk directly as an L2 block,
// bypassing the memtable/flush/compaction pipeline. The caller must ensure
// offset is block-aligned and len(data) == BlockSize.
func (ly *layer) writeBlockDirectL2(data []byte, offset uint64) error {
	blockIdx := BlockIdx(offset / BlockSize)

	// Split into pages.
	pages := make([]blockPage, BlockPages)
	for i := range BlockPages {
		pages[i] = blockPage{
			offset: uint16(i),
			data:   data[i*PageSize : (i+1)*PageSize],
		}
	}

	// Build the L2 block blob.
	blob, err := buildBlock(blockIdx, pages)
	if err != nil {
		return fmt.Errorf("build direct L2 block %d: %w", blockIdx, err)
	}

	// Upload.
	key := blockKey(ly.id, "l2", ly.writeLeaseSeq, blockIdx)
	ctx := context.Background()
	if err := ly.store.PutReader(ctx, key, bytes.NewReader(blob)); err != nil {
		return fmt.Errorf("upload direct L2 block %d: %w", blockIdx, err)
	}

	// Update the L2 map and persist the index so the entries survive
	// a volume close/reopen cycle.
	ly.mu.Lock()
	ly.l2Map = ly.l2Map.Set(blockIdx, ly.id, ly.writeLeaseSeq)
	ly.mu.Unlock()

	if err := ly.saveIndex(ctx); err != nil {
		return fmt.Errorf("save index after direct L2 block %d: %w", blockIdx, err)
	}

	return nil
}

func (ly *layer) writePage(pageIdx PageIdx, pageOff uint64, chunk []byte) error {
	ctx := context.Background()
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

	ly.mu.RLock()
	mt := ly.memtable
	err := mt.put(pageIdx, page[:])
	ly.mu.RUnlock()

	pageLock.Unlock()

	metrics.MemtableBytes.Set(float64(mt.size.Load()))

	// Backpressure: if the memtable is full, freeze and flush.
	for errors.Is(err, errMemtableFull) {
		bpStart := time.Now()
		if err := ly.maybeFreezeAndFlush(); err != nil {
			return err
		}
		ly.mu.RLock()
		mt = ly.memtable
		err = mt.put(pageIdx, page[:])
		ly.mu.RUnlock()
		metrics.BackpressureWaits.Inc()
		metrics.BackpressureWaitDuration.Observe(time.Since(bpStart).Seconds())
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
		bpStart := time.Now()
		if err := ly.flushFrozenTables(); err != nil {
			return err
		}
		metrics.BackpressureWaits.Inc()
		metrics.BackpressureWaitDuration.Observe(time.Since(bpStart).Seconds())
	}

	// Check if memtable needs freezing.
	if mt.size.Load() >= ly.config.FlushThreshold {
		if err := ly.maybeFreezeAndFlush(); err != nil {
			return err
		}
	}

	return nil
}

// PunchHole zeroes all pages fully or partially covered by the range.
func (ly *layer) PunchHole(offset, length uint64) error {
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
		if err := ly.Write(zeros, offset); err != nil {
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
		if err := ly.Write(zeros, pageIdx.ByteOffset()); err != nil {
			return err
		}
	}

	// Write zero pages for fully-covered interior pages.
	for pageIdx := firstFullPage; pageIdx < lastFullPage; pageIdx++ {
		if err := ly.Write(zeroPage[:], pageIdx.ByteOffset()); err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes all pending data to S3.
func (ly *layer) Flush() error {
	if err := ly.freezememtable(); err != nil {
		return err
	}
	return ly.flushFrozenTables()
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
	// Advance nextSeq to guarantee a unique L0 key for each frozen memtable.
	freezeSeq := ly.nextSeq.Add(1)
	mt, err := newMemtable(memDir, freezeSeq, ly.config.maxMemtablePages())
	if err != nil {
		return fmt.Errorf("create new memtable: %w", err)
	}

	old := ly.memtable
	old.freeze(freezeSeq)

	ly.frozenMu.Lock()
	ly.frozenTables = append(ly.frozenTables, old)
	metrics.FrozenTableCount.Set(float64(len(ly.frozenTables)))
	ly.frozenMu.Unlock()

	ly.memtable = mt
	metrics.MemtableBytes.Set(0)
	return nil
}

func (ly *layer) maybeFreezeAndFlush() error {
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
	return ly.flushFrozenTables()
}

func (ly *layer) flushFrozenTables() error {
	ly.flushMu.Lock()
	defer ly.flushMu.Unlock()
	return ly.flushFrozenTablesLocked(5)
}

func (ly *layer) flushFrozenTablesLocked(maxRetries int) error {
	for {
		ly.frozenMu.RLock()
		if len(ly.frozenTables) == 0 {
			ly.frozenMu.RUnlock()
			return nil
		}
		mt := ly.frozenTables[0]
		ly.frozenMu.RUnlock()

		if err := ly.flushMemtable(mt, maxRetries); err != nil {
			return err
		}

		ly.frozenMu.Lock()
		ly.frozenTables = ly.frozenTables[1:]
		metrics.FrozenTableCount.Set(float64(len(ly.frozenTables)))
		ly.frozenMu.Unlock()

		mt.cleanup()
	}
}

func (ly *layer) flushMemtable(mt *memtable, maxRetries int) error {
	entries := mt.entries()
	if len(entries) == 0 {
		return nil
	}

	data, l0entry, err := buildL0(mt, ly.id, entries, ly.writeLeaseSeq)
	if err != nil {
		return fmt.Errorf("build L0: %w", err)
	}

	cached := make([]cachedL0Page, 0, len(entries))
	for _, e := range entries {
		pageData, err := mt.readData(e.slot)
		if err == nil {
			cached = append(cached, cachedL0Page{pageIdx: e.pageIdx, data: pageData})
		}
	}

	return ly.commitL0Blob(data, l0entry, cached, maxRetries)
}

func (ly *layer) WritePagesDirectL0(pages []loophole.DirectPage) error {
	if len(pages) == 0 {
		return nil
	}

	// Expand multi-page ranges into individual pages and validate.
	sorted := expandDirectPages(pages)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	})
	for i := range sorted {
		if i > 0 && sorted[i-1].Offset == sorted[i].Offset {
			return fmt.Errorf("duplicate direct page offset %d", sorted[i].Offset)
		}
	}

	ly.flushMu.Lock()
	defer ly.flushMu.Unlock()

	if err := ly.freezememtable(); err != nil {
		return err
	}
	if err := ly.flushFrozenTablesLocked(5); err != nil {
		return err
	}

	seq := ly.nextSeq.Add(1)
	data, l0entry, err := buildL0FromDirectPages(ly.id, sorted, ly.writeLeaseSeq, seq)
	if err != nil {
		return fmt.Errorf("build direct L0: %w", err)
	}

	cached := make([]cachedL0Page, 0, len(sorted))
	for _, page := range sorted {
		cached = append(cached, cachedL0Page{
			pageIdx: PageIdx(page.Offset / PageSize),
			data:    append([]byte(nil), page.Data...),
		})
	}
	if err := ly.commitL0Blob(data, l0entry, cached, 5); err != nil {
		return err
	}

	ly.mu.RLock()
	total := totalL0Pages(ly.index.L0)
	ly.mu.RUnlock()
	if total >= ly.config.L0PagesMax {
		if err := ly.tryCompactL0(context.Background()); err != nil {
			slog.Error("direct L0 compaction failed", "layer", ly.id, "error", err)
		}
	}
	return nil
}

// expandDirectPages splits multi-page DirectPage ranges into individual
// single-page entries. Each input range must be page-aligned and a positive
// multiple of PageSize.
func expandDirectPages(pages []loophole.DirectPage) []loophole.DirectPage {
	n := 0
	for _, p := range pages {
		n += len(p.Data) / PageSize
	}
	out := make([]loophole.DirectPage, 0, n)
	for _, p := range pages {
		if p.Offset%PageSize != 0 {
			panic(fmt.Sprintf("direct page offset %d is not page-aligned", p.Offset))
		}
		if len(p.Data) == 0 || len(p.Data)%PageSize != 0 {
			panic(fmt.Sprintf("direct page at offset %d has size %d, must be a positive multiple of %d", p.Offset, len(p.Data), PageSize))
		}
		for off := 0; off < len(p.Data); off += PageSize {
			out = append(out, loophole.DirectPage{
				Offset: p.Offset + uint64(off),
				Data:   p.Data[off : off+PageSize],
			})
		}
	}
	return out
}

func (ly *layer) commitL0Blob(data []byte, l0entry l0Entry, cached []cachedL0Page, maxRetries int) error {
	ctx := context.Background()
	flushStart := time.Now()
	slog.Debug("flush: uploading L0", "layer", ly.id, "key", l0entry.Key, "pages", len(l0entry.Pages), "bytes", len(data))

	uploadStart := time.Now()
	var err error
	for attempt := range maxRetries {
		err = ly.store.PutReader(ctx, l0entry.Key, bytes.NewReader(data))
		if err == nil {
			metrics.FlushUploadDuration.Observe(time.Since(uploadStart).Seconds())
			slog.Debug("flush: uploaded L0", "layer", ly.id, "key", l0entry.Key, "bytes", len(data), "dur", time.Since(flushStart))
			break
		}
		if attempt == maxRetries-1 {
			metrics.FlushErrors.Inc()
			return fmt.Errorf("upload L0 (after %d attempts): %w", maxRetries, err)
		}
		slog.Warn("flush: retrying L0 upload", "layer", ly.id, "key", l0entry.Key, "attempt", attempt+1, "error", err)
	}

	metrics.FlushPages.Add(float64(len(l0entry.Pages)))
	metrics.FlushBlocks.Add(float64(len(l0entry.Pages)))
	metrics.FlushBytes.Add(float64(len(data)))

	p, _ := parseL0(data)
	if p != nil {
		p.store = ly.store
		p.key = l0entry.Key
		ly.l0Cache.put(l0entry.Key, p)
	}

	for _, page := range cached {
		ly.cachePage(layerIDFromBlobKey(l0entry.Key), page.pageIdx, page.data)
	}

	ly.mu.Lock()
	ly.index.L0 = append(ly.index.L0, l0entry)
	ly.mu.Unlock()

	ly.log("FLUSH layer=%s l0=%s pages=%d",
		ly.id[:min(8, len(ly.id))], l0entry.Key, len(l0entry.Pages))

	idxStart := time.Now()
	if err := ly.saveIndex(ctx); err != nil {
		ly.mu.Lock()
		ly.index.L0 = ly.index.L0[:len(ly.index.L0)-1]
		ly.mu.Unlock()
		return fmt.Errorf("save index: %w", err)
	}
	slog.Debug("flush: saved index", "layer", ly.id, "dur", time.Since(idxStart), "total_dur", time.Since(flushStart))

	metrics.FlushDuration.Observe(time.Since(flushStart).Seconds())
	return nil
}

// Snapshot freezes this layer and creates a child layer that inherits all
// data via a copy of this layer's index.json.
// Snapshot creates a child layer that inherits this layer's complete state.
//
// IMPORTANT INVARIANT: After calling Snapshot, the caller MUST ensure this
// layer is never written to again. The child's L1/L2 block ranges reference
// objects keyed by this layer's ID and writeLeaseSeq. If this layer continues
// to compact (rewrite L1 blocks), the child's references become stale.
//
// The volume layer enforces this by calling relayer() after Snapshot, which
// creates a new layer for the parent volume. The old layer is effectively
// frozen — only frozen (immutable) layers may be shared between volumes.
func (ly *layer) Snapshot(childID string) error {
	// 1. Flush everything.
	if err := ly.Flush(); err != nil {
		return fmt.Errorf("flush before snapshot: %w", err)
	}

	ctx := context.Background()
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
	if err := childStore.PutIfNotExists(ctx, "index.json", idxData, map[string]string{
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("create child index.json: %w", err)
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

// LayerDebugInfo holds layer structure details for the debug endpoint.
type LayerDebugInfo struct {
	LayerID        string `json:"layer_id"`
	L0Count        int    `json:"l0_count"`
	L0TotalPages   int    `json:"l0_total_pages"`
	L1Ranges       int    `json:"l1_ranges"`
	L2Ranges       int    `json:"l2_ranges"`
	MemtablePages  int    `json:"memtable_pages"`
	MemtableMax    int    `json:"memtable_max"`
	FrozenCount    int    `json:"frozen_memtables"`
	L0CacheEntries int    `json:"l0_cache_entries"`
	BlockCacheEnts int    `json:"block_cache_entries"`
}

func (ly *layer) debugInfo() LayerDebugInfo {
	ly.mu.RLock()
	idx := ly.index
	l1Ranges := len(ly.l1Map.Ranges())
	l2Ranges := len(ly.l2Map.Ranges())
	mtPages, mtMax := 0, 0
	if ly.memtable != nil {
		ly.memtable.mu.RLock()
		mtPages = len(ly.memtable.index)
		mtMax = ly.memtable.maxPages
		ly.memtable.mu.RUnlock()
	}
	ly.mu.RUnlock()

	ly.frozenMu.RLock()
	frozenCount := len(ly.frozenTables)
	ly.frozenMu.RUnlock()

	l0Pages := 0
	for i := range idx.L0 {
		l0Pages += len(idx.L0[i].Pages)
	}

	ly.l0Cache.mu.RLock()
	l0Cached := len(ly.l0Cache.entries)
	ly.l0Cache.mu.RUnlock()

	ly.blockCache.mu.RLock()
	blockCached := len(ly.blockCache.entries)
	ly.blockCache.mu.RUnlock()

	return LayerDebugInfo{
		LayerID:        ly.id,
		L0Count:        len(idx.L0),
		L0TotalPages:   l0Pages,
		L1Ranges:       l1Ranges,
		L2Ranges:       l2Ranges,
		MemtablePages:  mtPages,
		MemtableMax:    mtMax,
		FrozenCount:    frozenCount,
		L0CacheEntries: l0Cached,
		BlockCacheEnts: blockCached,
	}
}

// flushWriteDelay is how long to wait after a write-triggered flush
// before actually flushing, to batch nearby writes.
const flushWriteDelay = 2 * time.Second

// startPeriodicFlush starts the background flush goroutine.
func (ly *layer) startPeriodicFlush(parentCtx context.Context) {
	if ly.config.FlushInterval < 0 {
		return
	}
	if ly.flushStop != nil {
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

func (ly *layer) stopPeriodicFlush() {
	if ly.flushStop == nil {
		return
	}
	if ly.flushCancel != nil {
		ly.flushCancel()
		ly.flushCancel = nil
	}
	close(ly.flushStop)
	<-ly.flushDone
	ly.flushStop = nil
	ly.flushDone = nil
	ly.flushNotify = nil
	ly.writeNotify = nil
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
			metrics.EarlyFlushes.Inc()
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
		timer.Reset(ly.config.FlushInterval)
	}
}

func (ly *layer) doPeriodicFlush(ctx context.Context) {
	// Flush frozen tables. TryLock avoids blocking if another goroutine
	// (e.g. an explicit Flush call) already holds flushMu. Under synctest,
	// a blocked goroutine prevents time from advancing, which deadlocks
	// retry timers.
	if ly.flushMu.TryLock() {
		err := ly.flushFrozenTablesLocked(5)
		ly.flushMu.Unlock()
		if err != nil {
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
				err := ly.flushFrozenTablesLocked(5)
				ly.flushMu.Unlock()
				if err != nil {
					slog.Error("periodic flush failed", "layer", ly.id, "error", err)
				}
			}
		}
	}
	ly.lastFlushAt.Store(time.Now().UnixMilli())

	// Compact L0→L1 if threshold reached. Use tryCompactL0 to avoid
	// blocking if an explicit CompactL0 is already running. Under
	// synctest, a blocking Lock on compactMu combined with RetryStore's
	// backoff sleeps creates a cascade: each retry sleep advances the
	// fake clock, waking more periodicFlush goroutines that also try
	// to compact and retry, causing a deadlock.
	slog.Debug("periodicFlush: attempting compaction", "layer", ly.id)
	if err := ly.tryCompactL0(ctx); err != nil {
		slog.Error("L0 compaction failed", "layer", ly.id, "error", err)
	}
}

// blockKey returns the S3 key for a block blob, incorporating the write lease seq.
func blockKey(layerID, level string, writeLeaseSeq uint64, blockAddr BlockIdx) string {
	return fmt.Sprintf("layers/%s/%s/%016x-%016x", layerID, level, writeLeaseSeq, blockAddr)
}

func layerIDFromBlobKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) >= 2 && parts[0] == "layers" {
		return parts[1]
	}
	return ""
}

func (ly *layer) shouldRetryAfterLayoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, loophole.ErrNotFound) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "bad message") ||
		strings.Contains(msg, "bad magic") ||
		strings.Contains(msg, "parse block") ||
		strings.Contains(msg, "parse l0") ||
		strings.Contains(msg, "read block") ||
		strings.Contains(msg, "read l0") ||
		strings.Contains(msg, "extends beyond") ||
		strings.Contains(msg, "crc")
}

func (ly *layer) waitRefreshForLayoutChange(ctx context.Context, prevGen uint64) (bool, error) {
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		idx, _, err := loophole.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
		if err == nil && idx.LayoutGen != prevGen {
			if err := ly.refresh(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (ly *layer) acquireCompactionLease(ctx context.Context) (func(), error) {
	if ly.lease == nil {
		return func() {}, nil
	}
	key := "compaction-lock.json"
	token := ly.lease.Token()
	data, err := json.Marshal(compactionLockFile{Token: token})
	if err != nil {
		return nil, fmt.Errorf("marshal compaction lock: %w", err)
	}
	for attempts := 0; attempts < 3; attempts++ {
		err := ly.layerStore.PutIfNotExists(ctx, key, data)
		if err == nil {
			return func() {
				_ = ly.layerStore.DeleteObject(context.Background(), key)
			}, nil
		}
		if !errors.Is(err, loophole.ErrExists) {
			return nil, fmt.Errorf("create compaction lock: %w", err)
		}
		lock, _, err := loophole.ReadJSON[compactionLockFile](ctx, ly.layerStore, key)
		if err != nil {
			if errors.Is(err, loophole.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("read compaction lock: %w", err)
		}
		if lock.Token == token {
			return func() {}, nil
		}
		if err := ly.lease.CheckAvailable(ctx, lock.Token); err != nil {
			return nil, err
		}
		if err := ly.layerStore.DeleteObject(ctx, key); err != nil && !errors.Is(err, loophole.ErrNotFound) {
			return nil, fmt.Errorf("delete stale compaction lock: %w", err)
		}
	}
	return nil, fmt.Errorf("acquire compaction lock for layer %s", ly.id)
}

func (ly *layer) outputSeq(existingLayer string, existingSeq, fallback uint64) uint64 {
	if existingLayer == ly.id && existingSeq != 0 {
		return existingSeq
	}
	return fallback
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

// tryCompactL0 attempts compaction but skips if another compaction is
// already in progress. Used by periodicFlush to avoid blocking.
func (ly *layer) tryCompactL0(ctx context.Context) error {
	if !ly.compactMu.TryLock() {
		slog.Debug("tryCompactL0: skipped, compaction already running", "layer", ly.id)
		return nil
	}
	defer ly.compactMu.Unlock()
	return ly.compactL0Locked(ctx, false)
}

// CompactL0 merges L0 entries into L1 blocks if the L0 page count
// exceeds the configured threshold.
func (ly *layer) CompactL0() error {
	ly.compactMu.Lock()
	defer ly.compactMu.Unlock()
	return ly.compactL0Locked(context.Background(), false)
}

// ForceCompactL0 merges all L0 entries into L1 blocks regardless of threshold.
func (ly *layer) ForceCompactL0() error {
	ly.compactMu.Lock()
	defer ly.compactMu.Unlock()
	return ly.compactL0Locked(context.Background(), true)
}

func (ly *layer) compactL0Locked(ctx context.Context, force bool) error {
	ly.mu.RLock()
	startLayoutGen := ly.index.LayoutGen
	l0entries := make([]l0Entry, len(ly.index.L0))
	copy(l0entries, ly.index.L0)
	ly.mu.RUnlock()

	total := totalL0Pages(l0entries)
	if !force && total < ly.config.L0PagesMax {
		slog.Debug("compactL0: below threshold", "layer", ly.id,
			"l0Files", len(l0entries), "l0Pages", total, "threshold", ly.config.L0PagesMax)
		return nil
	}
	if total == 0 {
		return nil
	}

	releaseLock, err := ly.acquireCompactionLease(ctx)
	if err != nil {
		if refreshed, refreshErr := ly.waitRefreshForLayoutChange(ctx, startLayoutGen); refreshErr == nil && refreshed {
			return nil
		}
		return fmt.Errorf("acquire compaction lease: %w", err)
	}
	defer releaseLock()

	slog.Info("compactL0: starting", "layer", ly.id,
		"l0Files", len(l0entries), "l0Pages", total, "threshold", ly.config.L0PagesMax)

	compactTimer := metrics.NewTimer(metrics.CompactDuration)
	metrics.CompactRunning.Set(1)
	metrics.CompactL0InputPages.Add(float64(total))
	defer func() {
		metrics.CompactRunning.Set(0)
		metrics.CompactBlocksTotal.Set(0)
		metrics.CompactBlocksProcessed.Set(0)
		compactTimer.ObserveDuration()
	}()

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
	}

	// Snapshot l1Map/l2Map for reads. Each block has a unique address, so
	// we can process all blocks in parallel and apply map updates after.
	ly.mu.RLock()
	snapL1Map := ly.l1Map
	snapL2Map := ly.l2Map
	ly.mu.RUnlock()

	// blockResult holds the output of processing one block.
	type blockResult struct {
		blockAddr BlockIdx
		key       string
		pb        *parsedBlock
		promote   bool // true = L2 promotion, false = L1
		seq       uint64
	}

	// Process blocks in parallel with bounded concurrency.
	// Each in-flight block holds ~4MB of data, so 4 workers ≈ 16MB peak.
	const maxCompactWorkers = 4

	totalBlocks := len(blockGroups)
	metrics.CompactBlocksTotal.Set(float64(totalBlocks))
	var blocksProcessed atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxCompactWorkers)

	var resultsMu sync.Mutex
	results := make([]blockResult, 0, totalBlocks)

	for blockAddr, sources := range blockGroups {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			blockStart := time.Now()
			n := blocksProcessed.Add(1)
			if n%50 == 0 || int(n) == totalBlocks {
				slog.Debug("compactL0: progress", "layer", ly.id,
					"blocks", fmt.Sprintf("%d/%d", n, totalBlocks))
			}
			metrics.CompactBlocksProcessed.Set(float64(n))

			// Extract L0 compressed entries directly — no decompression.
			// Process sources in order so newer L0 entries win.
			sort.Slice(sources, func(i, j int) bool {
				return sources[i].l0Idx < sources[j].l0Idx
			})

			l0Entries := make(map[uint16]compressedBlockPage, len(sources))
			for _, src := range sources {
				pageOffset := uint16(src.pageIdx % BlockPages)
				p, err := ly.getParsedL0(gctx, &l0entries[src.l0Idx])
				if err != nil {
					return fmt.Errorf("read L0 for compact: %w", err)
				}
				if ce, ok := p.compressedEntry(src.pageIdx); ok {
					l0Entries[pageOffset] = ce
				}
			}

			// Build the set of L0 offsets that override existing blocks.
			l0Offsets := make(map[uint16]struct{}, len(l0Entries))
			for off := range l0Entries {
				l0Offsets[off] = struct{}{}
			}

			// Count merged pages to decide on L2 promotion.
			var existingL1 *parsedBlock
			existingL1Layer, existingL1Seq := snapL1Map.Find(blockAddr)
			mergedCount := len(l0Offsets)
			if existingL1Layer != "" {
				key := blockKey(existingL1Layer, "l1", existingL1Seq, blockAddr)
				pb, err := ly.getParsedBlock(gctx, key)
				if err != nil {
					return fmt.Errorf("read existing L1 block %d: %w", blockAddr, err)
				}
				existingL1 = pb
				for _, ie := range pb.index {
					if _, overwritten := l0Offsets[ie.PageOffset]; !overwritten {
						mergedCount++
					}
				}
			}

			promote := mergedCount >= L1PromoteThreshold
			var existingL2 *parsedBlock
			existingL2Layer, existingL2Seq := snapL2Map.Find(blockAddr)
			if promote {
				if existingL2Layer != "" {
					l2Key := blockKey(existingL2Layer, "l2", existingL2Seq, blockAddr)
					pb, err := ly.getParsedBlock(gctx, l2Key)
					if err != nil {
						return fmt.Errorf("read existing L2 block %d: %w", blockAddr, err)
					}
					existingL2 = pb
				}
			}

			// Collect all pre-compressed entries: L0 + existing block entries.
			var existing []compressedBlockPage
			for _, ce := range l0Entries {
				existing = append(existing, ce)
			}

			if promote {
				// Promotion: merge L2 + L1 + L0.
				l1Offsets := make(map[uint16]struct{})
				if existingL1 != nil {
					for _, ie := range existingL1.index {
						l1Offsets[ie.PageOffset] = struct{}{}
					}
				}

				// From L2: exclude pages overwritten by L1 or L0.
				if existingL2 != nil {
					l1AndL0 := make(map[uint16]struct{}, len(l1Offsets)+len(l0Offsets))
					for off := range l1Offsets {
						l1AndL0[off] = struct{}{}
					}
					for off := range l0Offsets {
						l1AndL0[off] = struct{}{}
					}
					existing = append(existing, existingL2.compressedEntriesExcluding(l1AndL0)...)
				}

				// From L1: exclude pages overwritten by L0.
				if existingL1 != nil {
					existing = append(existing, existingL1.compressedEntriesExcluding(l0Offsets)...)
				}
			} else {
				// L1 merge: copy untouched L1 entries, overlay L0 pages.
				if existingL1 != nil {
					existing = append(existing, existingL1.compressedEntriesExcluding(l0Offsets)...)
				}
			}

			blockData, err := patchBlock(blockAddr, existing, nil)
			if err != nil {
				return fmt.Errorf("build block %d: %w", blockAddr, err)
			}

			// Upload.
			var key string
			var outputSeq uint64
			if promote {
				outputSeq = ly.outputSeq(existingL2Layer, existingL2Seq, ly.writeLeaseSeq)
				key = blockKey(ly.id, "l2", outputSeq, blockAddr)
			} else {
				outputSeq = ly.outputSeq(existingL1Layer, existingL1Seq, ly.writeLeaseSeq)
				key = blockKey(ly.id, "l1", outputSeq, blockAddr)
			}
			if err := ly.store.PutReader(gctx, key, bytes.NewReader(blockData)); err != nil {
				return fmt.Errorf("upload block %d: %w", blockAddr, err)
			}

			metrics.CompactBlocksWritten.Inc()
			metrics.CompactBytesWritten.Add(float64(len(blockData)))
			if promote {
				metrics.CompactL2Promotions.Inc()
			}
			metrics.CompactBlockDuration.Observe(time.Since(blockStart).Seconds())

			// Parse for cache (best-effort).
			var pb *parsedBlock
			if parsed, parseErr := parseBlock(blockData); parseErr == nil {
				pb = parsed
			}

			resultsMu.Lock()
			results = append(results, blockResult{
				blockAddr: blockAddr,
				key:       key,
				pb:        pb,
				promote:   promote,
				seq:       outputSeq,
			})
			resultsMu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Apply map updates sequentially from collected results.
	newL1Map := snapL1Map
	newL2Map := snapL2Map
	for _, r := range results {
		if r.promote {
			newL2Map = newL2Map.Set(r.blockAddr, ly.id, r.seq)
			newL1Map = newL1Map.Remove(r.blockAddr)
		} else {
			newL1Map = newL1Map.Set(r.blockAddr, ly.id, r.seq)
		}
		if r.pb != nil {
			ly.blockCache.put(r.key, r.pb)
			// Update the page cache for all pages in this block so stale
			// entries (from a previous compaction that wrote the same block
			// key) are overwritten with the correct merged data.
			if ly.diskCache != nil {
				for _, ie := range r.pb.index {
					pageIdx := PageIdx(uint64(r.blockAddr)*BlockPages + uint64(ie.PageOffset))
					if data, _, err := r.pb.findPage(context.Background(), pageIdx); err == nil && data != nil {
						ly.cachePage(ly.id, pageIdx, data)
					}
				}
			}
		}
	}

	// All blocks processed successfully. Atomically swap maps and remove
	// only the L0 entries we processed. Entries added by concurrent flushes
	// during compaction are preserved.
	var committed layerIndex
	ly.mu.Lock()
	ly.l1Map = newL1Map
	ly.l2Map = newL2Map
	nProcessed := len(l0entries)
	remaining := ly.index.L0[nProcessed:]
	ly.index.L0 = make([]l0Entry, len(remaining))
	copy(ly.index.L0, remaining)
	ly.index.LayoutGen++
	committed = ly.index
	committed.NextSeq = ly.nextSeq.Load()
	committed.L1 = ly.l1Map.Ranges()
	committed.L2 = ly.l2Map.Ranges()
	ly.mu.Unlock()

	metrics.L0Compactions.Inc()

	slog.Info("compactL0: done", "layer", ly.id,
		"processed", nProcessed, "remaining", len(remaining),
		"l1Blocks", len(blockGroups))

	// CAS-save the updated index so readers only observe the new layout after
	// commit, and other compactors converge if the layer changed underneath us.
	ly.indexMu.Lock()
	current, etag, err := loophole.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
	if err != nil {
		ly.indexMu.Unlock()
		return fmt.Errorf("read current index for compact commit: %w", err)
	}
	if current.LayoutGen > startLayoutGen {
		ly.indexMu.Unlock()
		if err := ly.refresh(ctx); err != nil {
			return err
		}
		return nil
	}
	data, err := json.Marshal(committed)
	if err != nil {
		ly.indexMu.Unlock()
		return fmt.Errorf("marshal compacted index: %w", err)
	}
	if _, err := ly.layerStore.PutBytesCAS(ctx, "index.json", data, etag); err != nil {
		ly.indexMu.Unlock()
		if refreshed, refreshErr := ly.waitRefreshForLayoutChange(ctx, startLayoutGen); refreshErr == nil && refreshed {
			return nil
		}
		return fmt.Errorf("commit compacted index: %w", err)
	}
	ly.indexMu.Unlock()
	return nil
}
