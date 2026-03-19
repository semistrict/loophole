package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/objstore"
	"golang.org/x/sync/errgroup"
)

// layer is a writable storage layer backed by a tiered L1/L2 structure.
// Data flows: memtable → frozen memtables → L1 → L2 → zeros.
type layer struct {
	id         string
	store      objstore.ObjectStore // global root (for reading blobs by full key)
	layerStore objstore.ObjectStore // rooted at layers/<id>/ (for index.json)

	config    Config
	diskCache PageCache
	cacheDir  string
	lease     *objstore.LeaseManager

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
	indexMu      sync.Mutex // serializes index.json publication within one process

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
}

func (ly *layer) pageLock(idx PageIdx) *sync.Mutex {
	return &ly.pageLocks[uint64(idx)%uint64(len(ly.pageLocks))]
}

// layerSnapshot captures the layer state for a consistent read.
type layerSnapshot struct {
	layoutGen uint64
	memtable  *memtable
	frozen    []*memtable
	l1        *blockRangeMap
	l2        *blockRangeMap
}

// openLayer loads a layer from S3 and initializes its local state.
// store is the global root (not scoped to a layer prefix).
func openLayer(ctx context.Context, store objstore.ObjectStore, id string, config Config, diskCache PageCache, cacheDir string) (*layer, error) {
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
func openFrozenLayer(ctx context.Context, store objstore.ObjectStore, id string, config Config, diskCache PageCache) (*layer, error) {
	idx, _, err := objstore.ReadJSON[layerIndex](ctx, store.At("layers/"+id), "index.json")
	if err != nil {
		return nil, fmt.Errorf("read frozen layer index: %w", err)
	}
	return initFrozenLayerFromIndex(store, id, config, diskCache, idx), nil
}

// initLayerFromIndex creates a mutable layer from a pre-loaded index.
// No S3 reads — the caller already has the index. Used by Clone to avoid
// re-reading the child index that was just written.
func initLayerFromIndex(store objstore.ObjectStore, id string, config Config, diskCache PageCache, cacheDir string, idx layerIndex) (*layer, error) {
	config.setDefaults()
	ly := &layer{
		id:         id,
		store:      store,
		layerStore: store.At("layers/" + id),
		config:     config,
		diskCache:  diskCache,
		cacheDir:   cacheDir,
	}
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
func initFrozenLayerFromIndex(store objstore.ObjectStore, id string, config Config, diskCache PageCache, idx layerIndex) *layer {
	config.setDefaults()
	ly := &layer{
		id:         id,
		store:      store,
		layerStore: store.At("layers/" + id),
		config:     config,
		diskCache:  diskCache,
	}
	ly.blockCache.init(config.MaxCacheEntries)

	ly.index = idx
	ly.nextSeq.Store(idx.NextSeq)
	ly.l1Map = newBlockRangeMap(idx.L1)
	ly.l2Map = newBlockRangeMap(idx.L2)
	return ly
}

func (ly *layer) loadIndex(ctx context.Context) error {
	idx, _, err := objstore.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
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
	idx, _, err := objstore.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
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

// snapshotLayers captures current layer state under both read locks
// simultaneously, ensuring an atomic view of memtable + frozen + L1/L2.
func (ly *layer) snapshotLayers() layerSnapshot {
	ly.mu.RLock()
	ly.frozenMu.RLock()
	snap := layerSnapshot{
		layoutGen: ly.index.LayoutGen,
		memtable:  ly.memtable,
		frozen:    ly.frozenTables,
		l1:        ly.l1Map,
		l2:        ly.l2Map,
	}
	ly.frozenMu.RUnlock()
	ly.mu.RUnlock()
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

// ReadPages collects per-page slices for the given byte range using zero-copy
// references where possible. Each slice is appended to *slices, and each
// corresponding unpin function (or nil) is appended to *unpins.
func (ly *layer) ReadPages(ctx context.Context, offset uint64, length int, slices *[][]byte, unpins *[]func()) error {
	snap := ly.snapshotLayers()
	err := ly.readPagesWithSnapshot(ctx, offset, length, slices, unpins, &snap)
	if err == nil {
		return nil
	}
	if !ly.shouldRetryAfterLayoutError(err) {
		return err
	}
	refreshed, refreshErr := ly.waitRefreshForLayoutChange(ctx, snap.layoutGen)
	if refreshErr != nil || !refreshed {
		return err
	}
	return ly.ReadPages(ctx, offset, length, slices, unpins)
}

func (ly *layer) readPagesWithSnapshot(ctx context.Context, offset uint64, length int, slices *[][]byte, unpins *[]func(), snap *layerSnapshot) error {
	remaining := length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageIdx, pageOff := PageIdxOf(offset)

		data, unpin, err := ly.readPageRef(ctx, snap, pageIdx)
		if err != nil {
			return err
		}

		slice := data[pageOff:]
		if len(slice) > remaining {
			slice = slice[:remaining]
		}
		*slices = append(*slices, slice)
		*unpins = append(*unpins, unpin)
		n := len(slice)
		offset += uint64(n)
		remaining -= n
	}
	return nil
}

// readPageRef reads a single page using zero-copy references where possible.
// Returns (data, unpin, err). The caller MUST call unpin (if non-nil) when done.
func (ly *layer) readPageRef(ctx context.Context, snap *layerSnapshot, pageIdx PageIdx) ([]byte, func(), error) {
	// 1. Active memtable.
	if snap.memtable != nil {
		if slot, ok := snap.memtable.get(pageIdx); ok {
			data, unpin, err := snap.memtable.readDataRef(slot)
			if err == nil {
				metrics.PageReadSource.WithLabelValues("memtable").Inc()
				return data, unpin, nil
			}
			fresh := ly.snapshotLayers()
			return ly.readPageRef(ctx, &fresh, pageIdx)
		}
	}

	// 2. Frozen memtables (newest first).
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if slot, ok := snap.frozen[i].get(pageIdx); ok {
			data, unpin, err := snap.frozen[i].readDataRef(slot)
			if err == nil {
				metrics.PageReadSource.WithLabelValues("frozen").Inc()
				return data, unpin, nil
			}
			fresh := ly.snapshotLayers()
			return ly.readPageRef(ctx, &fresh, pageIdx)
		}
	}

	// 3. L1 (sparse blocks).
	if layer, seq := snap.l1.Find(pageIdx.Block()); layer != "" {
		if data, unpin := ly.cachedPageRef(layer, pageIdx); data != nil {
			return data, unpin, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			return nil, nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadSource.WithLabelValues("l1").Inc()
			return data, nil, nil // freshly allocated, no unpin needed
		}
	}

	// 4. L2 (dense blocks).
	if layer, seq := snap.l2.Find(pageIdx.Block()); layer != "" {
		if data, unpin := ly.cachedPageRef(layer, pageIdx); data != nil {
			return data, unpin, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			return nil, nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadSource.WithLabelValues("l2").Inc()
			return data, nil, nil
		}
	}

	// 5. Zero page — static, no unpin.
	metrics.PageReadSource.WithLabelValues("zero").Inc()
	return zeroPage[:], nil, nil
}

// cachedPageRef returns a zero-copy reference into the page cache arena.
func (ly *layer) cachedPageRef(sourceLayerID string, pageIdx PageIdx) ([]byte, func()) {
	if !ly.shouldUsePersistentPageCache(sourceLayerID) {
		return nil, nil
	}
	if data, unpin := ly.diskCache.GetPageRef(sourceLayerID, uint64(pageIdx)); data != nil {
		metrics.CacheHits.Inc()
		metrics.PageReadSource.WithLabelValues("cache").Inc()
		return data, unpin
	}
	metrics.CacheMisses.Inc()
	return nil, nil
}

// readPageWith reads a single page using a pre-captured layer snapshot.
func (ly *layer) readPageWith(ctx context.Context, snap *layerSnapshot, pageIdx PageIdx) ([]byte, error) {
	// 1. Active memtable (nil for frozen layers).
	if snap.memtable != nil {
		if slot, ok := snap.memtable.get(pageIdx); ok {
			data, err := snap.memtable.readData(slot)
			if err == nil {
				metrics.PageReadSource.WithLabelValues("memtable").Inc()
				return data, nil
			}
			// Memtable was cleaned up after flush. Retry with a fresh
			// snapshot — the data is now in L1/L2 or a new memtable.
			fresh := ly.snapshotLayers()
			return ly.readPageWith(ctx, &fresh, pageIdx)
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
			// Frozen memtable was cleaned up after flush. Retry with a
			// fresh snapshot — the data is now in L1/L2.
			fresh := ly.snapshotLayers()
			return ly.readPageWith(ctx, &fresh, pageIdx)
		}
	}

	// 3. L1 (sparse blocks).
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

	// 4. L2 (dense blocks).
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

	// 5. Page never written — return zeros.
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
	if data := ly.diskCache.GetPage(sourceLayerID, uint64(pageIdx)); data != nil {
		metrics.CacheHits.Inc()
		metrics.PageReadSource.WithLabelValues("cache").Inc()
		return data
	}
	metrics.CacheMisses.Inc()
	return nil
}

func (ly *layer) cachePage(sourceLayerID string, pageIdx PageIdx, data []byte) {
	if ly.shouldUsePersistentPageCache(sourceLayerID) {
		ly.diskCache.PutPage(sourceLayerID, uint64(pageIdx), data)
	}
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

		pb, err := parseBlock(data, !ly.config.DisableCompression)
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
		// write directly to L2 (skipping memtable → L1 → L2 pipeline).
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
// (no L1 ranges, empty memtable).
func (ly *layer) canDirectL2() bool {
	ly.mu.RLock()
	defer ly.mu.RUnlock()
	return ly.l1Map.Len() == 0 && ly.memtable.size.Load() == 0
}

// writeBlockDirectL2 writes a full BlockSize chunk directly as an L2 block,
// bypassing the memtable/flush pipeline. The caller must ensure
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
	blob, err := buildBlock(blockIdx, pages, !ly.config.DisableCompression)
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

	// Check if memtable needs freezing.
	if mt.size.Load() >= ly.config.FlushThreshold {
		bpStart := time.Now()
		if err := ly.maybeFreezeAndFlush(); err != nil {
			return err
		}
		metrics.BackpressureWaits.Inc()
		metrics.BackpressureWaitDuration.Observe(time.Since(bpStart).Seconds())
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
	// Advance nextSeq to guarantee unique keys for each frozen memtable.
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

const maxFlushWorkers = 8

func (ly *layer) flushFrozenTablesLocked(maxRetries int) error {
	for {
		ly.frozenMu.RLock()
		if len(ly.frozenTables) == 0 {
			ly.frozenMu.RUnlock()
			return nil
		}
		mt := ly.frozenTables[0]
		ly.frozenMu.RUnlock()

		if err := ly.flushMemtableDirectLocked(mt, maxRetries); err != nil {
			return err
		}

		// Remove from frozen only after L1/L2 maps are committed,
		// so concurrent readers always find the data in either
		// the frozen memtable or L1/L2.
		ly.frozenMu.Lock()
		ly.frozenTables = ly.frozenTables[1:]
		metrics.FrozenTableCount.Set(float64(len(ly.frozenTables)))
		ly.frozenMu.Unlock()

		mt.cleanup()
	}
}

// flushMemtableDirectLocked writes a single memtable's pages directly to
// L1/L2 blocks. Pages are merged into existing blocks via read-modify-write.
// The caller is responsible for cleanup of mt on success.
func (ly *layer) flushMemtableDirectLocked(mt *memtable, maxRetries int) error {
	flushCycleStart := time.Now()

	// Phase 1 — Collect pages from memtable, group directly by block.
	entries := mt.entries()
	if len(entries) == 0 {
		return nil
	}

	blockGroups := make(map[BlockIdx][]blockPage, len(entries)/BlockPages+1)
	totalPages := 0
	for _, e := range entries {
		data, err := mt.readData(e.slot)
		if err != nil {
			return fmt.Errorf("read memtable slot %d for page %v: %w", e.slot, e.pageIdx, err)
		}
		blockAddr := e.pageIdx.Block()
		offset := uint16(uint64(e.pageIdx) % BlockPages)
		blockGroups[blockAddr] = append(blockGroups[blockAddr], blockPage{
			offset: offset,
			data:   data,
		})
		totalPages++
	}

	// Snapshot l1Map/l2Map for reads.
	ly.mu.RLock()
	snapL1Map := ly.l1Map
	snapL2Map := ly.l2Map
	ly.mu.RUnlock()

	// Phase 2 — Build + upload blocks in parallel.
	type directBlockResult struct {
		blockAddr BlockIdx
		key       string
		pb        *parsedBlock
		promote   bool
		seq       uint64
		newPages  []blockPage // retained for cache population
	}

	ctx := context.Background()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxFlushWorkers)

	var resultsMu sync.Mutex
	results := make([]directBlockResult, 0, len(blockGroups))

	for blockAddr, newPages := range blockGroups {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			// Build the set of new page offsets for exclusion.
			newOffsets := make(map[uint16]struct{}, len(newPages))
			for _, p := range newPages {
				newOffsets[p.offset] = struct{}{}
			}

			// Read existing L1 block if any.
			var existingL1 *parsedBlock
			existingL1Layer, existingL1Seq := snapL1Map.Find(blockAddr)
			mergedCount := len(newOffsets)
			if existingL1Layer != "" {
				key := blockKey(existingL1Layer, "l1", existingL1Seq, blockAddr)
				pb, err := ly.getParsedBlock(gctx, key)
				if err != nil {
					return fmt.Errorf("read existing L1 block %d: %w", blockAddr, err)
				}
				existingL1 = pb
				for _, ie := range pb.index {
					if _, overwritten := newOffsets[ie.PageOffset]; !overwritten {
						mergedCount++
					}
				}
			}

			promote := mergedCount >= L1PromoteThreshold
			var existingL2 *parsedBlock
			existingL2Layer, existingL2Seq := snapL2Map.Find(blockAddr)
			if promote && existingL2Layer != "" {
				l2Key := blockKey(existingL2Layer, "l2", existingL2Seq, blockAddr)
				pb, err := ly.getParsedBlock(gctx, l2Key)
				if err != nil {
					return fmt.Errorf("read existing L2 block %d: %w", blockAddr, err)
				}
				existingL2 = pb
			}

			// Collect pre-compressed entries from existing blocks.
			var existing []compressedBlockPage
			if promote {
				l1Offsets := make(map[uint16]struct{})
				if existingL1 != nil {
					for _, ie := range existingL1.index {
						l1Offsets[ie.PageOffset] = struct{}{}
					}
				}
				if existingL2 != nil {
					// Exclude pages overwritten by L1 or new pages.
					// Check both maps inline instead of building a union map.
					existing = append(existing, existingL2.compressedEntriesExcluding2(newOffsets, l1Offsets)...)
				}
				if existingL1 != nil {
					existing = append(existing, existingL1.compressedEntriesExcluding(newOffsets)...)
				}
			} else {
				if existingL1 != nil {
					existing = append(existing, existingL1.compressedEntriesExcluding(newOffsets)...)
				}
			}

			blockData, err := patchBlock(blockAddr, existing, newPages, !ly.config.DisableCompression)
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

			uploadStart := time.Now()
			for attempt := range maxRetries {
				err = ly.store.PutReader(gctx, key, bytes.NewReader(blockData))
				if err == nil {
					metrics.FlushUploadDuration.Observe(time.Since(uploadStart).Seconds())
					break
				}
				if gctx.Err() != nil {
					return gctx.Err()
				}
				if attempt == maxRetries-1 {
					metrics.FlushErrors.Inc()
					return fmt.Errorf("upload block %d (after %d attempts): %w", blockAddr, maxRetries, err)
				}
				slog.Warn("direct flush: retrying block upload", "layer", ly.id, "block", blockAddr, "attempt", attempt+1, "error", err)
			}

			metrics.FlushDirectBlocksWritten.Inc()
			metrics.FlushBytes.Add(float64(len(blockData)))
			if promote {
				metrics.FlushDirectL2Promotions.Inc()
			}

			// Parse block header for cache (best-effort).
			var pb *parsedBlock
			if parsed, parseErr := parseBlock(blockData, !ly.config.DisableCompression); parseErr == nil {
				pb = parsed
			}

			resultsMu.Lock()
			results = append(results, directBlockResult{
				blockAddr: blockAddr,
				key:       key,
				pb:        pb,
				promote:   promote,
				seq:       outputSeq,
				newPages:  newPages,
			})
			resultsMu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Phase 3 — Commit: update maps, save index.
	ly.mu.Lock()
	newL1Map := ly.l1Map
	newL2Map := ly.l2Map
	for _, r := range results {
		if r.promote {
			newL2Map = newL2Map.Set(r.blockAddr, ly.id, r.seq)
			newL1Map = newL1Map.Remove(r.blockAddr)
		} else {
			newL1Map = newL1Map.Set(r.blockAddr, ly.id, r.seq)
		}
	}
	ly.l1Map = newL1Map
	ly.l2Map = newL2Map
	ly.index.LayoutGen++
	ly.mu.Unlock()

	if err := ly.saveIndex(ctx); err != nil {
		ly.mu.Lock()
		ly.l1Map = snapL1Map
		ly.l2Map = snapL2Map
		ly.index.LayoutGen--
		ly.mu.Unlock()
		return fmt.Errorf("save index: %w", err)
	}

	// Update caches from results. Use the original uncompressed page data
	// for the disk cache instead of decompressing from the just-built block.
	for _, r := range results {
		if r.pb != nil {
			ly.blockCache.put(r.key, r.pb)
		}
		if ly.diskCache != nil {
			for _, p := range r.newPages {
				pageIdx := r.blockAddr.PageIdx(p.offset)
				ly.cachePage(ly.id, pageIdx, p.data)
			}
		}
	}

	metrics.FlushPages.Add(float64(totalPages))
	metrics.FlushDuration.Observe(time.Since(flushCycleStart).Seconds())

	slog.Debug("direct flush: cycle complete", "layer", ly.id,
		"pages", totalPages, "blocks", len(results), "dur", time.Since(flushCycleStart))

	return nil
}

// Snapshot freezes this layer and creates a child layer that inherits all
// data via a copy of this layer's index.json.
// Snapshot creates a child layer that inherits this layer's complete state.
//
// IMPORTANT INVARIANT: After calling Snapshot, the caller MUST ensure this
// layer is never written to again. The child's L1/L2 block ranges reference
// objects keyed by this layer's ID and writeLeaseSeq. If this layer continues
// to flush (rewrite L1 blocks), the child's references become stale.
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
	L1Ranges       int    `json:"l1_ranges"`
	L2Ranges       int    `json:"l2_ranges"`
	MemtablePages  int    `json:"memtable_pages"`
	MemtableMax    int    `json:"memtable_max"`
	FrozenCount    int    `json:"frozen_memtables"`
	BlockCacheEnts int    `json:"block_cache_entries"`
}

func (ly *layer) debugInfo() LayerDebugInfo {
	ly.mu.RLock()
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

	ly.blockCache.mu.RLock()
	blockCached := len(ly.blockCache.entries)
	ly.blockCache.mu.RUnlock()

	return LayerDebugInfo{
		LayerID:        ly.id,
		L1Ranges:       l1Ranges,
		L2Ranges:       l2Ranges,
		MemtablePages:  mtPages,
		MemtableMax:    mtMax,
		FrozenCount:    frozenCount,
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
}

// blockKey returns the S3 key for a block blob, incorporating the write lease seq.
func blockKey(layerID, level string, writeLeaseSeq uint64, blockAddr BlockIdx) string {
	return fmt.Sprintf("layers/%s/%s/%016x-%016x", layerID, level, writeLeaseSeq, blockAddr)
}

func (ly *layer) shouldRetryAfterLayoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, objstore.ErrNotFound) {
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
		idx, _, err := objstore.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
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

func (ly *layer) outputSeq(existingLayer string, existingSeq, fallback uint64) uint64 {
	if existingLayer == ly.id && existingSeq != 0 {
		return existingSeq
	}
	return fallback
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
