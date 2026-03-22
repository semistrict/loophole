package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/env"
	"github.com/semistrict/loophole/internal/metrics"
	"github.com/semistrict/loophole/internal/safepoint"
	"golang.org/x/sync/errgroup"
)

// layer is a writable storage layer backed by a tiered L1/L2 structure.
// Data flows: active dirty batch → pending dirty batch → L1 → L2 → zeros.
type layer struct {
	id         string
	store      *blob.Store // global root (for reading blobs by full key)
	layerStore *blob.Store // rooted at layers/<id>/ (for index.json)

	config    Config
	diskCache PageCache
	safepoint *safepoint.Safepoint

	// writeLeaseSeq is the monotonically increasing sequence number
	// assigned when the write lease was acquired. All files written
	// during this lease session embed this value in their key names.
	writeLeaseSeq uint64

	// readOnlyFollower is true when this layer was opened in read-only
	// mode. A follower never writes; its layout comes from polling S3.
	readOnlyFollower bool

	// mu protects the complete visible layer state. Reads hold RLock for the
	// full operation. Writes, flush state transitions, publication, refresh, and
	// shutdown state changes take Lock.
	mu             sync.RWMutex
	dirtyCond      *sync.Cond
	index          layerIndex
	indexNew       bool // true if index.json has never been saved to S3
	l1Map          *blockRangeMap
	l2Map          *blockRangeMap
	active         *dirtyBatch
	pending        *dirtyBatch
	drainInFlight  bool
	drainForced    bool
	stopping       bool
	pendingDeletes []string

	nextSeq atomic.Uint64

	// Block header cache: parsed block headers keyed by S3 key (bounded).
	blockCache  boundedCache[*parsedBlock]
	blockFlight singleflight[*parsedBlock]

	// retiredMu protects retiredDirtyPages and retiredDrainActive.
	// Dirty resources removed from batches are queued here and only reclaimed
	// from a safepoint-exclusive section, reusing the existing global
	// borrowed-memory lifetime mechanism instead of adding a second local
	// lifetime protocol for dirty batches.
	retiredMu           sync.Mutex
	retiredDirtyPages   []*Page
	retiredDirtyBatches []*dirtyBatch
	retiredDrainActive  bool

	// pageLocks serialize mutating operations per page. They do not protect a
	// stored field directly; instead they provide the "same page mutates in
	// program order, different pages may proceed concurrently" guarantee that the
	// dirty-batch design depends on.
	pageLocks [256]sync.Mutex

	// The drain worker always exists for writable layers. FlushInterval only
	// controls whether the worker proactively rotates a stale active batch; it
	// does not control whether draining exists.
	flushStop         chan struct{}
	retryStop         chan struct{}
	flushDone         chan struct{}
	flushNotify       chan struct{}
	writeNotify       chan struct{} // poked on every write; triggers early flush if stale
	flushCancel       context.CancelFunc
	lastFlushAt       atomic.Int64 // UnixMilli of last successful flush
	closeOnce         sync.Once
	directL2DebugOnce atomic.Bool // log first canDirectL2 failure only once
}

var errDirectL2Unavailable = errors.New("direct l2 unavailable")

func (ly *layer) pageLock(idx PageIdx) *sync.Mutex {
	return &ly.pageLocks[uint64(idx)%uint64(len(ly.pageLocks))]
}

func (ly *layer) currentDirtyPageBytes() int64 {
	ly.mu.RLock()
	active := ly.active
	ly.mu.RUnlock()
	if active == nil {
		return 0
	}
	return active.bytes()
}

func (ly *layer) requestDrain() {
	ly.mu.Lock()
	ly.drainForced = true
	ly.mu.Unlock()
	ly.notifyDrainWorker()
}

func (ly *layer) beginShutdown() {
	ly.mu.Lock()
	ly.stopping = true
	if ly.retryStop != nil {
		close(ly.retryStop)
		ly.retryStop = nil
	}
	ly.dirtyCond.Broadcast()
	ly.mu.Unlock()
	ly.notifyDrainWorker()
}

// shutdownFlush rotates any remaining active pages into the pending batch and
// waits for shutdown to either flush everything or report that dirty data
// remains after the final drain attempt.
func (ly *layer) shutdownFlush() error {
	ly.requestDrain()

	ly.mu.Lock()
	if ly.pending == nil && ly.active != nil && !ly.active.isEmpty() {
		if ly.rotateActiveToPendingLocked() {
			ly.drainForced = true
		}
	}
	ly.stopping = true
	if ly.retryStop != nil {
		close(ly.retryStop)
		ly.retryStop = nil
	}
	attemptedDrain := false
	for {
		dirtyRemaining := ly.pending != nil || (ly.active != nil && !ly.active.isEmpty())
		if !dirtyRemaining && !ly.drainInFlight {
			ly.dirtyCond.Broadcast()
			ly.mu.Unlock()
			return nil
		}
		if dirtyRemaining && !ly.drainInFlight {
			if attemptedDrain {
				ly.dirtyCond.Broadcast()
				ly.mu.Unlock()
				return fmt.Errorf("flush dirty data on close: storage unavailable")
			}
			attemptedDrain = true
			if ly.pending == nil && ly.active != nil && !ly.active.isEmpty() {
				if ly.rotateActiveToPendingLocked() {
					ly.drainForced = true
				}
			}
			ly.notifyDrainWorkerLocked()
		}
		ly.dirtyCond.Wait()
	}
}

func (ly *layer) notifyDrainWorker() {
	ly.mu.RLock()
	flushNotify := ly.flushNotify
	ly.mu.RUnlock()
	notifyWorkerChan(flushNotify)
}

func (ly *layer) notifyDrainWorkerLocked() {
	notifyWorkerChan(ly.flushNotify)
}

func notifyWorkerChan(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (ly *layer) enqueueRetiredDirtyPages(pages ...*Page) {
	filtered := make([]*Page, 0, len(pages))
	for _, page := range pages {
		if page != nil {
			filtered = append(filtered, page)
		}
	}
	if len(filtered) == 0 {
		return
	}

	ly.retiredMu.Lock()
	ly.retiredDirtyPages = append(ly.retiredDirtyPages, filtered...)
	if ly.retiredDrainActive {
		ly.retiredMu.Unlock()
		return
	}
	ly.retiredDrainActive = true
	ly.retiredMu.Unlock()

	go ly.drainRetiredDirtyPages()
}

func (ly *layer) enqueueRetiredDirtyBatch(batch *dirtyBatch) {
	if batch == nil {
		return
	}

	ly.retiredMu.Lock()
	ly.retiredDirtyBatches = append(ly.retiredDirtyBatches, batch)
	if ly.retiredDrainActive {
		ly.retiredMu.Unlock()
		return
	}
	ly.retiredDrainActive = true
	ly.retiredMu.Unlock()

	go ly.drainRetiredDirtyPages()
}

func (ly *layer) drainRetiredDirtyPages() {
	drainOnce := func() (int, int) {
		var retiredPages []*Page
		var retiredBatches []*dirtyBatch
		drain := func() {
			ly.retiredMu.Lock()
			retiredPages = ly.retiredDirtyPages
			ly.retiredDirtyPages = nil
			retiredBatches = ly.retiredDirtyBatches
			ly.retiredDirtyBatches = nil
			if len(retiredPages) == 0 && len(retiredBatches) == 0 {
				ly.retiredDrainActive = false
			}
			ly.retiredMu.Unlock()
			for _, batch := range retiredBatches {
				for _, page := range batch.clearAndCollect() {
					dirtyPagePool.Put(page)
				}
			}
			for _, page := range retiredPages {
				dirtyPagePool.Put(page)
			}
		}
		if ly.safepoint != nil {
			ly.safepoint.Do(drain)
		} else {
			drain()
		}
		return len(retiredPages), len(retiredBatches)
	}

	for {
		pageCount, batchCount := drainOnce()
		if pageCount == 0 && batchCount == 0 {
			return
		}
	}
}

func (ly *layer) rotateActiveToPendingLocked() bool {
	if ly.active == nil || ly.active.isEmpty() || ly.pending != nil {
		return false
	}
	ly.active.markClosed()
	ly.pending = ly.active
	ly.active = newDirtyBatch(ly.config)
	metrics.DirtyPageBytes.Set(0)
	metrics.FrozenTableCount.Set(1)
	return true
}

func (ly *layer) waitForWriteCapacityLocked(active *dirtyBatch) {
	for {
		if ly.active != active {
			return
		}
		if ly.pending == nil {
			if ly.rotateActiveToPendingLocked() {
				ly.drainForced = true
				ly.notifyDrainWorkerLocked()
			}
			return
		}
		if !ly.drainInFlight {
			ly.notifyDrainWorkerLocked()
		}
		ly.dirtyCond.Wait()
	}
}

type layerParams struct {
	store     *blob.Store
	id        string
	config    Config
	diskCache PageCache
	safepoint *safepoint.Safepoint
}

func newLayer(p layerParams) (*layer, error) {
	p.config.setDefaults()
	ly := &layer{
		id:         p.id,
		store:      p.store,
		layerStore: p.store.At("layers/" + p.id),
		config:     p.config,
		diskCache:  p.diskCache,
		safepoint:  p.safepoint,
	}
	ly.dirtyCond = sync.NewCond(&ly.mu)
	ly.blockCache.init(p.config.MaxCacheEntries)
	return ly, nil
}

func (ly *layer) initDirtyBatches() {
	ly.active = newDirtyBatch(ly.config)
}

// openLayer loads a layer from S3 and initializes its local state.
func openLayer(ctx context.Context, p layerParams) (*layer, error) {
	ly, err := newLayer(p)
	if err != nil {
		return nil, err
	}
	if err := ly.loadIndex(ctx); err != nil {
		ly.Close()
		return nil, fmt.Errorf("load index: %w", err)
	}
	ly.initDirtyBatches()
	ly.startPeriodicFlush(context.Background())
	return ly, nil
}

// openLayerReadOnly loads a layer for read-only follower use. It loads the
// index from S3 but does not create dirty batches or start the flush loop.
func openLayerReadOnly(ctx context.Context, p layerParams) (*layer, error) {
	ly, err := newLayer(p)
	if err != nil {
		return nil, err
	}
	ly.readOnlyFollower = true
	if err := ly.loadIndex(ctx); err != nil {
		ly.Close()
		return nil, fmt.Errorf("load index: %w", err)
	}
	return ly, nil
}

// initLayerFromIndex creates a mutable layer from a pre-loaded index.
// No S3 reads — the caller already has the index.
func initLayerFromIndex(p layerParams, idx layerIndex) (*layer, error) {
	ly, err := newLayer(p)
	if err != nil {
		return nil, err
	}
	ly.index = idx
	ly.nextSeq.Store(idx.NextSeq)
	ly.l1Map = newBlockRangeMap(idx.L1)
	ly.l2Map = newBlockRangeMap(idx.L2)
	ly.initDirtyBatches()
	ly.startPeriodicFlush(context.Background())
	return ly, nil
}

func (ly *layer) loadIndex(ctx context.Context) error {
	idx, _, err := blob.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
	if err != nil {
		// No index.json yet — start fresh.
		ly.index = layerIndex{NextSeq: 1, LayoutGen: 1, DirectL2Eligible: true}
		ly.indexNew = true
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
	idx, _, err := blob.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
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
	ly.mu.Lock()
	defer ly.mu.Unlock()
	return ly.saveIndexLocked(ctx)
}

func (ly *layer) saveIndexLocked(ctx context.Context) error {
	idx := ly.index
	idx.NextSeq = ly.nextSeq.Load()
	idx.L1 = ly.l1Map.Ranges()
	idx.L2 = ly.l2Map.Ranges()

	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	if ly.indexNew {
		if err := ly.layerStore.PutIfNotExists(ctx, "index.json", data); err != nil {
			return fmt.Errorf("put new index: %w", err)
		}
		ly.indexNew = false
		return nil
	}

	_, etag, err := blob.ReadBytes(ctx, ly.layerStore, "index.json")
	if err != nil {
		return fmt.Errorf("read index etag: %w", err)
	}
	if _, err := ly.layerStore.PutBytesCAS(ctx, "index.json", data, etag); err != nil {
		return fmt.Errorf("cas index: %w", err)
	}
	return nil
}

// Read reads data from the layer into buf at the given byte offset.
func (ly *layer) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	for {
		prevGen, total, err := func() (uint64, int, error) {
			ly.mu.RLock()
			defer ly.mu.RUnlock()

			prevGen := ly.index.LayoutGen
			total := 0
			for total < len(buf) {
				if err := ctx.Err(); err != nil {
					return prevGen, total, err
				}
				pageIdx, pageOff := PageIdxOf(offset + uint64(total))
				data, err := ly.readPageLocked(ctx, pageIdx)
				if err != nil {
					return prevGen, total, err
				}
				n := copy(buf[total:], data[pageOff:])
				total += n
			}
			return prevGen, total, nil
		}()
		if err == nil || !ly.readOnlyFollower || !ly.shouldRetryAfterLayoutError(err) {
			return total, err
		}
		changed, refreshErr := ly.waitRefreshForLayoutChange(ctx, prevGen)
		if refreshErr != nil {
			return 0, refreshErr
		}
		if !changed {
			return total, err
		}
	}
}

// ReadPages collects per-page slices for the given byte range using zero-copy
// references where possible. Each slice is appended to *slices. The caller
// must hold the safepoint read lock for the duration of the returned slices' use.
func (ly *layer) ReadPages(ctx context.Context, g safepoint.Guard, offset uint64, length int) ([][]byte, error) {
	for {
		prevGen, slices, err := func() (uint64, [][]byte, error) {
			ly.mu.RLock()
			defer ly.mu.RUnlock()

			prevGen := ly.index.LayoutGen
			var slices [][]byte
			remaining := length
			readOffset := offset
			for remaining > 0 {
				if err := ctx.Err(); err != nil {
					return prevGen, nil, err
				}
				pageIdx, pageOff := PageIdxOf(readOffset)
				data, err := ly.readPageRefLocked(ctx, g, pageIdx)
				if err != nil {
					return prevGen, nil, err
				}

				slice := data[pageOff:]
				if len(slice) > remaining {
					slice = slice[:remaining]
				}
				g.Register(slice)
				slices = append(slices, slice)
				n := len(slice)
				readOffset += uint64(n)
				remaining -= n
			}
			return prevGen, slices, nil
		}()
		if err == nil || !ly.readOnlyFollower || !ly.shouldRetryAfterLayoutError(err) {
			return slices, err
		}
		changed, refreshErr := ly.waitRefreshForLayoutChange(ctx, prevGen)
		if refreshErr != nil {
			return nil, refreshErr
		}
		if !changed {
			return slices, err
		}
	}
}

// readPageRef reads a single page using zero-copy references where possible.
// Dirty pages are returned as detached copies; only stable cache/L1/L2 data
// uses borrowed zero-copy references that depend on the safepoint guard.
func (ly *layer) readPageRefLocked(ctx context.Context, g safepoint.Guard, pageIdx PageIdx) ([]byte, error) {
	if ly.active != nil {
		var page Page
		if ok, tombstone := ly.active.copyPage(pageIdx, &page); ok {
			metrics.PageReadDirtyPages.Inc()
			if tombstone {
				return zeroPage[:], nil
			}
			return append([]byte(nil), page[:]...), nil
		}
	}

	if ly.pending != nil {
		var page Page
		if ok, tombstone := ly.pending.copyPage(pageIdx, &page); ok {
			metrics.PageReadPendingDirtyBatch.Inc()
			if tombstone {
				return zeroPage[:], nil
			}
			return append([]byte(nil), page[:]...), nil
		}
	}

	if layer, seq := ly.l1Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPageRef(g, layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL1.Inc()
			return data, nil
		}
	}

	if layer, seq := ly.l2Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPageRef(g, layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL2.Inc()
			return data, nil
		}
	}

	metrics.PageReadZero.Inc()
	return zeroPage[:], nil
}

// cachedPageRef returns a zero-copy reference into the page cache arena.
// The caller must hold the safepoint read lock.
func (ly *layer) cachedPageRef(g safepoint.Guard, sourceLayerID string, pageIdx PageIdx) []byte {
	if !ly.shouldUsePersistentPageCache(sourceLayerID) {
		return nil
	}
	if data := ly.diskCache.GetPageRef(g, sourceLayerID, uint64(pageIdx)); data != nil {
		metrics.CacheHits.Inc()
		metrics.PageReadCache.Inc()
		return data
	}
	metrics.CacheMisses.Inc()
	return nil
}

func (ly *layer) readPageLocked(ctx context.Context, pageIdx PageIdx) ([]byte, error) {
	if ly.active != nil {
		var page Page
		if ok, tombstone := ly.active.copyPage(pageIdx, &page); ok {
			metrics.PageReadDirtyPages.Inc()
			if tombstone {
				return zeroPage[:], nil
			}
			return page[:], nil
		}
	}

	if ly.pending != nil {
		var page Page
		if ok, tombstone := ly.pending.copyPage(pageIdx, &page); ok {
			metrics.PageReadPendingDirtyBatch.Inc()
			if tombstone {
				return zeroPage[:], nil
			}
			return page[:], nil
		}
	}

	if layer, seq := ly.l1Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL1.Inc()
			return data, nil
		}
	}

	if layer, seq := ly.l2Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			return data, nil
		}
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			return nil, err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL2.Inc()
			return data, nil
		}
	}

	metrics.PageReadZero.Inc()
	return zeroPage[:], nil
}

func (ly *layer) shouldUsePersistentPageCache(sourceLayerID string) bool {
	if ly.diskCache == nil {
		return false
	}
	// The persistent page cache key is (sourceLayerID, pageIdx). Pages from the
	// current writable layer are therefore unstable even after a flush: later
	// rewrites in the same layer would reuse the same cache key and could return
	// stale data. Only pages from other layers, or from this layer when it is
	// not writable (for example a read-only follower), are safe to cache.
	return sourceLayerID != ly.id || ly.writeLeaseSeq == 0
}

func (ly *layer) cachedPage(sourceLayerID string, pageIdx PageIdx) []byte {
	if !ly.shouldUsePersistentPageCache(sourceLayerID) {
		return nil
	}
	if data := ly.diskCache.GetPage(sourceLayerID, uint64(pageIdx)); data != nil {
		metrics.CacheHits.Inc()
		metrics.PageReadCache.Inc()
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
	allowDirectL2 := true
	for written < len(data) {
		remaining := len(data) - written

		// Fast path: if this is a full block-aligned write on a fresh layer,
		// write directly to L2 (skipping dirty pages → L1 → L2 pipeline).
		if allowDirectL2 && remaining >= BlockSize && offset%BlockSize == 0 && ly.canDirectL2() {
			if err := ly.writeBlockDirectL2(data[written:written+BlockSize], offset); err != nil {
				if !errors.Is(err, errDirectL2Unavailable) {
					return err
				}
			} else {
				written += BlockSize
				offset += BlockSize
				continue
			}
		}
		allowDirectL2 = false

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
// (no L1 ranges, empty dirty pages).
func (ly *layer) canDirectL2() bool {
	ly.mu.RLock()
	defer ly.mu.RUnlock()
	return ly.canDirectL2Locked()
}

func (ly *layer) canDirectL2Locked() bool {
	ok := ly.index.DirectL2Eligible &&
		ly.l1Map.Len() == 0 &&
		ly.pending == nil &&
		(ly.active == nil || ly.active.isEmpty())
	if !ok && ly.directL2DebugOnce.CompareAndSwap(false, true) {
		slog.Warn("canDirectL2 first failure",
			"layer", ly.id,
			"eligible", ly.index.DirectL2Eligible,
			"l1_len", ly.l1Map.Len(),
			"has_pending", ly.pending != nil,
			"active_nil", ly.active == nil,
			"active_empty", ly.active == nil || ly.active.isEmpty(),
		)
	}
	return ok
}

func (ly *layer) disableDirectL2() {
	ly.mu.Lock()
	ly.disableDirectL2Locked()
	ly.mu.Unlock()
}

func (ly *layer) disableDirectL2Locked() {
	if ly.index.DirectL2Eligible {
		slog.Warn("disabling DirectL2Eligible",
			"layer", ly.id,
			"caller", callerName(2),
		)
	}
	ly.index.DirectL2Eligible = false
}

func callerName(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}
	return runtime.FuncForPC(pc).Name()
}

// writeBlockDirectL2 writes a full BlockSize chunk directly as an L2 block,
// bypassing the dirty pages/flush pipeline. The caller must ensure
// offset is block-aligned and len(data) == BlockSize.
func (ly *layer) writeBlockDirectL2(data []byte, offset uint64) error {
	blockIdx := BlockIdx(offset / BlockSize)

	// Skip all-zero blocks. This is safe because writeBlockDirectL2 is only
	// reachable when DirectL2Eligible is true, which means this is a fresh
	// layer with no parent — unwritten regions read back as zeros.
	if isZeroBlock(data) {
		return nil
	}

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

	ctx := context.Background()
	for attempt := 0; ; attempt++ {
		ly.mu.Lock()
		if !ly.canDirectL2Locked() {
			slog.Warn("direct L2 unavailable",
				"block", blockIdx,
				"eligible", ly.index.DirectL2Eligible,
				"l1_len", ly.l1Map.Len(),
				"has_pending", ly.pending != nil,
				"active_empty", ly.active == nil || ly.active.isEmpty(),
			)
			ly.mu.Unlock()
			return errDirectL2Unavailable
		}

		existingLayer, existingSeq := ly.l2Map.Find(blockIdx)
		outputSeq := ly.outputSeq(existingLayer, existingSeq, ly.writeLeaseSeq)
		key := blockKey(ly.id, "l2", outputSeq, blockIdx)
		if err := ly.store.PutReader(ctx, key, bytes.NewReader(blob)); err != nil {
			ly.mu.Unlock()
			ly.sleepTransientRetry("upload direct l2 block", attempt, err)
			continue
		}

		ly.l2Map = ly.l2Map.Set(blockIdx, ly.id, outputSeq)
		ly.index.LayoutGen++

		for saveAttempt := 0; ; saveAttempt++ {
			if err := ly.saveIndexLocked(ctx); err == nil {
				ly.mu.Unlock()
				return nil
			} else {
				ly.mu.Unlock()
				ly.sleepTransientRetry("save index after direct l2 block", saveAttempt, err)
				ly.mu.Lock()
				if !ly.canDirectL2Locked() {
					slog.Warn("direct L2 unavailable after index save retry",
						"block", blockIdx,
						"eligible", ly.index.DirectL2Eligible,
						"l1_len", ly.l1Map.Len(),
						"has_pending", ly.pending != nil,
						"active_empty", ly.active == nil || ly.active.isEmpty(),
					)
					ly.mu.Unlock()
					return errDirectL2Unavailable
				}
			}
		}
	}
}

func (ly *layer) writePage(pageIdx PageIdx, pageOff uint64, chunk []byte) error {
	ctx := context.Background()
	pageLock := ly.pageLock(pageIdx)
	pageLock.Lock()
	defer pageLock.Unlock()

	page := dirtyPagePool.Get().(*Page)

	// If partial page write, read-modify-write.
	if uint64(len(chunk)) < PageSize {
		ly.mu.RLock()
		err := ly.readPageForWriteLocked(ctx, pageIdx, page)
		ly.mu.RUnlock()
		if err != nil {
			dirtyPagePool.Put(page)
			return err
		}
	} else {
		// Full-page write: zero the page since pool pages contain stale data.
		*page = Page{}
	}

	copy(page[pageOff:], chunk)

	for {
		ly.mu.Lock()
		ly.disableDirectL2Locked()
		active := ly.active
		retired, err := active.stagePageDirect(pageIdx, page)
		if traceLayerEnabled(ly.id) && tracePageEnabled(pageIdx) {
			slog.Info("trace write page",
				"layer", ly.id,
				"page", pageIdx,
				"page_offset", pageOff,
				"chunk_len", len(chunk),
				"dirty_batch_err", err,
			)
		}
		switch {
		case err == nil:
			// Ownership of page transferred to dirty batch.
			shouldDrain := ly.noteWriteLocked()
			writeNotify := ly.writeNotify
			ly.mu.Unlock()
			notifyWorkerChan(writeNotify)
			ly.enqueueRetiredDirtyPages(retired)
			metrics.DirtyPageBytes.Set(float64(ly.currentDirtyPageBytes()))
			if shouldDrain {
				ly.requestDrain()
			}
			return nil
		case errors.Is(err, errDirtyBatchClosed):
			ly.mu.Unlock()
			continue
		case errors.Is(err, errDirtyBatchFull):
			ly.waitForWriteCapacityLocked(active)
			ly.mu.Unlock()
			bpStart := time.Now()
			metrics.BackpressureWaits.Inc()
			metrics.BackpressureWaitDuration.Observe(time.Since(bpStart).Seconds())
			continue
		default:
			ly.mu.Unlock()
			dirtyPagePool.Put(page)
			return err
		}
	}
}

func (ly *layer) readPageForWriteLocked(ctx context.Context, pageIdx PageIdx, dst *Page) error {
	return ly.readPageIntoLocked(ctx, pageIdx, dst)
}

// readPageIntoLocked reads a page into a caller-provided buffer, avoiding
// heap allocations for the common dirty-batch and zero-page paths.
func (ly *layer) readPageIntoLocked(ctx context.Context, pageIdx PageIdx, dst *Page) error {
	if ly.active != nil {
		if ok, tombstone := ly.active.copyPage(pageIdx, dst); ok {
			metrics.PageReadDirtyPages.Inc()
			if tombstone {
				*dst = Page{}
			}
			return nil
		}
	}

	if ly.pending != nil {
		if ok, tombstone := ly.pending.copyPage(pageIdx, dst); ok {
			metrics.PageReadPendingDirtyBatch.Inc()
			if tombstone {
				*dst = Page{}
			}
			return nil
		}
	}

	if layer, seq := ly.l1Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			copy(dst[:], data)
			return nil
		}
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			return err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL1.Inc()
			copy(dst[:], data)
			return nil
		}
	}

	if layer, seq := ly.l2Map.Find(pageIdx.Block()); layer != "" {
		if data := ly.cachedPage(layer, pageIdx); data != nil {
			copy(dst[:], data)
			return nil
		}
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			return err
		}
		if found {
			ly.cachePage(layer, pageIdx, data)
			metrics.PageReadL2.Inc()
			copy(dst[:], data)
			return nil
		}
	}

	metrics.PageReadZero.Inc()
	*dst = Page{}
	return nil
}

// PunchHole zeroes all pages fully or partially covered by the range.
func (ly *layer) PunchHole(offset, length uint64) error {
	ly.disableDirectL2()

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

	// Write tombstones for fully-covered interior pages.
	for pageIdx := firstFullPage; pageIdx < lastFullPage; pageIdx++ {
		pageLock := ly.pageLock(pageIdx)
		pageLock.Lock()
		for {
			ly.mu.Lock()
			active := ly.active
			retired, err := active.stageTombstoneWithRetired(pageIdx)
			switch {
			case err == nil:
				shouldDrain := ly.noteWriteLocked()
				writeNotify := ly.writeNotify
				ly.mu.Unlock()
				notifyWorkerChan(writeNotify)
				ly.enqueueRetiredDirtyPages(retired)
				metrics.DirtyPageBytes.Set(float64(ly.currentDirtyPageBytes()))
				if shouldDrain {
					ly.requestDrain()
				}
				pageLock.Unlock()
				goto nextPage
			case errors.Is(err, errDirtyBatchClosed):
				ly.mu.Unlock()
				continue
			case errors.Is(err, errDirtyBatchFull):
				ly.waitForWriteCapacityLocked(active)
				ly.mu.Unlock()
				continue
			default:
				ly.mu.Unlock()
				pageLock.Unlock()
				return err
			}
		}
	nextPage:
	}

	return nil
}

// Flush flushes all pending data to S3.
func (ly *layer) Flush() error {
	ly.requestDrain()
	ly.mu.Lock()
	for {
		if ly.stopping {
			ly.mu.Unlock()
			return fmt.Errorf("layer stopping")
		}
		if ly.pending == nil && ly.active != nil && !ly.active.isEmpty() {
			if ly.rotateActiveToPendingLocked() {
				ly.drainForced = true
				ly.mu.Unlock()
				ly.requestDrain()
				ly.mu.Lock()
				continue
			}
		}
		if ly.pending == nil && !ly.drainInFlight && (ly.active == nil || ly.active.isEmpty()) {
			ly.mu.Unlock()
			return nil
		}
		if ly.pending != nil && !ly.drainInFlight {
			ly.notifyDrainWorkerLocked()
		}
		ly.dirtyCond.Wait()
	}
}

// directBlockResult holds the outcome of uploading a single rebuilt block.
type directBlockResult struct {
	blockAddr BlockIdx
	key       string
	pb        *parsedBlock
	promote   bool
	seq       uint64
	newPages  []blockPage // retained for cache population
}

// flushDirtyBatchDirectLocked writes a single dirty batch directly to
// L1/L2 blocks. Pages are merged into existing blocks via read-modify-write.
// The caller is responsible for cleanup of mt on success.
var maxFlushWorkers = env.OptionInt("storage.maxflushworkers", 8)

func (ly *layer) flushDirtyBatchDirectLocked(mt *dirtyBatch, maxRetries int) error {
	flushCycleStart := time.Now()
	ly.mu.Lock()
	ly.flushPendingDeletesLocked()
	snapL1Map := ly.l1Map
	snapL2Map := ly.l2Map
	ly.mu.Unlock()

	// Hold the pending batch RLock for the full flush so block assembly may
	// borrow dirty page buffers without copying. Batch teardown returns pooled
	// buffers under Lock, so clearAndRelease will wait until this flush is fully
	// finished with them.
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// Phase 1 — Collect pages from the dirty batch and group by block.
	entries := mt.entriesLocked()
	if len(entries) == 0 {
		return nil
	}

	blockGroups := make(map[BlockIdx][]blockPage, len(entries)/BlockPages+1)
	totalPages := 0
	for _, e := range entries {
		blockAddr := e.pageIdx.Block()
		offset := uint16(uint64(e.pageIdx) % BlockPages)
		if e.record.tombstone {
			blockGroups[blockAddr] = append(blockGroups[blockAddr], blockPage{
				offset: offset,
				data:   nil,
			})
		} else {
			blockGroups[blockAddr] = append(blockGroups[blockAddr], blockPage{
				offset: offset,
				data:   e.record.bytes(),
			})
		}
		totalPages++
	}

	// Phase 2 — Build + upload blocks in parallel.
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

			if traceLayerEnabled(ly.id) && blockAddr == 0 {
				existingOffsets := make([]uint16, 0, len(existing))
				existingTombstones := 0
				for _, e := range existing {
					existingOffsets = append(existingOffsets, e.offset)
					if len(e.compressed) == 0 {
						existingTombstones++
					}
				}
				newOffsetsList := make([]uint16, 0, len(newPages))
				newTombstones := 0
				for _, p := range newPages {
					newOffsetsList = append(newOffsetsList, p.offset)
					if p.data == nil {
						newTombstones++
					}
				}
				slog.Info("trace block rebuild",
					"layer", ly.id,
					"block", blockAddr,
					"promote", promote,
					"existing_l1_layer", existingL1Layer,
					"existing_l1_seq", existingL1Seq,
					"existing_l2_layer", existingL2Layer,
					"existing_l2_seq", existingL2Seq,
					"existing_offsets", existingOffsets,
					"existing_tombstones", existingTombstones,
					"new_offsets", newOffsetsList,
					"new_tombstones", newTombstones,
				)
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
			if traceLayerEnabled(ly.id) && blockAddr == 0 {
				slog.Info("trace block upload target",
					"layer", ly.id,
					"block", blockAddr,
					"promote", promote,
					"output_seq", outputSeq,
					"key", key,
				)
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
			metrics.FlushBlockCompressedSize.Observe(float64(len(blockData)))
			uncompressedPages := len(existing)
			for _, p := range newPages {
				if p.data != nil {
					uncompressedPages++
				}
			}
			metrics.FlushBlockUncompressedSize.Observe(float64(uncompressedPages) * PageSize)
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

	metrics.FlushBlocksPerCycle.Observe(float64(len(results)))

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
	if err := ly.saveIndexLocked(ctx); err != nil {
		ly.l1Map = snapL1Map
		ly.l2Map = snapL2Map
		ly.index.LayoutGen--
		ly.mu.Unlock()
		return fmt.Errorf("save index: %w", err)
	}
	ly.deleteSupersededBlocksLocked(snapL1Map, snapL2Map, results)
	ly.mu.Unlock()

	// Update caches from results. Use the original uncompressed page data
	// for the disk cache instead of decompressing from the just-built block.
	for _, r := range results {
		if r.pb != nil {
			ly.blockCache.put(r.key, r.pb)
		}
		if ly.diskCache != nil {
			for _, p := range r.newPages {
				pageIdx := r.blockAddr.PageIdx(p.offset)
				if p.data == nil {
					ly.cachePage(ly.id, pageIdx, zeroPage[:])
				} else {
					ly.cachePage(ly.id, pageIdx, p.data)
				}
			}
		}
	}

	metrics.FlushPages.Add(float64(totalPages))
	metrics.FlushDuration.Observe(time.Since(flushCycleStart).Seconds())

	slog.Debug("direct flush: cycle complete", "layer", ly.id,
		"pages", totalPages, "blocks", len(results), "dur", time.Since(flushCycleStart))

	return nil
}

// deleteSupersededBlocks removes block blobs that were replaced by a
// successful flush commit. Only blobs owned by this layer are deleted;
// parent-layer blobs are never touched. Deletes are fire-and-forget:
// failures are logged but do not block the caller.
func (ly *layer) deleteSupersededBlocksLocked(
	snapL1Map, snapL2Map *blockRangeMap,
	results []directBlockResult,
) {
	var staleKeys []string
	for _, r := range results {
		// L1 rewrite: old L1 key is garbage if it belonged to this layer.
		if oldLayer, oldSeq := snapL1Map.Find(r.blockAddr); oldLayer == ly.id {
			oldKey := blockKey(ly.id, "l1", oldSeq, r.blockAddr)
			if oldKey != r.key {
				staleKeys = append(staleKeys, oldKey)
			}
		}
		// L2 promotion: old L2 key is also garbage if it belonged to this layer.
		if r.promote {
			if oldLayer, oldSeq := snapL2Map.Find(r.blockAddr); oldLayer == ly.id {
				oldKey := blockKey(ly.id, "l2", oldSeq, r.blockAddr)
				if oldKey != r.key {
					staleKeys = append(staleKeys, oldKey)
				}
			}
		}
	}
	if len(staleKeys) == 0 {
		return
	}

	if traceLayerEnabled(ly.id) {
		slog.Info("trace delete superseded blocks",
			"layer", ly.id,
			"count", len(staleKeys),
			"keys", staleKeys,
		)
	}

	// Defer deletion to the start of the next flush cycle after the new layout
	// has been visible long enough that no read can still need the old key.
	ly.pendingDeletes = append(ly.pendingDeletes, staleKeys...)
}

func (ly *layer) flushPendingDeletesLocked() {
	keys := ly.pendingDeletes
	ly.pendingDeletes = nil
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		ly.blockCache.delete(key)
	}
	go func() {
		if err := ly.store.DeleteObjects(context.Background(), keys); err != nil {
			slog.Warn("delete superseded blocks", "layer", ly.id, "count", len(keys), "error", err)
		} else {
			metrics.FlushSupersededDeletes.Add(float64(len(keys)))
		}
	}()
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

func (ly *layer) noteWriteLocked() bool {
	if ly.active != nil && ly.active.bytes() >= ly.config.FlushThreshold {
		ly.drainForced = true
		if ly.pending == nil {
			ly.rotateActiveToPendingLocked()
		}
		return true
	}
	return false
}

func (ly *layer) sleepTransientRetry(op string, attempt int, err error) bool {
	delay := 100 * time.Millisecond
	for i := 0; i < attempt && delay < 5*time.Second; i++ {
		delay *= 2
	}
	slog.Warn("storage retrying", "layer", ly.id, "op", op, "attempt", attempt+1, "delay", delay, "error", err)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	ly.mu.RLock()
	flushStop := ly.flushStop
	retryStop := ly.retryStop
	ly.mu.RUnlock()

	if flushStop != nil || retryStop != nil {
		select {
		case <-timer.C:
			return true
		case <-flushStop:
			return false
		case <-retryStop:
			return false
		}
	}

	<-timer.C
	return true
}

// Close shuts down the layer's background goroutines and cleans up resources.
func (ly *layer) Close() {
	ly.closeOnce.Do(func() {
		ly.beginShutdown()

		ly.mu.Lock()
		active := ly.active
		pending := ly.pending
		ly.active = nil
		ly.pending = nil
		ly.mu.Unlock()

		if ly.flushCancel != nil {
			ly.flushCancel()
		}
		if ly.flushStop != nil {
			close(ly.flushStop)
			<-ly.flushDone
		}
		if active != nil {
			ly.enqueueRetiredDirtyBatch(active)
		}
		if pending != nil {
			ly.enqueueRetiredDirtyBatch(pending)
		}
	})
}

// LayerDebugInfo holds layer structure details for the debug endpoint.
type LayerDebugInfo struct {
	LayerID             string `json:"layer_id"`
	L1Ranges            int    `json:"l1_ranges"`
	L2Ranges            int    `json:"l2_ranges"`
	DirtyPages          int    `json:"dirty_pages"`
	DirtyPageSlots      int    `json:"dirty_page_slots"`
	PendingDirtyBatches int    `json:"pending_dirty_batches"`
	BlockCacheEnts      int    `json:"block_cache_entries"`
}

func (ly *layer) debugInfo() LayerDebugInfo {
	ly.mu.RLock()
	l1Ranges := len(ly.l1Map.Ranges())
	l2Ranges := len(ly.l2Map.Ranges())
	mtPages, mtMax := 0, 0
	if ly.active != nil {
		mtPages = ly.active.pages()
		mtMax = ly.active.maxEntries
	}
	frozenCount := 0
	if ly.pending != nil {
		frozenCount = 1
	}
	ly.mu.RUnlock()

	ly.blockCache.mu.RLock()
	blockCached := len(ly.blockCache.entries)
	ly.blockCache.mu.RUnlock()

	return LayerDebugInfo{
		LayerID:             ly.id,
		L1Ranges:            l1Ranges,
		L2Ranges:            l2Ranges,
		DirtyPages:          mtPages,
		DirtyPageSlots:      mtMax,
		PendingDirtyBatches: frozenCount,
		BlockCacheEnts:      blockCached,
	}
}

// flushWriteDelay is how long to wait after a write-triggered flush
// before actually flushing, to batch nearby writes.
const flushWriteDelay = 2 * time.Second

// startPeriodicFlush starts the background drain worker. The worker always
// exists once started; FlushInterval only controls whether it proactively
// rotates a stale active batch when writes have gone quiet.
func (ly *layer) startPeriodicFlush(parentCtx context.Context) {
	if ly.flushStop != nil {
		return
	}

	ctx, cancel := context.WithCancel(parentCtx)
	ly.flushCancel = cancel
	ly.mu.Lock()
	ly.flushStop = make(chan struct{})
	ly.retryStop = make(chan struct{})
	ly.flushDone = make(chan struct{})
	ly.flushNotify = make(chan struct{}, 1)
	ly.writeNotify = make(chan struct{}, 1)
	ly.lastFlushAt.Store(time.Now().UnixMilli())
	ly.mu.Unlock()

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
	ly.mu.Lock()
	ly.flushStop = nil
	ly.retryStop = nil
	ly.flushDone = nil
	ly.flushNotify = nil
	ly.writeNotify = nil
	ly.mu.Unlock()
}

func (ly *layer) periodicFlushLoop(ctx context.Context) {
	defer close(ly.flushDone)
	var (
		timer  *time.Timer
		timerC <-chan time.Time
	)
	if ly.config.FlushInterval > 0 {
		timer = time.NewTimer(ly.config.FlushInterval)
		timerC = timer.C
		defer timer.Stop()
	}

	for {
		forceRotate := false
		select {
		case <-ly.flushStop:
			return
		case <-ctx.Done():
			return
		case <-timerC:
			forceRotate = true
		case <-ly.flushNotify:
			forceRotate = true
		case <-ly.writeNotify:
			if ly.config.FlushInterval < 0 {
				continue
			}
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
			forceRotate = true
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

		ly.doPeriodicFlush(forceRotate)
		if timer != nil {
			timer.Reset(ly.config.FlushInterval)
		}
	}
}

func (ly *layer) doPeriodicFlush(forceRotate bool) {
	for {
		var pending *dirtyBatch

		ly.mu.Lock()
		if ly.pending == nil && ly.active != nil && !ly.active.isEmpty() && (forceRotate || ly.drainForced) {
			if ly.rotateActiveToPendingLocked() {
				ly.drainForced = false
			}
		}
		if ly.pending != nil && !ly.drainInFlight {
			pending = ly.pending
			ly.drainInFlight = true
		}
		ly.mu.Unlock()

		if pending == nil {
			return
		}

		for attempt := 0; ; attempt++ {
			if err := ly.flushDirtyBatchDirectLocked(pending, 5); err != nil {
				ly.mu.Lock()
				stopping := ly.stopping
				ly.mu.Unlock()
				if stopping || !ly.sleepTransientRetry("flush pending batch", attempt, err) {
					ly.mu.Lock()
					ly.drainInFlight = false
					ly.dirtyCond.Broadcast()
					ly.mu.Unlock()
					return
				}
				continue
			}
			break
		}

		ly.mu.Lock()
		if ly.pending == pending {
			ly.pending = nil
			metrics.FrozenTableCount.Set(0)
		}
		ly.drainInFlight = false
		ly.lastFlushAt.Store(time.Now().UnixMilli())
		shouldContinue := ly.pending == nil && ly.active != nil && !ly.active.isEmpty() && (forceRotate || ly.drainForced)
		ly.dirtyCond.Broadcast()
		ly.mu.Unlock()
		ly.enqueueRetiredDirtyBatch(pending)

		if !shouldContinue {
			return
		}
	}
}

// blockKey returns the S3 key for a block blob, incorporating the write lease seq.
func blockKey(layerID, level string, writeLeaseSeq uint64, blockAddr BlockIdx) string {
	return fmt.Sprintf("layers/%s/%s/%016x-%016x", layerID, level, writeLeaseSeq, blockAddr)
}

func (ly *layer) shouldRetryAfterLayoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, blob.ErrNotFound) {
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
		idx, _, err := blob.ReadJSON[layerIndex](ctx, ly.layerStore, "index.json")
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
	// Never overwrite an already-published blob for the current layer.
	// Failed flush attempts may upload some blocks before aborting; if those
	// uploads reuse the live key, readers can observe partial state even though
	// the index was never committed. Rewrites therefore get a fresh versioned
	// key, and the index swap publishes the new blob atomically.
	if existingLayer == ly.id {
		return ly.nextSeq.Add(1)
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
