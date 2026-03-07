package lsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/diskcache"
	"github.com/semistrict/loophole/metrics"
)

// maxMemLayerSlots caps the number of unique page slots in a memlayer.
// This prevents absurd mmap file sizes when FlushThreshold is set very high.
const maxMemLayerSlots = 65536

// timelineMeta is the S3-persisted metadata for a timeline.
type timelineMeta struct {
	Ancestor     string        `json:"ancestor,omitempty"`
	AncestorSeq  uint64        `json:"ancestor_seq,omitempty"`
	BranchPoints []BranchPoint `json:"branch_points,omitempty"`
	LeaseToken   string        `json:"lease_token,omitempty"`
	CreatedAt    string        `json:"created_at"`
}

// BranchPoint records when a child timeline branched from this one.
type BranchPoint struct {
	Child string `json:"child"`
	Seq   uint64 `json:"seq"`
}

// timeline is the storage backing a single volume — an ordered set of
// layers plus an optional ancestor pointer.
type timeline struct {
	id            string
	store         loophole.ObjectStore // rooted at timelines/<id>/
	timelinesRoot loophole.ObjectStore // rooted at timelines/
	cacheDir      string
	config        Config
	diskCache     *diskcache.DiskCache
	fs            LocalFS

	ancestor    *timeline // nil for root timelines
	ancestorSeq uint64    // read from ancestor only at seq <= this value

	mu       sync.RWMutex
	layers   LayerMap
	memLayer *MemLayer

	nextSeq atomic.Uint64

	// Layer caches: parsed layers keyed by S3 key.
	deltaCache  layerCache[parsedDeltaLayer]
	imageCache  layerCache[parsedImageLayer]
	layerFlight singleflight.Group // deduplicates concurrent layer downloads

	frozenMu     sync.RWMutex
	frozenLayers []*MemLayer
	flushMu      sync.Mutex // serializes concurrent calls to flushFrozenLayers
	compactMu    sync.Mutex // serializes concurrent calls to Compact

	// pageLocks serializes read-modify-write cycles for partial page writes.
	// Without this, concurrent sub-page writes to the same 64KB page race:
	// both read the old page, apply their chunk, and the last writer wins,
	// silently dropping the other's data. This is the root cause of NBD mode
	// returning all-zeros (the kernel sends 4KB writes concurrently).
	pageLocks [256]sync.Mutex

	// Periodic flush goroutine.
	flushStop   chan struct{}
	flushDone   chan struct{}
	flushNotify chan struct{}
	writeNotify chan struct{}      // poked on every write; triggers early flush if stale
	flushCancel context.CancelFunc // cancels the flush loop's context
	lastFlushAt atomic.Int64       // UnixMilli of last successful flush
	closeOnce   sync.Once

	// Debug logging for compact/flush tracing. Set in tests.
	debugLog func(string)
}

// openTimeline loads a timeline from S3 and initializes its local state.
func (m *Manager) openTimeline(ctx context.Context, store loophole.ObjectStore, id string, readOnly bool) (*timeline, timelineMeta, string, error) {
	meta, metaEtag, err := loophole.ReadJSON[timelineMeta](ctx, store, "meta.json")
	if err != nil {
		return nil, timelineMeta{}, "", fmt.Errorf("read timeline meta: %w", err)
	}

	tl := &timeline{
		id:            id,
		store:         store,
		timelinesRoot: m.timelines,
		cacheDir:      filepath.Join(m.cacheDir, "timelines", id),
		config:        m.config,
		diskCache:     m.diskCache,
		fs:            m.fs,
		deltaCache:    newLayerCache[parsedDeltaLayer](m.config.MaxLayerCacheEntries),
		imageCache:    newLayerCache[parsedImageLayer](m.config.MaxLayerCacheEntries),
	}

	// Load ancestor chain. Ancestors are always read-only and cached
	// on the Manager so that siblings/cousins share one instance.
	// We only cache ancestors that aren't actively used by a writable
	// volume, since writable timelines flush new layers that would make
	// a cached copy stale.
	if meta.Ancestor != "" {
		tl.ancestorSeq = meta.AncestorSeq
		m.mu.Lock()
		cached, ok := m.timelineCache[meta.Ancestor]
		// Check if any open volume is still writing to this timeline.
		writable := false
		if !ok {
			for _, v := range m.volumes {
				if v.timeline.id == meta.Ancestor {
					writable = true
					break
				}
			}
		}
		m.mu.Unlock()
		if ok {
			tl.ancestor = cached
		} else {
			ancestorStore := m.timelines.At(meta.Ancestor)
			ancestor, _, _, err := m.openTimeline(ctx, ancestorStore, meta.Ancestor, true)
			if err != nil {
				return nil, timelineMeta{}, "", fmt.Errorf("open ancestor %s: %w", meta.Ancestor, err)
			}
			if !writable {
				m.mu.Lock()
				if existing, ok := m.timelineCache[meta.Ancestor]; ok {
					ancestor = existing // another goroutine won the race
				} else {
					m.timelineCache[meta.Ancestor] = ancestor
				}
				m.mu.Unlock()
			}
			tl.ancestor = ancestor
		}
	}

	// Reconstruct layer map from S3.
	if err := tl.loadLayerMap(ctx); err != nil {
		return nil, timelineMeta{}, "", fmt.Errorf("load layer map: %w", err)
	}

	if readOnly {
		// Read-only timelines don't need a mmap-backed memLayer.
		tl.memLayer = &MemLayer{index: make(map[uint64]memEntry)}
	} else {
		memDir := filepath.Join(tl.cacheDir, "mem")
		if err := ensureMemDir(memDir); err != nil {
			return nil, timelineMeta{}, "", fmt.Errorf("create mem dir: %w", err)
		}
		tl.memLayer, err = newMemLayer(memDir, tl.nextSeq.Load(), tl.config.maxMemLayerPages())
		if err != nil {
			return nil, timelineMeta{}, "", fmt.Errorf("create memlayer: %w", err)
		}
	}

	return tl, meta, metaEtag, nil
}

func (tl *timeline) pageCacheKey(pageAddr uint64) diskcache.CacheKey {
	return diskcache.CacheKey{Timeline: tl.id, PageAddr: pageAddr}
}

func (tl *timeline) blobCacheKey(key string) string {
	return tl.id + ":" + key
}

// Read reads data from the timeline's layers into buf at the given byte offset.
func (tl *timeline) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	// Snapshot layer state once for the entire read, avoiding repeated lock
	// acquisitions for each page (a 1MB read touches 256 × 4KB pages).
	snap := tl.snapshotLayers()

	total := 0
	for total < len(buf) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		pageAddr := (offset + uint64(total)) / PageSize
		pageOff := (offset + uint64(total)) % PageSize
		chunk := min(uint64(len(buf)-total), PageSize-pageOff)

		page, err := tl.readPageWith(ctx, &snap, pageAddr)
		if err != nil {
			return total, err
		}
		copy(buf[total:], page[pageOff:pageOff+chunk])
		total += int(chunk)
	}
	return total, nil
}

// layerSnapshot holds an immutable view of the timeline's layer state,
// captured under the lock once and reused across multiple page reads.
type layerSnapshot struct {
	memLayer    *MemLayer
	frozen      []*MemLayer
	deltas      []DeltaLayerMeta
	images      []ImageLayerMeta
	ancestor    *timeline
	ancestorSeq uint64
}

// snapshotLayers captures the current layer state under the appropriate locks.
func (tl *timeline) snapshotLayers() layerSnapshot {
	tl.mu.RLock()
	snap := layerSnapshot{
		memLayer:    tl.memLayer,
		deltas:      tl.layers.Deltas,
		images:      tl.layers.Images,
		ancestor:    tl.ancestor,
		ancestorSeq: tl.ancestorSeq,
	}
	tl.mu.RUnlock()

	tl.frozenMu.RLock()
	snap.frozen = make([]*MemLayer, len(tl.frozenLayers))
	copy(snap.frozen, tl.frozenLayers)
	tl.frozenMu.RUnlock()

	return snap
}

// readPageWith reads a single page using a pre-captured layer snapshot.
// This avoids re-acquiring locks and re-snapshotting state for each page
// in a batched read.
func (tl *timeline) readPageWith(ctx context.Context, snap *layerSnapshot, pageAddr uint64) ([]byte, error) {
	const beforeSeq = math.MaxUint64

	// 1. Active memLayer.
	if entry, ok := snap.memLayer.get(pageAddr); ok && entry.seq < beforeSeq {
		if entry.tombstone {
			return zeroPage[:], nil
		}
		return snap.memLayer.readData(entry)
	}

	// 2. Frozen memLayers (newest first).
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if entry, ok := snap.frozen[i].get(pageAddr); ok && entry.seq < beforeSeq {
			if entry.tombstone {
				return zeroPage[:], nil
			}
			data, err := snap.frozen[i].readData(entry)
			if err == nil {
				return data, nil
			}
			break
		}
	}

	// 3. Page cache.
	if tl.diskCache != nil {
		if cached := tl.diskCache.GetPage(tl.pageCacheKey(pageAddr)); cached != nil {
			return cached, nil
		}
	}

	// 4. Delta layers (newest first).
	for i := len(snap.deltas) - 1; i >= 0; i-- {
		dl := &snap.deltas[i]
		if pageAddr < dl.PageRange[0] || pageAddr > dl.PageRange[1] {
			continue
		}
		data, found, err := tl.readFromDeltaLayer(ctx, dl, pageAddr, beforeSeq)
		if err != nil {
			return nil, err
		}
		if found {
			if tl.diskCache != nil {
				tl.diskCache.PutPage(tl.pageCacheKey(pageAddr), data)
			}
			return data, nil
		}
	}

	// 5. Image layers.
	for i := len(snap.images) - 1; i >= 0; i-- {
		il := &snap.images[i]
		if pageAddr < il.PageRange[0] || pageAddr > il.PageRange[1] {
			continue
		}
		data, found, err := tl.readFromImageLayer(ctx, il, pageAddr)
		if err != nil {
			return nil, err
		}
		if found {
			if tl.diskCache != nil {
				tl.diskCache.PutPage(tl.pageCacheKey(pageAddr), data)
			}
			return data, nil
		}
	}

	// 6. Ancestor timeline.
	if snap.ancestor != nil {
		return snap.ancestor.readPage(ctx, pageAddr, snap.ancestorSeq)
	}

	// 7. Page never written — return zeros.
	return zeroPage[:], nil
}

// readPagePinned reads a single page using a pre-captured layer snapshot,
// returning a pinned slice and a release function. For memLayer hits the
// slice points directly into the mmap and the release function unlocks the
// RWMutex. For all other sources (delta/image/cache/ancestor) the data is
// heap-allocated and release is a no-op.
func (tl *timeline) readPagePinned(ctx context.Context, snap *layerSnapshot, pageAddr uint64) ([]byte, func(), error) {
	const beforeSeq = math.MaxUint64

	// 1. Active memLayer.
	if entry, ok := snap.memLayer.get(pageAddr); ok && entry.seq < beforeSeq {
		return snap.memLayer.readDataPinned(entry)
	}

	// 2. Frozen memLayers (newest first).
	for i := len(snap.frozen) - 1; i >= 0; i-- {
		if entry, ok := snap.frozen[i].get(pageAddr); ok && entry.seq < beforeSeq {
			data, release, err := snap.frozen[i].readDataPinned(entry)
			if err == nil {
				return data, release, nil
			}
			break
		}
	}

	// 3. Disk cache (pinned).
	if tl.diskCache != nil {
		if data, release := tl.diskCache.GetPagePinned(tl.pageCacheKey(pageAddr)); data != nil {
			return data, release, nil
		}
	}

	// 4–8. Delta, image, ancestor, zero — all allocate.
	data, err := tl.readPageWith(ctx, snap, pageAddr)
	if err != nil {
		return nil, nil, err
	}
	return data, func() {}, nil
}

// ReadPinned reads n bytes starting at offset, returning a pinned slice and
// release function. For single-page aligned reads that hit the memLayer, the
// returned slice is zero-copy from the mmap. Callers MUST call release when
// done with the data.
func (tl *timeline) ReadPinned(ctx context.Context, offset uint64, n int) ([]byte, func(), error) {
	pageAddr := offset / PageSize
	pageOff := offset % PageSize

	// Fast path: single full page, page-aligned.
	if pageOff == 0 && n == PageSize {
		snap := tl.snapshotLayers()
		return tl.readPagePinned(ctx, &snap, pageAddr)
	}

	// Slow path: spans multiple pages or sub-page — allocate and copy.
	buf := make([]byte, n)
	got, err := tl.Read(ctx, buf, offset)
	if err != nil {
		return nil, nil, err
	}
	return buf[:got], func() {}, nil
}

// Write writes data to the timeline's active memLayer at the given byte offset.
func (tl *timeline) Write(ctx context.Context, data []byte, offset uint64) error {
	written := 0
	for written < len(data) {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageAddr := (offset + uint64(written)) / PageSize
		pageOff := (offset + uint64(written)) % PageSize
		chunk := min(uint64(len(data)-written), PageSize-pageOff)

		if err := tl.writePage(ctx, pageAddr, pageOff, data[written:written+int(chunk)]); err != nil {
			return err
		}

		written += int(chunk)
	}
	return nil
}

// writePage writes a chunk of data into a single page, handling the
// read-modify-write cycle under a per-page lock to prevent lost updates
// from concurrent sub-page writes.
func (tl *timeline) writePage(ctx context.Context, pageAddr, pageOff uint64, chunk []byte) error {
	pageLock := &tl.pageLocks[pageAddr%uint64(len(tl.pageLocks))]

	// Hold the per-page lock only for the read-modify-write + memLayer put.
	// Release it before any flush work to avoid blocking concurrent PunchHole
	// or Write calls to the same page while a slow S3 upload runs.
	pageLock.Lock()

	var page [PageSize]byte

	// If partial page write, read-modify-write.
	if uint64(len(chunk)) < PageSize {
		existing, err := tl.readPage(ctx, pageAddr, math.MaxUint64)
		if err != nil {
			pageLock.Unlock()
			return err
		}
		copy(page[:], existing)
	}

	copy(page[pageOff:], chunk)

	// Write to the memLayer FIRST, before any flush attempts. This
	// guarantees the data is recorded even if a subsequent flush fails.
	// Callers depend on this: a Write error means "data is in the
	// memLayer (or frozen layer) but couldn't be persisted to S3 yet"
	// — a later Flush will persist it.
	seq := tl.nextSeq.Add(1) - 1
	tl.mu.RLock()
	ml := tl.memLayer
	err := ml.put(pageAddr, seq, page[:])
	tl.mu.RUnlock()

	pageLock.Unlock()

	// Backpressure: if the memlayer is full (all slots used), freeze it
	// and flush to make room, then retry the write on the new memlayer.
	for errors.Is(err, ErrMemLayerFull) {
		if err := tl.maybeFreezeAndFlush(ctx); err != nil {
			return err
		}
		tl.mu.RLock()
		ml = tl.memLayer
		err = ml.put(pageAddr, seq, page[:])
		tl.mu.RUnlock()
	}
	if err != nil {
		return err
	}

	// Notify flush loop that a write happened (non-blocking).
	if tl.writeNotify != nil {
		select {
		case tl.writeNotify <- struct{}{}:
		default:
		}
	}

	// Backpressure: if frozen layers are at capacity, flush them now.
	tl.frozenMu.RLock()
	nfrozen := len(tl.frozenLayers)
	tl.frozenMu.RUnlock()
	if nfrozen >= tl.config.MaxFrozenLayers {
		if err := tl.flushFrozenLayers(ctx); err != nil {
			return err
		}
	}

	// Check if memLayer needs freezing (safe: ml pointer captured under lock).
	if ml.size.Load() >= tl.config.FlushThreshold {
		if err := tl.maybeFreezeAndFlush(ctx); err != nil {
			return err
		}
	}

	return nil
}

// PunchHole records tombstones for all pages fully covered by the range.
func (tl *timeline) PunchHole(ctx context.Context, offset, length uint64) error {
	end := offset + length

	// Page boundaries for the fully-covered interior.
	firstFullPage := (offset + PageSize - 1) / PageSize // round up
	lastFullPage := end / PageSize                      // round down (exclusive)

	// Handle partial page at the start: write zeros through tl.Write so the
	// read-modify-write happens under the per-page lock.
	if startOff := offset % PageSize; startOff != 0 {
		pageAddr := offset / PageSize
		clearEnd := uint64(PageSize)
		if end < (pageAddr+1)*PageSize {
			clearEnd = end - pageAddr*PageSize
		}
		zeros := make([]byte, clearEnd-startOff)
		if err := tl.Write(ctx, zeros, offset); err != nil {
			return err
		}
		if end <= (pageAddr+1)*PageSize {
			return nil
		}
	}

	// Handle partial page at the end: write zeros through tl.Write.
	if endOff := end % PageSize; endOff != 0 {
		pageAddr := end / PageSize
		zeros := make([]byte, endOff)
		if err := tl.Write(ctx, zeros, pageAddr*PageSize); err != nil {
			return err
		}
	}

	// Tombstone fully-covered interior pages under the page lock to prevent
	// races with concurrent writePage calls to the same page.
	for pageAddr := firstFullPage; pageAddr < lastFullPage; pageAddr++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageLock := &tl.pageLocks[pageAddr%uint64(len(tl.pageLocks))]
		pageLock.Lock()

		seq := tl.nextSeq.Add(1) - 1
		tl.mu.RLock()
		err := tl.memLayer.putTombstone(pageAddr, seq)
		tl.mu.RUnlock()

		pageLock.Unlock()

		if err != nil {
			return err
		}
		if tl.diskCache != nil {
			tl.diskCache.DeletePage(tl.pageCacheKey(pageAddr))
		}
	}
	return nil
}

// Flush freezes the current memLayer and uploads it as a delta layer to S3.
func (tl *timeline) Flush(ctx context.Context) error {
	if err := tl.freezeMemLayer(); err != nil {
		return err
	}
	return tl.flushFrozenLayers(ctx)
}

// readPage searches layers for the given page. Only entries with seq < beforeSeq
// are visible (exclusive upper bound).
func (tl *timeline) readPage(ctx context.Context, pageAddr, beforeSeq uint64) ([]byte, error) {
	// Snapshot mutable state under the lock, then release before S3 I/O.
	tl.mu.RLock()
	memLayer := tl.memLayer
	deltas := tl.layers.Deltas
	images := tl.layers.Images
	ancestor := tl.ancestor
	ancestorSeq := tl.ancestorSeq
	tl.mu.RUnlock()

	// 1. Active memLayer.
	if entry, ok := memLayer.get(pageAddr); ok && entry.seq < beforeSeq {
		if entry.tombstone {
			return zeroPage[:], nil
		}
		return memLayer.readData(entry)
	}

	// 2. Frozen memLayers (newest first).
	tl.frozenMu.RLock()
	frozen := make([]*MemLayer, len(tl.frozenLayers))
	copy(frozen, tl.frozenLayers)
	tl.frozenMu.RUnlock()

	for i := len(frozen) - 1; i >= 0; i-- {
		if entry, ok := frozen[i].get(pageAddr); ok && entry.seq < beforeSeq {
			if entry.tombstone {
				return zeroPage[:], nil
			}
			data, err := frozen[i].readData(entry)
			if err == nil {
				return data, nil
			}
			// Frozen layer was cleaned up during flush. The data is now
			// in a delta layer, so fall through to the delta layer search.
			break
		}
	}

	// Only use page cache for direct reads (beforeSeq == MaxUint64).
	// Ancestor reads with limited beforeSeq can't use the cache because the
	// cached entry may have a seq higher than what's visible at beforeSeq.
	useCache := tl.diskCache != nil && beforeSeq == math.MaxUint64

	// 3. Page cache (for pages previously read from S3 layers).
	if useCache {
		if cached := tl.diskCache.GetPage(tl.pageCacheKey(pageAddr)); cached != nil {
			return cached, nil
		}
	}

	// 4. Delta layers (newest first). Uses snapshot — no lock held during S3 I/O.
	for i := len(deltas) - 1; i >= 0; i-- {
		dl := &deltas[i]
		if dl.StartSeq >= beforeSeq {
			continue
		}
		if pageAddr < dl.PageRange[0] || pageAddr > dl.PageRange[1] {
			continue
		}
		data, found, err := tl.readFromDeltaLayer(ctx, dl, pageAddr, beforeSeq)
		if err != nil {
			return nil, err
		}
		if found {
			if useCache {
				tl.diskCache.PutPage(tl.pageCacheKey(pageAddr), data)
			}
			return data, nil
		}
	}

	// 5. Image layers.
	for i := len(images) - 1; i >= 0; i-- {
		il := &images[i]
		if il.Seq > beforeSeq {
			continue
		}
		if pageAddr < il.PageRange[0] || pageAddr > il.PageRange[1] {
			continue
		}
		data, found, err := tl.readFromImageLayer(ctx, il, pageAddr)
		if err != nil {
			return nil, err
		}
		if found {
			if useCache {
				tl.diskCache.PutPage(tl.pageCacheKey(pageAddr), data)
			}
			return data, nil
		}
	}

	// 6. Ancestor timeline.
	if ancestor != nil {
		return ancestor.readPage(ctx, pageAddr, ancestorSeq)
	}

	// 7. Page never written — return zeros.
	return zeroPage[:], nil
}

// freezeMemLayer freezes the current memLayer and starts a new one.
func (tl *timeline) freezeMemLayer() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	memDir := filepath.Join(tl.cacheDir, "mem")
	if err := ensureMemDir(memDir); err != nil {
		return fmt.Errorf("ensure mem dir: %w", err)
	}
	ml, err := newMemLayer(memDir, tl.nextSeq.Load(), tl.config.maxMemLayerPages())
	if err != nil {
		return fmt.Errorf("create new memlayer: %w", err)
	}

	old := tl.memLayer
	old.freeze(tl.nextSeq.Load())

	tl.frozenMu.Lock()
	tl.frozenLayers = append(tl.frozenLayers, old)
	tl.frozenMu.Unlock()

	tl.memLayer = ml
	return nil
}

// maybeFreezeAndFlush freezes the memLayer and notifies the background
// flush loop to upload it. The write path never blocks on S3 here;
// backpressure is applied separately when frozen layer count is too high.
// If no flush loop is running (FlushInterval <= 0), falls back to inline flush.
func (tl *timeline) maybeFreezeAndFlush(ctx context.Context) error {
	if err := tl.freezeMemLayer(); err != nil {
		return err
	}
	if tl.flushNotify != nil {
		// Non-blocking signal to the flush loop.
		select {
		case tl.flushNotify <- struct{}{}:
		default:
		}
		return nil
	}
	// No background flush loop — flush inline.
	return tl.flushFrozenLayers(ctx)
}

// flushFrozenLayers uploads all frozen memLayers as delta layers to S3.
// Serialized by flushMu to prevent concurrent flushes from racing.
func (tl *timeline) flushFrozenLayers(ctx context.Context) error {
	tl.flushMu.Lock()
	defer tl.flushMu.Unlock()
	return tl.flushFrozenLayersLocked(ctx, 5)
}

// flushFrozenLayersLocked is the inner loop of flushFrozenLayers.
// Caller must hold flushMu.
func (tl *timeline) flushFrozenLayersLocked(ctx context.Context, maxRetries int) error {
	for {
		tl.frozenMu.RLock()
		if len(tl.frozenLayers) == 0 {
			tl.frozenMu.RUnlock()
			return nil
		}
		ml := tl.frozenLayers[0]
		tl.frozenMu.RUnlock()

		if err := tl.flushMemLayer(ctx, ml, maxRetries); err != nil {
			return err
		}

		tl.frozenMu.Lock()
		tl.frozenLayers = tl.frozenLayers[1:]
		tl.frozenMu.Unlock()

		// Clean up after removal from the list so concurrent readPage
		// callers that snapshot frozenLayers can't see a cleaned-up layer.
		ml.cleanup()
	}
}

// flushMemLayer serializes a frozen memLayer as a delta layer and uploads to S3.
func (tl *timeline) flushMemLayer(ctx context.Context, ml *MemLayer, maxRetries int) error {
	// Build delta layer from memLayer entries.
	entries := ml.entries()
	if len(entries) == 0 {
		return nil
	}

	data, meta, err := buildDeltaLayer(ml, entries)
	if err != nil {
		return fmt.Errorf("build delta layer: %w", err)
	}

	// Upload to S3 with retry on transient errors.
	for attempt := range maxRetries {
		err = tl.store.PutReader(ctx, meta.Key, bytes.NewReader(data))
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("upload delta layer: %w", err)
		}
		if attempt == maxRetries-1 {
			return fmt.Errorf("upload delta layer (after %d attempts): %w", maxRetries, err)
		}
		backoff := time.Duration(1<<attempt) * 100 * time.Millisecond // 100ms, 200ms, 400ms, 800ms, 1.6s
		slog.Warn("retrying delta upload", "key", meta.Key, "attempt", attempt+1, "error", err, "backoff", backoff)
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("upload delta layer: %w", ctx.Err())
		case <-t.C:
		}
	}
	metrics.FlushBytes.Add(float64(len(data)))

	// Pre-cache locally so subsequent reads don't hit S3.
	tl.cacheDelta(meta.Key, data)

	// Update layer map and invalidate page cache entries for all pages in
	// the flushed layer. Both happen under tl.mu.Lock so that readPage
	// (which holds RLock for its entire duration) sees a consistent state:
	// either pre-flush (cache valid, no new delta) or post-flush (cache
	// invalidated, new delta visible).
	tl.mu.Lock()
	tl.layers.Deltas = append(tl.layers.Deltas, meta)
	if tl.diskCache != nil {
		for i := range entries {
			tl.diskCache.DeletePage(tl.pageCacheKey(entries[i].pageAddr))
		}
	}
	tl.mu.Unlock()
	tl.compactLog("FLUSH tl=%s delta=%s seq=[%d,%d) pages=[%d,%d]",
		tl.id[:8], meta.Key, meta.StartSeq, meta.EndSeq, meta.PageRange[0], meta.PageRange[1])

	// Checkpoint layers.json.
	if err := tl.saveLayerMap(ctx); err != nil {
		// Revert: remove the appended delta since S3 still has the old layer map.
		// The delta data exists on S3 as an orphan but that's acceptable.
		tl.mu.Lock()
		tl.layers.Deltas = tl.layers.Deltas[:len(tl.layers.Deltas)-1]
		tl.mu.Unlock()
		tl.compactLog("FLUSH saveLayerMap FAILED (reverted) tl=%s: %v", tl.id[:8], err)
		return fmt.Errorf("save layer map: %w", err)
	}
	tl.compactLog("FLUSH saveLayerMap OK tl=%s total_deltas=%d total_images=%d",
		tl.id[:8], len(tl.layers.Deltas), len(tl.layers.Images))

	return nil
}

// createChild creates a new child timeline branching at branchSeq.
func (tl *timeline) createChild(ctx context.Context, childID string, branchSeq uint64) error {
	childStore := tl.timelinesRoot.At(childID)

	meta := timelineMeta{
		Ancestor:    tl.id,
		AncestorSeq: branchSeq,
		CreatedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := childStore.PutIfNotExists(ctx, "meta.json", data); err != nil {
		return fmt.Errorf("create child meta: %w", err)
	}

	// Write an empty layers.json so that openTimeline's loadLayerMap
	// hits the fast path instead of falling back to ListKeys.
	emptyLayers := layersJSON{NextSeq: branchSeq}
	layersData, err := json.Marshal(emptyLayers)
	if err != nil {
		return err
	}
	if err := childStore.PutIfNotExists(ctx, "layers.json", layersData); err != nil {
		return fmt.Errorf("create child layers.json: %w", err)
	}

	// Update parent's meta.json to record the branch point (CAS to avoid
	// dropping concurrent branch point additions).
	if err := loophole.ModifyJSON[timelineMeta](ctx, tl.store, "meta.json", func(m *timelineMeta) error {
		m.BranchPoints = append(m.BranchPoints, BranchPoint{
			Child: childID,
			Seq:   branchSeq,
		})
		return nil
	}); err != nil {
		return fmt.Errorf("update parent meta: %w", err)
	}

	return nil
}

// loadLayerMap reconstructs the layer map from S3 listing.
func (tl *timeline) loadLayerMap(ctx context.Context) error {
	// Try loading layers.json first (fast path).
	lm, _, err := loophole.ReadJSON[layersJSON](ctx, tl.store, "layers.json")
	if err == nil {
		tl.layers = LayerMap{
			Deltas: lm.Deltas,
			Images: lm.Images,
		}
		tl.nextSeq.Store(lm.NextSeq)
		return nil
	}

	// Only fall back to listing if layers.json is missing.
	// Any other error (corrupt JSON, I/O error) is a hard failure.
	if !errors.Is(err, loophole.ErrNotFound) {
		return fmt.Errorf("load layers.json: %w", err)
	}

	// Fall back to S3 listing: list deltas/* and images/* and parse from key names.
	var maxSeq uint64

	deltaObjects, listErr := tl.store.ListKeys(ctx, "deltas/")
	if listErr != nil {
		// No deltas dir is fine for a fresh timeline.
		tl.layers = LayerMap{}
		return nil
	}

	for _, obj := range deltaObjects {
		dm, parseErr := parseDeltaKey("deltas/"+obj.Key, obj.Size)
		if parseErr != nil {
			continue
		}
		tl.layers.Deltas = append(tl.layers.Deltas, dm)
		if dm.EndSeq > maxSeq {
			maxSeq = dm.EndSeq
		}
	}
	// Sort by StartSeq ascending.
	sort.Slice(tl.layers.Deltas, func(i, j int) bool {
		return tl.layers.Deltas[i].StartSeq < tl.layers.Deltas[j].StartSeq
	})

	// Also list images/ — segments are stored as images/<seq>/<segIdx>.
	imageObjects, listErr := tl.store.ListKeys(ctx, "images/")
	if listErr == nil {
		// Group segments by seq.
		seqSegments := make(map[uint64][]SegmentMeta)
		for _, obj := range imageObjects {
			// obj.Key is relative to "images/", e.g. "0000000000000005/0000000000000000"
			fullKey := "images/" + obj.Key
			parts := strings.SplitN(obj.Key, "/", 2)
			if len(parts) != 2 {
				continue
			}
			seq, err := strconv.ParseUint(parts[0], 16, 64)
			if err != nil {
				continue
			}
			segIdx, err := strconv.ParseUint(parts[1], 16, 64)
			if err != nil {
				continue
			}
			seqSegments[seq] = append(seqSegments[seq], SegmentMeta{
				SegIdx: segIdx,
				Key:    fullKey,
				Size:   obj.Size,
			})
		}
		for seq, segs := range seqSegments {
			sort.Slice(segs, func(i, j int) bool { return segs[i].SegIdx < segs[j].SegIdx })
			tl.layers.Images = append(tl.layers.Images, ImageLayerMeta{
				Seq:       seq,
				PageRange: [2]uint64{0, math.MaxUint64},
				Segments:  segs,
			})
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		sort.Slice(tl.layers.Images, func(i, j int) bool {
			return tl.layers.Images[i].Seq < tl.layers.Images[j].Seq
		})
	}

	tl.nextSeq.Store(maxSeq)

	return nil
}

// Refresh re-reads the layer map from S3 to pick up new layers written by
// another writer. This is used for read-only "follow" mode.
func (tl *timeline) Refresh(ctx context.Context) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.loadLayerMap(ctx)
}

// saveLayerMap writes the current layer map as layers.json.
func (tl *timeline) saveLayerMap(ctx context.Context) error {
	tl.mu.RLock()
	lm := layersJSON{
		NextSeq: tl.nextSeq.Load(),
		Deltas:  tl.layers.Deltas,
		Images:  tl.layers.Images,
	}
	tl.mu.RUnlock()

	data, err := json.Marshal(lm)
	if err != nil {
		return err
	}
	return tl.store.PutReader(ctx, "layers.json", bytes.NewReader(data))
}

// readFromDeltaLayer reads a page from a delta layer on S3.
// Caller must hold tl.mu.RLock.
func (tl *timeline) readFromDeltaLayer(ctx context.Context, dl *DeltaLayerMeta, pageAddr, beforeSeq uint64) ([]byte, bool, error) {
	parsed, err := tl.getDeltaLayer(ctx, dl.Key)
	if err != nil {
		return nil, false, fmt.Errorf("get delta layer %s: %w", dl.Key, err)
	}
	return parsed.findPage(ctx, pageAddr, beforeSeq)
}

// layerCache is a bounded in-memory cache of parsed layers keyed by S3 key.
// Used for both delta and image layer caches.
type layerCache[T any] struct {
	mu    sync.Mutex
	items map[string]*T
	max   int
}

func newLayerCache[T any](max int) layerCache[T] {
	return layerCache[T]{items: make(map[string]*T), max: max}
}

func (lc *layerCache[T]) get(key string) (*T, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	v, ok := lc.items[key]
	return v, ok
}

func (lc *layerCache[T]) put(key string, v *T) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.items[key] = v
	if len(lc.items) > lc.max {
		for k := range lc.items {
			if k != key {
				delete(lc.items, k)
				break
			}
		}
	}
}

func (lc *layerCache[T]) evict(key string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.items, key)
}

// getLayer fetches a parsed layer from cache, local disk, or S3.
// On S3 cache miss, the full blob is downloaded (not just the index)
// because the FUSE read path typically needs many pages from the same
// layer, and one GET is cheaper than N per-page GetRange calls.
// Concurrent requests for the same key are deduplicated.
func getLayer[T any](
	ctx context.Context,
	tl *timeline,
	cache *layerCache[T],
	flightPrefix string,
	key string,
	parseBlob func([]byte) (*T, error),
) (*T, error) {
	if cached, ok := cache.get(key); ok {
		return cached, nil
	}

	v, err, _ := tl.layerFlight.Do(flightPrefix+key, func() (any, error) {
		// Double-check cache after winning the flight.
		if cached, ok := cache.get(key); ok {
			return cached, nil
		}

		// Try shared disk cache before S3.
		if tl.diskCache != nil {
			if data := tl.diskCache.GetBlob(tl.blobCacheKey(key)); data != nil {
				if parsed, err := parseBlob(data); err == nil {
					cache.put(key, parsed)
					return parsed, nil
				}
			}
		}

		// Download full blob from S3.
		data, err := tl.downloadLayer(ctx, key)
		if err != nil {
			return nil, err
		}

		parsed, err := parseBlob(data)
		if err == nil {
			cache.put(key, parsed)
			return parsed, nil
		}

		return nil, err
	})
	if err != nil {
		return nil, err
	}
	return v.(*T), nil
}

// downloadLayer downloads a complete layer blob from S3 and caches it locally.
func (tl *timeline) downloadLayer(ctx context.Context, key string) ([]byte, error) {
	if tl.diskCache != nil {
		if data := tl.diskCache.GetBlob(tl.blobCacheKey(key)); data != nil {
			return data, nil
		}
	}

	body, _, err := tl.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download layer %s: %w", key, err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, fmt.Errorf("read layer %s: %w", key, err)
	}
	tl.cacheBlob(key, data)
	return data, nil
}

func (tl *timeline) cacheBlob(key string, data []byte) {
	if tl.diskCache != nil {
		tl.diskCache.PutBlob(tl.blobCacheKey(key), data)
	}
}

func (tl *timeline) cacheDelta(key string, data []byte) {
	if parsed, err := parseDeltaLayer(data); err == nil {
		tl.deltaCache.put(key, parsed)
	}
	tl.cacheBlob(key, data)
}

func (tl *timeline) getDeltaLayer(ctx context.Context, key string) (*parsedDeltaLayer, error) {
	return getLayer(ctx, tl, &tl.deltaCache, "delta:", key, parseDeltaLayer)
}

func (tl *timeline) getImageLayer(ctx context.Context, key string) (*parsedImageLayer, error) {
	return getLayer(ctx, tl, &tl.imageCache, "image:", key, parseImageLayer)
}

// readFromImageLayer reads a page from the correct segment of an image layer.
func (tl *timeline) readFromImageLayer(ctx context.Context, il *ImageLayerMeta, pageAddr uint64) ([]byte, bool, error) {
	seg := il.FindSegment(pageAddr)
	if seg == nil {
		return nil, false, nil
	}
	parsed, err := tl.getImageLayer(ctx, seg.Key)
	if err != nil {
		return nil, false, fmt.Errorf("get image segment %s: %w", seg.Key, err)
	}
	return parsed.findPage(pageAddr)
}

// cleanupOldLayers deletes replaced layer blobs from S3 (best-effort) and
// evicts their entries from the in-memory index caches.
func (tl *timeline) cleanupOldLayers(ctx context.Context, deltas []DeltaLayerMeta, images []ImageLayerMeta) {
	for _, dl := range deltas {
		_ = tl.store.DeleteObject(ctx, dl.Key)
	}
	for _, il := range images {
		for _, seg := range il.Segments {
			_ = tl.store.DeleteObject(ctx, seg.Key)
		}
	}
	for _, dl := range deltas {
		tl.deltaCache.evict(dl.Key)
	}
	for _, il := range images {
		for _, seg := range il.Segments {
			tl.imageCache.evict(seg.Key)
		}
	}
}

func (tl *timeline) compactLog(format string, args ...any) {
	if tl.debugLog != nil {
		tl.debugLog(fmt.Sprintf(format, args...))
	}
}

// Compact merges delta layers, choosing between a lightweight delta merge
// and a full image promotion based on relative sizes:
//   - If there are fewer than 2 deltas, nothing to do.
//   - If no image exists yet, or total delta size > 50% of image size,
//     do a full image promotion (compactToImage).
//   - Otherwise, merge all compactable deltas into one (compactMergeDeltas).
func (tl *timeline) Compact(ctx context.Context) error {
	tl.compactMu.Lock()
	defer tl.compactMu.Unlock()

	// Flush any pending writes first.
	if err := tl.Flush(ctx); err != nil {
		return fmt.Errorf("flush before compact: %w", err)
	}

	// Read branch points to determine the safe compaction boundary.
	parentMeta, _, err := loophole.ReadJSON[timelineMeta](ctx, tl.store, "meta.json")
	if err != nil {
		return fmt.Errorf("read meta for compact: %w", err)
	}

	tl.mu.RLock()
	deltas := make([]DeltaLayerMeta, len(tl.layers.Deltas))
	copy(deltas, tl.layers.Deltas)
	existingImages := make([]ImageLayerMeta, len(tl.layers.Images))
	copy(existingImages, tl.layers.Images)
	tl.mu.RUnlock()

	if len(deltas) == 0 {
		return nil
	}

	tl.compactLog("START tl=%s deltas=%d images=%d branches=%d",
		tl.id[:8], len(deltas), len(existingImages), len(parentMeta.BranchPoints))
	for i, dl := range deltas {
		tl.compactLog("  delta[%d]: %s seq=[%d,%d) pages=[%d,%d]", i, dl.Key, dl.StartSeq, dl.EndSeq, dl.PageRange[0], dl.PageRange[1])
	}
	for i, il := range existingImages {
		tl.compactLog("  image[%d]: %s seq=%d pages=[%d,%d] segs=%d", i, il.LogKey(), il.Seq, il.PageRange[0], il.PageRange[1], len(il.Segments))
	}

	// Split deltas into compactable vs remaining based on branch points.
	var remainingDeltas []DeltaLayerMeta
	if len(parentMeta.BranchPoints) > 0 {
		minBranch := parentMeta.BranchPoints[0].Seq
		for _, bp := range parentMeta.BranchPoints[1:] {
			if bp.Seq < minBranch {
				minBranch = bp.Seq
			}
		}
		tl.compactLog("  minBranch=%d", minBranch)
		var compactable []DeltaLayerMeta
		for _, dl := range deltas {
			if dl.EndSeq <= minBranch {
				compactable = append(compactable, dl)
			} else {
				remainingDeltas = append(remainingDeltas, dl)
			}
		}
		if len(compactable) == 0 {
			tl.compactLog("  nothing safe to compact")
			return nil
		}
		tl.compactLog("  compactable=%d remaining=%d", len(compactable), len(remainingDeltas))
		deltas = compactable
	}

	// Decide: delta merge vs image promotion.
	var totalDeltaSize int64
	for _, dl := range deltas {
		totalDeltaSize += dl.Size
	}
	var totalImageSize int64
	for _, il := range existingImages {
		totalImageSize += il.TotalSize()
	}

	compactSeq := deltas[len(deltas)-1].EndSeq
	tl.compactLog("  compactSeq=%d totalDeltaSize=%d totalImageSize=%d", compactSeq, totalDeltaSize, totalImageSize)

	if totalImageSize == 0 || totalDeltaSize > totalImageSize/2 {
		tl.compactLog("  -> image promotion (no image or deltas > 50%% of image)")
		return tl.compactToImage(ctx, deltas, existingImages, remainingDeltas, compactSeq)
	}

	if len(deltas) < 2 {
		tl.compactLog("  fewer than 2 compactable deltas, nothing to merge")
		return nil
	}

	tl.compactLog("  -> delta merge")
	return tl.compactMergeDeltas(ctx, deltas, remainingDeltas)
}

// compactMergeDeltas merges multiple delta layers into a single merged delta.
// Cost is O(sum of delta sizes) — does not touch image layers.
func (tl *timeline) compactMergeDeltas(ctx context.Context, deltas []DeltaLayerMeta, remainingDeltas []DeltaLayerMeta) error {
	// Download and parse all delta layers concurrently.
	parsed := make([]*parsedDeltaLayer, len(deltas))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, dl := range deltas {
		g.Go(func() error {
			data, err := tl.downloadDeltaLayer(gctx, dl.Key)
			if err != nil {
				return fmt.Errorf("download delta layer %s: %w", dl.Key, err)
			}
			p, err := parseDeltaLayer(data)
			if err != nil {
				return fmt.Errorf("parse delta layer %s: %w", dl.Key, err)
			}
			parsed[i] = p
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Merge.
	mergedData, mergedMeta, err := mergeDeltaLayers(ctx, parsed, deltas)
	if err != nil {
		return fmt.Errorf("merge delta layers: %w", err)
	}

	// Upload merged delta to S3.
	if err := tl.store.PutReader(ctx, mergedMeta.Key, bytes.NewReader(mergedData)); err != nil {
		return fmt.Errorf("upload merged delta: %w", err)
	}
	tl.compactLog("  merged delta uploaded: %s seq=[%d,%d) pages=[%d,%d]",
		mergedMeta.Key, mergedMeta.StartSeq, mergedMeta.EndSeq, mergedMeta.PageRange[0], mergedMeta.PageRange[1])

	// Pre-cache locally so subsequent reads don't hit S3.
	tl.cacheDelta(mergedMeta.Key, mergedData)

	// Update layer map: replace compacted deltas with merged + remaining.
	newDeltas := []DeltaLayerMeta{mergedMeta}
	newDeltas = append(newDeltas, remainingDeltas...)

	tl.mu.Lock()
	oldDeltas := tl.layers.Deltas
	tl.layers.Deltas = newDeltas
	tl.mu.Unlock()

	if err := tl.saveLayerMap(ctx); err != nil {
		tl.mu.Lock()
		tl.layers.Deltas = oldDeltas
		tl.mu.Unlock()
		tl.compactLog("  saveLayerMap FAILED (reverted): %v", err)
		return fmt.Errorf("save layer map: %w", err)
	}

	// Clean up replaced layers and invalidate page cache.
	tl.cleanupOldLayers(ctx, deltas, nil)
	if tl.diskCache != nil {
		for _, p := range parsed {
			for _, ie := range p.index {
				tl.diskCache.DeletePage(tl.pageCacheKey(ie.PageAddr))
			}
		}
	}

	tl.compactLog("  DONE (delta merge) tl=%s final deltas=%d images=%d",
		tl.id[:8], len(tl.layers.Deltas), len(tl.layers.Images))
	return nil
}

// downloadDeltaLayer downloads a complete delta layer blob from S3 or shared disk cache.
func (tl *timeline) downloadDeltaLayer(ctx context.Context, key string) ([]byte, error) {
	if tl.diskCache != nil {
		if data := tl.diskCache.GetBlob(tl.blobCacheKey(key)); data != nil {
			return data, nil
		}
	}

	// Download from S3.
	body, _, err := tl.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}

	tl.cacheBlob(key, data)
	return data, nil
}

// collectPageAddrs returns all unique page addresses across the given delta and
// image layers, sorted ascending.
func (tl *timeline) collectPageAddrs(ctx context.Context, deltas []DeltaLayerMeta, images []ImageLayerMeta) ([]uint64, error) {
	pageSet := make(map[uint64]struct{})
	for _, dl := range deltas {
		parsed, err := tl.getDeltaLayer(ctx, dl.Key)
		if err != nil {
			return nil, fmt.Errorf("get delta layer %s: %w", dl.Key, err)
		}
		for _, ie := range parsed.index {
			pageSet[ie.PageAddr] = struct{}{}
		}
	}
	for _, il := range images {
		for _, seg := range il.Segments {
			parsed, err := tl.getImageLayer(ctx, seg.Key)
			if err != nil {
				return nil, fmt.Errorf("get image segment %s: %w", seg.Key, err)
			}
			for _, ie := range parsed.index {
				pageSet[ie.PageAddr] = struct{}{}
			}
		}
	}

	addrs := make([]uint64, 0, len(pageSet))
	for addr := range pageSet {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })
	return addrs, nil
}

// compactRemoveAllLayers handles the special case where all pages are zero after
// compaction: removes all compacted deltas and images from the layer map.
func (tl *timeline) compactRemoveAllLayers(ctx context.Context, deltas []DeltaLayerMeta, images []ImageLayerMeta, remainingDeltas []DeltaLayerMeta) error {
	tl.compactLog("  ALL ZERO PATH: removing %d compacted deltas, keeping %d remaining", len(deltas), len(remainingDeltas))
	tl.mu.Lock()
	oldDeltas := tl.layers.Deltas
	oldImages := tl.layers.Images
	tl.layers.Deltas = remainingDeltas
	tl.layers.Images = nil
	tl.mu.Unlock()
	if err := tl.saveLayerMap(ctx); err != nil {
		tl.mu.Lock()
		tl.layers.Deltas = oldDeltas
		tl.layers.Images = oldImages
		tl.mu.Unlock()
		tl.compactLog("  ALL ZERO saveLayerMap FAILED (reverted): %v", err)
		return err
	}
	tl.cleanupOldLayers(ctx, deltas, images)
	tl.compactLog("  ALL ZERO done, final deltas=%d images=%d", len(tl.layers.Deltas), len(tl.layers.Images))
	return nil
}

// compactToImage builds an image layer from all compactable deltas + existing
// images. This is the original full-compaction behavior.
func (tl *timeline) compactToImage(ctx context.Context, deltas []DeltaLayerMeta, existingImages []ImageLayerMeta, remainingDeltas []DeltaLayerMeta, compactSeq uint64) error {
	addrs, err := tl.collectPageAddrs(ctx, deltas, existingImages)
	if err != nil {
		return err
	}
	tl.compactLog("  pageSet=%d pages, addrs=%v", len(addrs), addrs)

	// Read the latest value for each page concurrently (searching layers at compactSeq).
	type addrData struct {
		addr uint64
		data []byte
	}
	results := make([]addrData, len(addrs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, addr := range addrs {
		results[i].addr = addr
		g.Go(func() error {
			data, err := tl.readPage(gctx, addr, compactSeq)
			if err != nil {
				return fmt.Errorf("read page %d: %w", addr, err)
			}
			results[i].data = data
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	var pages []imagePageInput
	for _, r := range results {
		// Skip zero pages only when there's no ancestor. With an ancestor,
		// zero pages may be tombstones that mask ancestor data and must be
		// preserved in the image layer.
		if bytes.Equal(r.data, zeroPage[:]) && tl.ancestor == nil {
			tl.compactLog("  page %d: zero (skipped, root tl)", r.addr)
			continue
		}
		pages = append(pages, imagePageInput{PageAddr: r.addr, Data: r.data})
	}
	tl.compactLog("  non-zero pages=%d", len(pages))

	if len(pages) == 0 {
		return tl.compactRemoveAllLayers(ctx, deltas, existingImages, remainingDeltas)
	}

	// Build and upload image segments.
	segBlobs, imgMeta, err := buildImageSegments(compactSeq, pages)
	if err != nil {
		return fmt.Errorf("build image segments: %w", err)
	}
	for _, sb := range segBlobs {
		key := imageSegmentKey(compactSeq, sb.SegIdx)
		if err := tl.store.PutReader(ctx, key, bytes.NewReader(sb.Data)); err != nil {
			tl.compactLog("  segment upload FAILED %s: %v", key, err)
			return fmt.Errorf("upload image segment %s: %w", key, err)
		}
		tl.compactLog("  segment uploaded: %s", key)
	}
	tl.compactLog("  image uploaded: %s seq=%d pages=[%d,%d] segments=%d",
		imgMeta.LogKey(), imgMeta.Seq, imgMeta.PageRange[0], imgMeta.PageRange[1], len(imgMeta.Segments))

	// Update layer map: replace compacted layers with the new image.
	tl.mu.Lock()
	oldDeltas := tl.layers.Deltas
	oldImages := tl.layers.Images
	tl.layers.Deltas = remainingDeltas
	tl.layers.Images = []ImageLayerMeta{imgMeta}
	tl.mu.Unlock()

	if err := tl.saveLayerMap(ctx); err != nil {
		tl.mu.Lock()
		tl.layers.Deltas = oldDeltas
		tl.layers.Images = oldImages
		tl.mu.Unlock()
		tl.compactLog("  saveLayerMap FAILED (reverted in-memory state): %v", err)
		return fmt.Errorf("save layer map: %w", err)
	}
	tl.compactLog("  saveLayerMap OK, deleting %d old deltas, %d old images", len(deltas), len(existingImages))

	tl.cleanupOldLayers(ctx, deltas, existingImages)

	tl.compactLog("  DONE tl=%s final deltas=%d images=%d", tl.id[:8], len(tl.layers.Deltas), len(tl.layers.Images))
	return nil
}

// DebugPage traces a page read through every layer, reporting what each one
// contains. Unlike readPage it does NOT short-circuit — it shows the full
// picture so you can see exactly why a read returns what it returns.
// Returns the debug trace as a string. Safe to call from tests.
func (tl *timeline) DebugPage(ctx context.Context, pageAddr uint64, beforeSeq uint64) string {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	isZero := func(b []byte) bool {
		for _, v := range b {
			if v != 0 {
				return false
			}
		}
		return true
	}
	hash := func(b []byte) string {
		if len(b) >= 4 {
			return fmt.Sprintf("%02x%02x%02x%02x", b[0], b[1], b[2], b[3])
		}
		return "?"
	}

	var lines []string
	add := func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	add("=== DebugPage tl=%s page=%d beforeSeq=%d ===", tl.id[:8], pageAddr, beforeSeq)
	add("  layers: %d deltas, %d images", len(tl.layers.Deltas), len(tl.layers.Images))
	for i, dl := range tl.layers.Deltas {
		add("    delta[%d]: %s seq=[%d,%d) pages=[%d,%d]", i, dl.Key, dl.StartSeq, dl.EndSeq, dl.PageRange[0], dl.PageRange[1])
	}
	for i, il := range tl.layers.Images {
		add("    image[%d]: %s seq=%d pages=[%d,%d] segs=%d", i, il.LogKey(), il.Seq, il.PageRange[0], il.PageRange[1], len(il.Segments))
	}

	// 1. Active memLayer.
	if entry, ok := tl.memLayer.get(pageAddr); ok {
		if entry.seq < beforeSeq {
			if entry.tombstone {
				add("  [1] memLayer: TOMBSTONE seq=%d (VISIBLE)", entry.seq)
			} else {
				data, err := tl.memLayer.readData(entry)
				if err != nil {
					add("  [1] memLayer: seq=%d err=%v", entry.seq, err)
				} else {
					add("  [1] memLayer: seq=%d zero=%v hash=%s (VISIBLE)", entry.seq, isZero(data), hash(data))
				}
			}
		} else {
			add("  [1] memLayer: seq=%d FILTERED (seq >= beforeSeq=%d)", entry.seq, beforeSeq)
		}
	} else {
		add("  [1] memLayer: not present")
	}

	// 2. Frozen memLayers.
	tl.frozenMu.RLock()
	frozen := make([]*MemLayer, len(tl.frozenLayers))
	copy(frozen, tl.frozenLayers)
	tl.frozenMu.RUnlock()

	for i := len(frozen) - 1; i >= 0; i-- {
		if entry, ok := frozen[i].get(pageAddr); ok {
			if entry.seq < beforeSeq {
				if entry.tombstone {
					add("  [2] frozen[%d]: TOMBSTONE seq=%d (VISIBLE)", i, entry.seq)
				} else {
					data, err := frozen[i].readData(entry)
					if err != nil {
						add("  [2] frozen[%d]: seq=%d err=%v", i, entry.seq, err)
					} else {
						add("  [2] frozen[%d]: seq=%d zero=%v hash=%s (VISIBLE)", i, entry.seq, isZero(data), hash(data))
					}
				}
			} else {
				add("  [2] frozen[%d]: seq=%d FILTERED (seq >= beforeSeq=%d)", i, entry.seq, beforeSeq)
			}
		}
	}

	// 3. Disk cache.
	if tl.diskCache != nil && beforeSeq == math.MaxUint64 {
		if cached := tl.diskCache.GetPage(tl.pageCacheKey(pageAddr)); cached != nil {
			add("  [3] diskCache: zero=%v hash=%s", isZero(cached), hash(cached))
		} else {
			add("  [3] diskCache: miss")
		}
	} else {
		add("  [3] diskCache: skipped (beforeSeq != MaxUint64 or no cache)")
	}

	// 4. Delta layers.
	for i := len(tl.layers.Deltas) - 1; i >= 0; i-- {
		dl := &tl.layers.Deltas[i]
		if dl.StartSeq >= beforeSeq {
			add("  [4] delta[%d] %s: startSeq=%d FILTERED (>= beforeSeq=%d)", i, dl.Key, dl.StartSeq, beforeSeq)
			continue
		}
		if pageAddr < dl.PageRange[0] || pageAddr > dl.PageRange[1] {
			add("  [4] delta[%d] %s: range=[%d,%d] NOT IN RANGE", i, dl.Key, dl.PageRange[0], dl.PageRange[1])
			continue
		}
		data, found, err := tl.readFromDeltaLayer(ctx, dl, pageAddr, beforeSeq)
		if err != nil {
			add("  [4] delta[%d] %s: err=%v", i, dl.Key, err)
		} else if found {
			add("  [4] delta[%d] %s: seq=[%d,%d) FOUND zero=%v hash=%s", i, dl.Key, dl.StartSeq, dl.EndSeq, isZero(data), hash(data))
		} else {
			add("  [4] delta[%d] %s: seq=[%d,%d) page not in layer", i, dl.Key, dl.StartSeq, dl.EndSeq)
		}
	}

	// 5. Image layers.
	for i := len(tl.layers.Images) - 1; i >= 0; i-- {
		il := &tl.layers.Images[i]
		logKey := il.LogKey()
		if il.Seq > beforeSeq {
			add("  [5] image[%d] %s: seq=%d FILTERED (> beforeSeq=%d)", i, logKey, il.Seq, beforeSeq)
			continue
		}
		if pageAddr < il.PageRange[0] || pageAddr > il.PageRange[1] {
			add("  [5] image[%d] %s: range=[%d,%d] NOT IN RANGE", i, logKey, il.PageRange[0], il.PageRange[1])
			continue
		}
		data, found, err := tl.readFromImageLayer(ctx, il, pageAddr)
		if err != nil {
			add("  [5] image[%d] %s: err=%v", i, logKey, err)
		} else if found {
			add("  [5] image[%d] %s: seq=%d FOUND zero=%v hash=%s", i, logKey, il.Seq, isZero(data), hash(data))
		} else {
			add("  [5] image[%d] %s: seq=%d page not in layer", i, logKey, il.Seq)
		}
	}

	// 6. Ancestor.
	if tl.ancestor != nil {
		add("  [6] ancestor: tl=%s ancestorSeq=%d", tl.ancestor.id[:8], tl.ancestorSeq)
		// Recurse into ancestor.
		ancestorDebug := tl.ancestor.DebugPage(ctx, pageAddr, tl.ancestorSeq)
		for _, line := range splitLines(ancestorDebug) {
			add("    %s", line)
		}
	} else {
		add("  [6] ancestor: none (root timeline)")
	}

	result := ""
	for _, l := range lines {
		result += l + "\n"
	}
	return result
}

func splitLines(s string) []string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// acquireLease checks the existing lease token in meta.json and writes ours.
// meta and etag should come from the initial read in openTimeline to avoid
// a redundant re-read.
func (tl *timeline) acquireLease(ctx context.Context, lm *loophole.LeaseManager, meta timelineMeta, etag string) error {
	if err := lm.CheckAvailable(ctx, meta.LeaseToken); err != nil {
		return fmt.Errorf("timeline %s: %w", tl.id, err)
	}
	meta.LeaseToken = lm.Token()
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = tl.store.PutBytesCAS(ctx, "meta.json", data, etag)
	return err
}

// releaseLease clears the lease token from meta.json if it matches ours.
func (tl *timeline) releaseLease(ctx context.Context, token string) error {
	return loophole.ModifyJSON[timelineMeta](ctx, tl.store, "meta.json", func(m *timelineMeta) error {
		if m.LeaseToken == token {
			m.LeaseToken = ""
		}
		return nil
	})
}

func (tl *timeline) startPeriodicFlush() {
	tl.flushStop = make(chan struct{})
	tl.flushDone = make(chan struct{})
	tl.flushNotify = make(chan struct{}, 1)
	tl.writeNotify = make(chan struct{}, 1)
	tl.lastFlushAt.Store(time.Now().UnixMilli())
	ctx, cancel := context.WithCancel(context.Background())
	tl.flushCancel = cancel
	go tl.periodicFlushLoop(ctx)
}

// flushWriteDelay is how long to wait after a write-triggered flush
// before actually flushing, to batch nearby writes.
const flushWriteDelay = 2 * time.Second

func (tl *timeline) periodicFlushLoop(ctx context.Context) {
	defer close(tl.flushDone)
	timer := time.NewTimer(tl.config.FlushInterval)
	defer timer.Stop()
	for {
		select {
		case <-tl.flushStop:
			return
		case <-timer.C:
		case <-tl.flushNotify:
		case <-tl.writeNotify:
			// A write arrived. If the last flush was longer than FlushInterval
			// ago, schedule a flush after a short delay to batch nearby writes.
			sinceFlush := time.Since(time.UnixMilli(tl.lastFlushAt.Load()))
			if sinceFlush < tl.config.FlushInterval {
				continue // not stale, let the regular timer handle it
			}
			// Drain any extra write notifications.
			select {
			case <-tl.writeNotify:
			default:
			}
			// Wait a short delay to batch writes, but still listen for stop.
			delay := time.NewTimer(flushWriteDelay)
			select {
			case <-tl.flushStop:
				delay.Stop()
				return
			case <-delay.C:
			}
		}

		// Drain any pending notifications so we don't loop unnecessarily.
		select {
		case <-tl.flushNotify:
		default:
		}
		select {
		case <-tl.writeNotify:
		default:
		}

		tl.doPeriodicFlush(ctx)
		if ctx.Err() != nil {
			return
		}
		// Wait another interval after the flush completes.
		timer.Reset(tl.config.FlushInterval)
	}
}

func (tl *timeline) doPeriodicFlush(ctx context.Context) {
	// Use TryLock to avoid blocking on flushMu. Under synctest,
	// a goroutine blocked on a mutex is not "idle", so if an inline
	// Flush holds flushMu and waits on a retry timer, synctest
	// cannot advance fake time — deadlock. TryLock sidesteps this:
	// if someone else is flushing, we skip and retry next interval.
	if tl.flushMu.TryLock() {
		err := tl.flushFrozenLayersLocked(ctx, 1)
		tl.flushMu.Unlock()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("periodic flush failed", "timeline", tl.id, "error", err)
		}
	}

	tl.mu.RLock()
	ml := tl.memLayer
	tl.mu.RUnlock()
	if !ml.isEmpty() {
		if err := tl.freezeMemLayer(); err == nil {
			if tl.flushMu.TryLock() {
				err := tl.flushFrozenLayersLocked(ctx, 1)
				tl.flushMu.Unlock()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Warn("periodic flush failed", "timeline", tl.id, "error", err)
				}
			}
		}
	}
	tl.lastFlushAt.Store(time.Now().UnixMilli())
}

func (tl *timeline) close(ctx context.Context) {
	tl.closeOnce.Do(func() {
		// Stop periodic flush and wait for any in-progress flush to finish.
		if tl.flushStop != nil {
			tl.flushCancel() // cancel in-flight S3 ops so the loop exits promptly
			close(tl.flushStop)
			<-tl.flushDone
		}

		// Flush all dirty data to S3 before cleaning up ephemeral files.
		// Read-only timelines (ancestors) have no mmap-backed memLayer,
		// so skip the flush to avoid creating one.
		if tl.memLayer != nil && tl.memLayer.file != nil {
			if err := tl.Flush(ctx); err != nil {
				slog.Warn("flush on close failed", "timeline", tl.id, "error", err)
			}
		}

		tl.mu.Lock()
		if tl.memLayer != nil {
			tl.memLayer.cleanup()
		}
		tl.mu.Unlock()

		tl.frozenMu.Lock()
		for _, ml := range tl.frozenLayers {
			ml.cleanup()
		}
		tl.frozenLayers = nil
		tl.frozenMu.Unlock()
	})
}

// layersJSON is the S3-persisted layer map checkpoint.
type layersJSON struct {
	NextSeq uint64           `json:"next_seq"`
	Deltas  []DeltaLayerMeta `json:"deltas"`
	Images  []ImageLayerMeta `json:"images"`
}
