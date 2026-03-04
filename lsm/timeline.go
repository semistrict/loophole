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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole"
)

// TimelineMeta is the S3-persisted metadata for a timeline.
type TimelineMeta struct {
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

// Timeline is the storage backing a single volume — an ordered set of
// layers plus an optional ancestor pointer.
type Timeline struct {
	id            string
	store         loophole.ObjectStore // rooted at timelines/<id>/
	timelinesRoot loophole.ObjectStore // rooted at timelines/
	cacheDir      string
	config        Config
	pageCache     *PageCache
	fs            LocalFS

	ancestor    *Timeline // nil for root timelines
	ancestorSeq uint64    // read from ancestor only at seq <= this value

	mu       sync.RWMutex
	layers   LayerMap
	memLayer *MemLayer

	nextSeq atomic.Uint64

	// Layer caches: parsed layers keyed by S3 key.
	deltaIndexMu    sync.Mutex
	deltaIndexCache map[string]*parsedDeltaLayer
	imageIndexMu    sync.Mutex
	imageIndexCache map[string]*parsedImageLayer
	layerFlight     singleflight.Group // deduplicates concurrent layer downloads

	frozenMu     sync.Mutex
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
	flushStop chan struct{}
	flushDone chan struct{}

	// Debug logging for compact/flush tracing. Set in tests.
	debugLog func(string)
}

// openTimeline loads a timeline from S3 and initializes its local state.
func (m *Manager) openTimeline(ctx context.Context, store loophole.ObjectStore, id string) (*Timeline, error) {
	meta, _, err := loophole.ReadJSON[TimelineMeta](ctx, store, "meta.json")
	if err != nil {
		return nil, fmt.Errorf("read timeline meta: %w", err)
	}

	tl := &Timeline{
		id:              id,
		store:           store,
		timelinesRoot:   m.timelines,
		cacheDir:        filepath.Join(m.cacheDir, "timelines", id),
		config:          m.config,
		pageCache:       m.pageCache,
		fs:              m.fs,
		deltaIndexCache: make(map[string]*parsedDeltaLayer),
		imageIndexCache: make(map[string]*parsedImageLayer),
	}

	// Load ancestor chain.
	if meta.Ancestor != "" {
		tl.ancestorSeq = meta.AncestorSeq
		ancestorStore := m.timelines.At(meta.Ancestor)
		ancestor, err := m.openTimeline(ctx, ancestorStore, meta.Ancestor)
		if err != nil {
			return nil, fmt.Errorf("open ancestor %s: %w", meta.Ancestor, err)
		}
		tl.ancestor = ancestor
	}

	// Reconstruct layer map from S3.
	if err := tl.loadLayerMap(ctx); err != nil {
		return nil, fmt.Errorf("load layer map: %w", err)
	}

	// Create local dirs. Use os.MkdirAll directly because mmap needs a
	// real OS path, even when tl.fs is a SimLocalFS in tests.
	memDir := filepath.Join(tl.cacheDir, "mem")
	if err := os.MkdirAll(memDir, 0o755); err != nil {
		return nil, fmt.Errorf("create mem dir: %w", err)
	}

	// Start fresh memLayer. Cap at 16384 slots (1GB) to avoid absurd
	// file sizes when FlushThreshold is set very high (e.g., in tests).
	maxPages := int(tl.config.FlushThreshold / PageSize)
	if maxPages < 1 {
		maxPages = 1
	}
	const maxMemLayerSlots = 16384
	if maxPages > maxMemLayerSlots {
		maxPages = maxMemLayerSlots
	}
	tl.memLayer, err = newMemLayer(memDir, tl.nextSeq.Load(), maxPages)
	if err != nil {
		return nil, fmt.Errorf("create memlayer: %w", err)
	}

	return tl, nil
}

// Read reads data from the timeline's layers into buf at the given byte offset.
func (tl *Timeline) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	total := 0
	for total < len(buf) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		pageAddr := (offset + uint64(total)) / PageSize
		pageOff := (offset + uint64(total)) % PageSize
		chunk := min(uint64(len(buf)-total), PageSize-pageOff)

		page, err := tl.readPage(ctx, pageAddr, math.MaxUint64)
		if err != nil {
			return total, err
		}
		copy(buf[total:], page[pageOff:pageOff+chunk])
		total += int(chunk)
	}
	return total, nil
}

// Write writes data to the timeline's active memLayer at the given byte offset.
func (tl *Timeline) Write(ctx context.Context, data []byte, offset uint64) error {
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
func (tl *Timeline) writePage(ctx context.Context, pageAddr, pageOff uint64, chunk []byte) error {
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

	if err != nil {
		return err
	}

	// Backpressure: if frozen layers are at capacity, flush them now.
	tl.frozenMu.Lock()
	nfrozen := len(tl.frozenLayers)
	tl.frozenMu.Unlock()
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
func (tl *Timeline) PunchHole(ctx context.Context, offset, length uint64) error {
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
		if tl.pageCache != nil {
			tl.pageCache.Delete(tl.id, pageAddr)
		}
	}
	return nil
}

// Flush freezes the current memLayer and uploads it as a delta layer to S3.
func (tl *Timeline) Flush(ctx context.Context) error {
	if err := tl.freezeMemLayer(); err != nil {
		return err
	}
	return tl.flushFrozenLayers(ctx)
}

// readPage searches layers for the given page. Only entries with seq < beforeSeq
// are visible (exclusive upper bound).
func (tl *Timeline) readPage(ctx context.Context, pageAddr, beforeSeq uint64) ([]byte, error) {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	// 1. Active memLayer.
	if entry, ok := tl.memLayer.get(pageAddr); ok && entry.seq < beforeSeq {
		if entry.tombstone {
			return zeroPage[:], nil
		}
		return tl.memLayer.readData(entry)
	}

	// 2. Frozen memLayers (newest first).
	tl.frozenMu.Lock()
	frozen := make([]*MemLayer, len(tl.frozenLayers))
	copy(frozen, tl.frozenLayers)
	tl.frozenMu.Unlock()

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
	useCache := tl.pageCache != nil && beforeSeq == math.MaxUint64

	// 3. Page cache (for pages previously read from S3 layers).
	if useCache {
		if cached := tl.pageCache.Get(tl.id, pageAddr); cached != nil {
			return cached, nil
		}
	}

	// 4. Delta layers (newest first).
	for i := len(tl.layers.Deltas) - 1; i >= 0; i-- {
		dl := &tl.layers.Deltas[i]
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
				tl.pageCache.Put(tl.id, pageAddr, data)
			}
			return data, nil
		}
	}

	// 5. Image layers.
	for i := len(tl.layers.Images) - 1; i >= 0; i-- {
		il := &tl.layers.Images[i]
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
				tl.pageCache.Put(tl.id, pageAddr, data)
			}
			return data, nil
		}
	}

	// 6. Ancestor timeline.
	if tl.ancestor != nil {
		return tl.ancestor.readPage(ctx, pageAddr, tl.ancestorSeq)
	}

	// 7. Page never written — return zeros.
	return zeroPage[:], nil
}

// freezeMemLayer freezes the current memLayer and starts a new one.
func (tl *Timeline) freezeMemLayer() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	memDir := filepath.Join(tl.cacheDir, "mem")
	maxPages := int(tl.config.FlushThreshold / PageSize)
	if maxPages < 1 {
		maxPages = 1
	}
	const maxMemLayerSlots = 16384
	if maxPages > maxMemLayerSlots {
		maxPages = maxMemLayerSlots
	}
	ml, err := newMemLayer(memDir, tl.nextSeq.Load(), maxPages)
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

// maybeFreezeAndFlush freezes the memLayer and flushes all frozen layers
// synchronously. Called inline from Write when the memLayer exceeds the
// flush threshold.
func (tl *Timeline) maybeFreezeAndFlush(ctx context.Context) error {
	if err := tl.freezeMemLayer(); err != nil {
		return err
	}
	return tl.flushFrozenLayers(ctx)
}

// flushFrozenLayers uploads all frozen memLayers as delta layers to S3.
// Serialized by flushMu to prevent concurrent flushes from racing.
func (tl *Timeline) flushFrozenLayers(ctx context.Context) error {
	tl.flushMu.Lock()
	defer tl.flushMu.Unlock()

	for {
		tl.frozenMu.Lock()
		if len(tl.frozenLayers) == 0 {
			tl.frozenMu.Unlock()
			return nil
		}
		ml := tl.frozenLayers[0]
		tl.frozenMu.Unlock()

		if err := tl.flushMemLayer(ctx, ml); err != nil {
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
func (tl *Timeline) flushMemLayer(ctx context.Context, ml *MemLayer) error {
	// Build delta layer from memLayer entries.
	entries := ml.entries()
	if len(entries) == 0 {
		return nil
	}

	data, meta, err := buildDeltaLayer(ml, entries)
	if err != nil {
		return fmt.Errorf("build delta layer: %w", err)
	}

	// Upload to S3.
	if err := tl.store.PutReader(ctx, meta.Key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload delta layer: %w", err)
	}

	// Pre-cache the delta layer locally so subsequent reads don't hit S3.
	// Parse first (before taking tl.mu.Lock) to keep the critical section short.
	parsed, parseErr := parseDeltaLayer(data)
	if parseErr == nil {
		tl.cacheDeltaLayer(meta.Key, parsed)
	}
	// Also write to local disk cache so evicted layers can be reloaded without S3.
	localPath := filepath.Join(tl.cacheDir, meta.Key)
	if dir := filepath.Dir(localPath); dir != "" {
		_ = tl.fs.MkdirAll(dir, 0o755)
	}
	_ = tl.fs.WriteFile(localPath, data, 0o644)

	// Update layer map and invalidate page cache entries for all pages in
	// the flushed layer. Both happen under tl.mu.Lock so that readPage
	// (which holds RLock for its entire duration) sees a consistent state:
	// either pre-flush (cache valid, no new delta) or post-flush (cache
	// invalidated, new delta visible).
	tl.mu.Lock()
	tl.layers.Deltas = append(tl.layers.Deltas, meta)
	if tl.pageCache != nil {
		for i := range entries {
			tl.pageCache.Delete(tl.id, entries[i].pageAddr)
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
func (tl *Timeline) createChild(ctx context.Context, childID string, branchSeq uint64) error {
	childStore := tl.timelinesRoot.At(childID)

	meta := TimelineMeta{
		Ancestor:    tl.id,
		AncestorSeq: branchSeq,
		CreatedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if _, err := childStore.PutIfNotExists(ctx, "meta.json", data); err != nil {
		return fmt.Errorf("create child meta: %w", err)
	}

	// Update parent's meta.json to record the branch point (CAS to avoid
	// dropping concurrent branch point additions).
	if err := loophole.ModifyJSON[TimelineMeta](ctx, tl.store, "meta.json", func(m *TimelineMeta) error {
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
func (tl *Timeline) loadLayerMap(ctx context.Context) error {
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

	// Also list images/.
	imageObjects, listErr := tl.store.ListKeys(ctx, "images/")
	if listErr == nil {
		for _, obj := range imageObjects {
			im, parseErr := parseImageKey("images/"+obj.Key, obj.Size)
			if parseErr != nil {
				continue
			}
			tl.layers.Images = append(tl.layers.Images, im)
			if im.Seq > maxSeq {
				maxSeq = im.Seq
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
func (tl *Timeline) Refresh(ctx context.Context) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.loadLayerMap(ctx)
}

// saveLayerMap writes the current layer map as layers.json.
func (tl *Timeline) saveLayerMap(ctx context.Context) error {
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
func (tl *Timeline) readFromDeltaLayer(ctx context.Context, dl *DeltaLayerMeta, pageAddr, beforeSeq uint64) ([]byte, bool, error) {
	parsed, err := tl.getDeltaLayer(ctx, dl.Key)
	if err != nil {
		return nil, false, fmt.Errorf("get delta layer %s: %w", dl.Key, err)
	}
	return parsed.findPage(ctx, pageAddr, beforeSeq)
}

// getDeltaLayer returns a parsed delta layer, downloading and caching it if needed.
// Checks: in-memory cache → local disk cache → S3.
// Concurrent requests for the same key are deduplicated via singleflight.
func (tl *Timeline) getDeltaLayer(ctx context.Context, key string) (*parsedDeltaLayer, error) {
	tl.deltaIndexMu.Lock()
	if cached, ok := tl.deltaIndexCache[key]; ok {
		tl.deltaIndexMu.Unlock()
		return cached, nil
	}
	tl.deltaIndexMu.Unlock()

	v, err, _ := tl.layerFlight.Do("delta:"+key, func() (any, error) {
		// Double-check cache after winning the flight.
		tl.deltaIndexMu.Lock()
		if cached, ok := tl.deltaIndexCache[key]; ok {
			tl.deltaIndexMu.Unlock()
			return cached, nil
		}
		tl.deltaIndexMu.Unlock()

		// Try local disk cache.
		localPath := filepath.Join(tl.cacheDir, key)
		if data, err := tl.fs.ReadFile(localPath); err == nil {
			parsed, err := parseDeltaLayer(data)
			if err == nil {
				tl.cacheDeltaLayer(key, parsed)
				return parsed, nil
			}
			// Corrupted cache file — remove and re-download.
			_ = tl.fs.Remove(localPath)
		}

		// Index-only download from S3 via range reads.
		parsed, err := parseDeltaIndex(ctx, tl.store, key)
		if err != nil {
			return nil, err
		}

		tl.cacheDeltaLayer(key, parsed)
		return parsed, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*parsedDeltaLayer), nil
}

// cacheDeltaLayer adds a parsed layer to the in-memory cache, evicting a
// random entry if the cache exceeds MaxLayerCacheEntries.
func (tl *Timeline) cacheDeltaLayer(key string, parsed *parsedDeltaLayer) {
	tl.deltaIndexMu.Lock()
	defer tl.deltaIndexMu.Unlock()
	tl.deltaIndexCache[key] = parsed
	if len(tl.deltaIndexCache) > tl.config.MaxLayerCacheEntries {
		for k := range tl.deltaIndexCache {
			if k != key {
				delete(tl.deltaIndexCache, k)
				break
			}
		}
	}
}

// readFromImageLayer reads a page from an image layer on S3.
func (tl *Timeline) readFromImageLayer(ctx context.Context, il *ImageLayerMeta, pageAddr uint64) ([]byte, bool, error) {
	parsed, err := tl.getImageLayer(ctx, il.Key)
	if err != nil {
		return nil, false, fmt.Errorf("get image layer %s: %w", il.Key, err)
	}
	return parsed.findPage(ctx, pageAddr)
}

func (tl *Timeline) getImageLayer(ctx context.Context, key string) (*parsedImageLayer, error) {
	tl.imageIndexMu.Lock()
	if cached, ok := tl.imageIndexCache[key]; ok {
		tl.imageIndexMu.Unlock()
		return cached, nil
	}
	tl.imageIndexMu.Unlock()

	v, err, _ := tl.layerFlight.Do("image:"+key, func() (any, error) {
		// Double-check cache after winning the flight.
		tl.imageIndexMu.Lock()
		if cached, ok := tl.imageIndexCache[key]; ok {
			tl.imageIndexMu.Unlock()
			return cached, nil
		}
		tl.imageIndexMu.Unlock()

		// Try local disk cache.
		localPath := filepath.Join(tl.cacheDir, key)
		if data, err := tl.fs.ReadFile(localPath); err == nil {
			parsed, err := parseImageLayer(data)
			if err == nil {
				tl.cacheImageLayer(key, parsed)
				return parsed, nil
			}
			_ = tl.fs.Remove(localPath)
		}

		// Index-only download from S3 via range reads.
		parsed, err := parseImageIndex(ctx, tl.store, key)
		if err != nil {
			return nil, err
		}

		tl.cacheImageLayer(key, parsed)
		return parsed, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*parsedImageLayer), nil
}

// cacheImageLayer adds a parsed layer to the in-memory cache, evicting a
// random entry if the cache exceeds MaxLayerCacheEntries.
func (tl *Timeline) cacheImageLayer(key string, parsed *parsedImageLayer) {
	tl.imageIndexMu.Lock()
	defer tl.imageIndexMu.Unlock()
	tl.imageIndexCache[key] = parsed
	if len(tl.imageIndexCache) > tl.config.MaxLayerCacheEntries {
		for k := range tl.imageIndexCache {
			if k != key {
				delete(tl.imageIndexCache, k)
				break
			}
		}
	}
}

func (tl *Timeline) compactLog(format string, args ...any) {
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
func (tl *Timeline) Compact(ctx context.Context) error {
	tl.compactMu.Lock()
	defer tl.compactMu.Unlock()

	// Flush any pending writes first.
	if err := tl.Flush(ctx); err != nil {
		return fmt.Errorf("flush before compact: %w", err)
	}

	// Read branch points to determine the safe compaction boundary.
	parentMeta, _, err := loophole.ReadJSON[TimelineMeta](ctx, tl.store, "meta.json")
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
		tl.compactLog("  image[%d]: %s seq=%d pages=[%d,%d]", i, il.Key, il.Seq, il.PageRange[0], il.PageRange[1])
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
		totalImageSize += il.Size
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
func (tl *Timeline) compactMergeDeltas(ctx context.Context, deltas []DeltaLayerMeta, remainingDeltas []DeltaLayerMeta) error {
	// Download and parse all delta layers.
	parsed := make([]*parsedDeltaLayer, len(deltas))
	for i, dl := range deltas {
		data, err := tl.downloadDeltaLayer(ctx, dl.Key)
		if err != nil {
			return fmt.Errorf("download delta layer %s: %w", dl.Key, err)
		}
		p, err := parseDeltaLayer(data)
		if err != nil {
			return fmt.Errorf("parse delta layer %s: %w", dl.Key, err)
		}
		parsed[i] = p
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

	// Pre-cache the merged layer.
	mergedParsed, parseErr := parseDeltaLayer(mergedData)
	if parseErr == nil {
		tl.cacheDeltaLayer(mergedMeta.Key, mergedParsed)
	}
	localPath := filepath.Join(tl.cacheDir, mergedMeta.Key)
	if dir := filepath.Dir(localPath); dir != "" {
		_ = tl.fs.MkdirAll(dir, 0o755)
	}
	_ = tl.fs.WriteFile(localPath, mergedData, 0o644)

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

	// Delete old deltas from S3 (best-effort).
	for _, dl := range deltas {
		_ = tl.store.DeleteObject(ctx, dl.Key)
	}

	// Clear caches for deleted layers.
	tl.deltaIndexMu.Lock()
	for _, dl := range deltas {
		delete(tl.deltaIndexCache, dl.Key)
	}
	tl.deltaIndexMu.Unlock()

	// Invalidate page cache — merged delta may have different data.
	if tl.pageCache != nil {
		for _, p := range parsed {
			for _, ie := range p.index {
				tl.pageCache.Delete(tl.id, ie.PageAddr)
			}
		}
	}

	tl.compactLog("  DONE (delta merge) tl=%s final deltas=%d images=%d",
		tl.id[:8], len(tl.layers.Deltas), len(tl.layers.Images))
	return nil
}

// downloadDeltaLayer downloads a complete delta layer blob from S3 or local cache.
func (tl *Timeline) downloadDeltaLayer(ctx context.Context, key string) ([]byte, error) {
	// Try local disk cache first.
	localPath := filepath.Join(tl.cacheDir, key)
	if data, err := tl.fs.ReadFile(localPath); err == nil {
		return data, nil
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

	// Cache locally.
	if dir := filepath.Dir(localPath); dir != "" {
		_ = tl.fs.MkdirAll(dir, 0o755)
	}
	_ = tl.fs.WriteFile(localPath, data, 0o644)

	return data, nil
}

// compactToImage builds an image layer from all compactable deltas + existing
// images. This is the original full-compaction behavior.
func (tl *Timeline) compactToImage(ctx context.Context, deltas []DeltaLayerMeta, existingImages []ImageLayerMeta, remainingDeltas []DeltaLayerMeta, compactSeq uint64) error {
	// Collect all unique pages across all delta layers (and existing images).
	pageSet := make(map[uint64]struct{})
	for _, dl := range deltas {
		parsed, err := tl.getDeltaLayer(ctx, dl.Key)
		if err != nil {
			return fmt.Errorf("get delta layer %s: %w", dl.Key, err)
		}
		for _, ie := range parsed.index {
			pageSet[ie.PageAddr] = struct{}{}
		}
	}
	for _, il := range existingImages {
		parsed, err := tl.getImageLayer(ctx, il.Key)
		if err != nil {
			return fmt.Errorf("get image layer %s: %w", il.Key, err)
		}
		for _, ie := range parsed.index {
			pageSet[ie.PageAddr] = struct{}{}
		}
	}

	// Sort page addresses.
	addrs := make([]uint64, 0, len(pageSet))
	for addr := range pageSet {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })
	tl.compactLog("  pageSet=%d pages, addrs=%v", len(addrs), addrs)

	// Read the latest value for each page (searching layers at compactSeq).
	var pages []imagePageInput
	for _, addr := range addrs {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := tl.readPage(ctx, addr, compactSeq)
		if err != nil {
			return fmt.Errorf("read page %d: %w", addr, err)
		}
		// Skip zero pages only when there's no ancestor. With an ancestor,
		// zero pages may be tombstones that mask ancestor data and must be
		// preserved in the image layer.
		if bytes.Equal(data, zeroPage[:]) && tl.ancestor == nil {
			tl.compactLog("  page %d: zero (skipped, root tl)", addr)
			continue
		}
		pages = append(pages, imagePageInput{PageAddr: addr, Data: data})
	}
	tl.compactLog("  non-zero pages=%d", len(pages))

	if len(pages) == 0 {
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
		for _, dl := range deltas {
			_ = tl.store.DeleteObject(ctx, dl.Key)
		}
		for _, il := range existingImages {
			_ = tl.store.DeleteObject(ctx, il.Key)
		}
		tl.compactLog("  ALL ZERO done, final deltas=%d images=%d", len(tl.layers.Deltas), len(tl.layers.Images))
		return nil
	}

	// Build image layer.
	imgData, imgMeta, err := buildImageLayer(compactSeq, pages)
	if err != nil {
		return fmt.Errorf("build image layer: %w", err)
	}

	// Upload image layer.
	if err := tl.store.PutReader(ctx, imgMeta.Key, bytes.NewReader(imgData)); err != nil {
		tl.compactLog("  image upload FAILED: %v", err)
		return fmt.Errorf("upload image layer: %w", err)
	}
	tl.compactLog("  image uploaded: %s seq=%d pages=[%d,%d]", imgMeta.Key, imgMeta.Seq, imgMeta.PageRange[0], imgMeta.PageRange[1])

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

	for _, dl := range deltas {
		_ = tl.store.DeleteObject(ctx, dl.Key)
	}
	for _, il := range existingImages {
		_ = tl.store.DeleteObject(ctx, il.Key)
	}

	tl.deltaIndexMu.Lock()
	for _, dl := range deltas {
		delete(tl.deltaIndexCache, dl.Key)
	}
	tl.deltaIndexMu.Unlock()

	tl.imageIndexMu.Lock()
	for _, il := range existingImages {
		delete(tl.imageIndexCache, il.Key)
	}
	tl.imageIndexMu.Unlock()

	tl.compactLog("  DONE tl=%s final deltas=%d images=%d", tl.id[:8], len(tl.layers.Deltas), len(tl.layers.Images))
	return nil
}

// DebugPage traces a page read through every layer, reporting what each one
// contains. Unlike readPage it does NOT short-circuit — it shows the full
// picture so you can see exactly why a read returns what it returns.
// Returns the debug trace as a string. Safe to call from tests.
func (tl *Timeline) DebugPage(ctx context.Context, pageAddr uint64, beforeSeq uint64) string {
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
		add("    image[%d]: %s seq=%d pages=[%d,%d]", i, il.Key, il.Seq, il.PageRange[0], il.PageRange[1])
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
	tl.frozenMu.Lock()
	frozen := make([]*MemLayer, len(tl.frozenLayers))
	copy(frozen, tl.frozenLayers)
	tl.frozenMu.Unlock()

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

	// 3. Page cache.
	if tl.pageCache != nil && beforeSeq == math.MaxUint64 {
		if cached := tl.pageCache.Get(tl.id, pageAddr); cached != nil {
			add("  [3] pageCache: zero=%v hash=%s", isZero(cached), hash(cached))
		} else {
			add("  [3] pageCache: miss")
		}
	} else {
		add("  [3] pageCache: skipped (beforeSeq != MaxUint64 or no cache)")
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
		if il.Seq > beforeSeq {
			add("  [5] image[%d] %s: seq=%d FILTERED (> beforeSeq=%d)", i, il.Key, il.Seq, beforeSeq)
			continue
		}
		if pageAddr < il.PageRange[0] || pageAddr > il.PageRange[1] {
			add("  [5] image[%d] %s: range=[%d,%d] NOT IN RANGE", i, il.Key, il.PageRange[0], il.PageRange[1])
			continue
		}
		data, found, err := tl.readFromImageLayer(ctx, il, pageAddr)
		if err != nil {
			add("  [5] image[%d] %s: err=%v", i, il.Key, err)
		} else if found {
			add("  [5] image[%d] %s: seq=%d FOUND zero=%v hash=%s", i, il.Key, il.Seq, isZero(data), hash(data))
		} else {
			add("  [5] image[%d] %s: seq=%d page not in layer", i, il.Key, il.Seq)
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
func (tl *Timeline) acquireLease(ctx context.Context, lm *loophole.LeaseManager) error {
	return loophole.ModifyJSON[TimelineMeta](ctx, tl.store, "meta.json", func(m *TimelineMeta) error {
		if err := lm.CheckAvailable(ctx, m.LeaseToken); err != nil {
			return fmt.Errorf("timeline %s: %w", tl.id, err)
		}
		m.LeaseToken = lm.Token()
		return nil
	})
}

// releaseLease clears the lease token from meta.json if it matches ours.
func (tl *Timeline) releaseLease(ctx context.Context, token string) error {
	return loophole.ModifyJSON[TimelineMeta](ctx, tl.store, "meta.json", func(m *TimelineMeta) error {
		if m.LeaseToken == token {
			m.LeaseToken = ""
		}
		return nil
	})
}

func (tl *Timeline) startPeriodicFlush() {
	tl.flushStop = make(chan struct{})
	tl.flushDone = make(chan struct{})
	go tl.periodicFlushLoop()
}

func (tl *Timeline) periodicFlushLoop() {
	defer close(tl.flushDone)
	timer := time.NewTimer(tl.config.FlushInterval)
	defer timer.Stop()
	for {
		select {
		case <-tl.flushStop:
			return
		case <-timer.C:
		}

		tl.mu.RLock()
		ml := tl.memLayer
		tl.mu.RUnlock()
		if !ml.isEmpty() {
			if err := tl.Flush(context.Background()); err != nil {
				slog.Warn("periodic flush failed", "timeline", tl.id, "error", err)
			}
		}
		// Wait another interval after the flush completes.
		timer.Reset(tl.config.FlushInterval)
	}
}

func (tl *Timeline) close(ctx context.Context) {
	// Stop periodic flush and wait for any in-progress flush to finish.
	if tl.flushStop != nil {
		close(tl.flushStop)
		<-tl.flushDone
	}

	// Flush all dirty data to S3 before cleaning up ephemeral files.
	if err := tl.Flush(ctx); err != nil {
		slog.Warn("flush on close failed", "timeline", tl.id, "error", err)
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

	if tl.ancestor != nil {
		tl.ancestor.close(ctx)
	}
}

// layersJSON is the S3-persisted layer map checkpoint.
type layersJSON struct {
	NextSeq uint64           `json:"next_seq"`
	Deltas  []DeltaLayerMeta `json:"deltas"`
	Images  []ImageLayerMeta `json:"images"`
}
