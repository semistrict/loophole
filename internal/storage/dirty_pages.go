package storage

import (
	"errors"
	"sort"
	"sync"
)

var (
	// errDirtyBatchClosed means a writer raced with batch rotation. The caller
	// must retry against the layer's current active batch.
	errDirtyBatchClosed = errors.New("dirty batch closed")

	// errDirtyBatchFull means the batch cannot accept the new record without
	// exceeding its per-batch bounds. The caller must rotate or wait.
	errDirtyBatchFull = errors.New("dirty batch full")

	// dirtyPagePool is shared across all layers. Retired dirty page buffers are
	// returned to this pool only from a safepoint-exclusive section so no
	// ReadPages caller can still be holding borrowed references.
	dirtyPagePool = sync.Pool{
		New: func() any {
			return new(Page)
		},
	}
)

// dirtyRecord is immutable once constructed and inserted into a batch.
// Writers always replace the map entry with a fresh record instead of mutating
// bytes in place.
type dirtyRecord struct {
	tombstone bool
	page      *Page
}

func newPageRecord(data Page) *dirtyRecord {
	page := dirtyPagePool.Get().(*Page)
	*page = data
	return &dirtyRecord{page: page}
}

func newTombstoneRecord() *dirtyRecord {
	return &dirtyRecord{tombstone: true}
}

func (r *dirtyRecord) bytes() []byte {
	if r == nil || r.tombstone {
		return zeroPage[:]
	}
	return r.page[:]
}

// sortedEntry is used when a pending batch is flushed to blocks. The record
// pointer is immutable and remains valid for the duration of the flush.
type sortedEntry struct {
	pageIdx PageIdx
	record  *dirtyRecord
}

// dirtyBatch is the in-memory write buffer for either the active mutable batch
// or the pending immutable batch.
//
// Mutex contract:
//   - mu protects only the mutable batch metadata: records, bytesUsed,
//     entryCount, and closed.
//   - readers take RLock only long enough to copy a record out. They must not
//     retain dirtyRecord pointers or byte slices backed by pooled page buffers
//     after unlocking.
//   - the background flusher is the one exception: it may hold RLock for the
//     full lifetime of a pending-batch flush so it can borrow dirty page
//     buffers without copying.
//   - writers take Lock only long enough to insert/replace/remove a map entry
//     and update counters.
//   - once a batch has been rotated out of active, closed is set and any late
//     writer that still holds the old batch pointer will see errDirtyBatchClosed
//     instead of mutating the now-pending batch.
//   - record payloads are immutable after insertion. Overwrites replace the map
//     entry with a fresh record and hand the old page buffer back to the layer
//     for deferred safepoint-based reclamation.
//   - batch teardown also happens under Lock. Once a batch has been removed
//     from layer state and no flush is using its entries, clearAndCollect
//     hands all remaining page buffers back to the layer for deferred
//     safepoint-based reclamation.
type dirtyBatch struct {
	mu sync.RWMutex

	records    map[PageIdx]*dirtyRecord
	bytesUsed  int64
	entryCount int
	maxBytes   int64
	maxEntries int
	closed     bool
}

func newDirtyBatch(cfg Config) *dirtyBatch {
	cfg.setDefaults()
	return &dirtyBatch{
		records:    make(map[PageIdx]*dirtyRecord),
		maxBytes:   cfg.FlushThreshold,
		maxEntries: cfg.maxDirtyPageSlots(),
	}
}

func (b *dirtyBatch) markClosed() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

func (b *dirtyBatch) isEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.records) == 0
}

func (b *dirtyBatch) lookup(pageIdx PageIdx) (*dirtyRecord, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rec, ok := b.records[pageIdx]
	return rec, ok
}

func (b *dirtyBatch) copyPage(pageIdx PageIdx, dst *Page) (ok bool, tombstone bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rec, ok := b.records[pageIdx]
	if !ok || rec == nil {
		return false, false
	}
	if rec.tombstone {
		return true, true
	}
	copy(dst[:], rec.page[:])
	return true, false
}

func (b *dirtyBatch) pages() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.records)
}

func (b *dirtyBatch) bytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bytesUsed
}

func (b *dirtyBatch) canStageLocked(pageIdx PageIdx, newRecord *dirtyRecord) error {
	if b.closed {
		return errDirtyBatchClosed
	}

	nextBytes := b.bytesUsed
	nextEntries := b.entryCount

	prev, ok := b.records[pageIdx]
	if !ok {
		nextEntries++
	} else if prev != nil && !prev.tombstone {
		nextBytes -= PageSize
	}
	if newRecord != nil && !newRecord.tombstone {
		nextBytes += PageSize
	}

	if nextEntries > b.maxEntries || nextBytes > b.maxBytes {
		return errDirtyBatchFull
	}
	return nil
}

func (b *dirtyBatch) stageRecord(pageIdx PageIdx, newRecord *dirtyRecord) (*Page, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.canStageLocked(pageIdx, newRecord); err != nil {
		if newRecord != nil && !newRecord.tombstone && newRecord.page != nil {
			dirtyPagePool.Put(newRecord.page)
			newRecord.page = nil
		}
		return nil, err
	}

	var retired *Page
	prev, ok := b.records[pageIdx]
	if !ok {
		b.entryCount++
	} else if prev != nil && !prev.tombstone {
		b.bytesUsed -= PageSize
		retired = prev.page
		prev.page = nil
	}
	if newRecord != nil && !newRecord.tombstone {
		b.bytesUsed += PageSize
	}
	b.records[pageIdx] = newRecord
	return retired, nil
}

func (b *dirtyBatch) stagePageWithRetired(pageIdx PageIdx, data Page) (*Page, error) {
	return b.stageRecord(pageIdx, newPageRecord(data))
}

// stagePageDirect stages a pool-allocated *Page into the batch without copying.
// On success, ownership transfers to the batch — the caller must not use page after.
// On error, ownership remains with the caller (the page is NOT returned to the pool).
func (b *dirtyBatch) stagePageDirect(pageIdx PageIdx, page *Page) (*Page, error) {
	rec := &dirtyRecord{page: page}

	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.canStageLocked(pageIdx, rec); err != nil {
		// Do NOT return page to pool — caller still owns it and may retry.
		rec.page = nil
		return nil, err
	}

	var retired *Page
	prev, ok := b.records[pageIdx]
	if !ok {
		b.entryCount++
	} else if prev != nil && !prev.tombstone {
		b.bytesUsed -= PageSize
		retired = prev.page
		prev.page = nil
	}
	b.bytesUsed += PageSize
	b.records[pageIdx] = rec
	return retired, nil
}

func (b *dirtyBatch) stagePage(pageIdx PageIdx, data Page) error {
	_, err := b.stagePageWithRetired(pageIdx, data)
	return err
}

func (b *dirtyBatch) stageTombstoneWithRetired(pageIdx PageIdx) (*Page, error) {
	return b.stageRecord(pageIdx, newTombstoneRecord())
}

func (b *dirtyBatch) stageTombstone(pageIdx PageIdx) error {
	_, err := b.stageTombstoneWithRetired(pageIdx)
	return err
}

func (b *dirtyBatch) entries() []sortedEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.entriesLocked()
}

func (b *dirtyBatch) entriesLocked() []sortedEntry {
	out := make([]sortedEntry, 0, len(b.records))
	for pageIdx, record := range b.records {
		out = append(out, sortedEntry{pageIdx: pageIdx, record: record})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].pageIdx < out[j].pageIdx
	})
	return out
}

func (b *dirtyBatch) clearAndCollect() []*Page {
	b.mu.Lock()
	defer b.mu.Unlock()

	retired := make([]*Page, 0, len(b.records))
	for pageIdx, record := range b.records {
		if record != nil && !record.tombstone && record.page != nil {
			retired = append(retired, record.page)
			record.page = nil
		}
		delete(b.records, pageIdx)
	}
	b.bytesUsed = 0
	b.entryCount = 0
	return retired
}
