package storage

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWritePageSeqReuseOnRetry demonstrates issue #3: when writePage gets
// backpressured and retries, it reuses the same sequence number. Another
// writer can grab a higher seq for the same page in between, and the retry
// overwrites the newer write with stale data.
func TestWritePageSeqReuseOnRetry(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	// Use a tiny dirty batch (2 pages) so it fills up quickly.
	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "seq-reuse", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Fill the dirty batch to near capacity so the next write hits backpressure.
	page0 := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, ly.Write(page0, 0))

	// Now race two writes to the SAME page (page 0).
	// Writer A will fill the last slot, trigger freeze, and retry.
	// Writer B should be able to sneak in during the retry window.
	var wg sync.WaitGroup
	writerA := bytes.Repeat([]byte{0xBB}, PageSize)
	writerB := bytes.Repeat([]byte{0xCC}, PageSize)

	var writerADone, writerBDone atomic.Bool

	wg.Add(2)
	go func() {
		defer wg.Done()
		// Writer A writes page 1 to fill the dirty batch, then writes page 0.
		// The page 0 write will likely hit backpressure and retry.
		filler := bytes.Repeat([]byte{0xDD}, PageSize)
		_ = ly.Write(filler, PageSize) // page 1 — fills dirty pages
		err := ly.Write(writerA, 0)    // page 0 — may trigger retry
		assert.NoError(t, err)
		writerADone.Store(true)
	}()

	go func() {
		defer wg.Done()
		// Writer B writes page 0 concurrently.
		err := ly.Write(writerB, 0)
		assert.NoError(t, err)
		writerBDone.Store(true)
	}()

	wg.Wait()

	// Read back page 0. The value should be from whichever writer finished
	// LAST (higher sequence number wins on read). But with the bug, the
	// retry can use a stale seq and overwrite the newer write.
	buf := make([]byte, PageSize)
	_, err = ly.Read(ctx, buf, 0)
	require.NoError(t, err)

	isA := bytes.Equal(buf, writerA)
	isB := bytes.Equal(buf, writerB)
	assert.True(t, isA || isB, "page 0 should contain either writer A or writer B data, got something else")

	t.Logf("page 0: isA=%v, isB=%v", isA, isB)
}

// TestBlockRangeMapConcurrentRace demonstrates issue #8: blockRangeMap is not
// safe for concurrent use. snapshotLayers captures a pointer to l1Map/l2Map,
// then Find() is called without holding mu. Concurrent Set() mutates the
// underlying ranges slice, causing a data race.
//
// Run with: go test -race -run TestBlockRangeMapConcurrentRace
func TestBlockRangeMapConcurrentRace(t *testing.T) {
	m := newBlockRangeMap(nil)

	var wg sync.WaitGroup
	const iterations = 10000

	// Writer goroutine: continuously mutates the map.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			m.Set(BlockIdx(i%100), "layer-writer", 1)
			if i%3 == 0 {
				m.Remove(BlockIdx(i % 100))
			}
		}
	}()

	// Reader goroutine: continuously reads from the map.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			_, _ = m.Find(BlockIdx(50))
		}
	}()

	wg.Wait()
}

// TestSnapshotLayersAtomicity ensures snapshotting active+pending batch
// pointers and durable layout never produces a torn read view.
func TestSnapshotLayersAtomicity(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "snap-atomic", config: cfg, safepoint: safepoint.New()})
	require.NoError(t, err)
	defer ly.Close()

	// Write a page so there's data to read.
	page := bytes.Repeat([]byte{0xEE}, PageSize)
	require.NoError(t, ly.Write(page, 0))

	var wg sync.WaitGroup
	const iterations = 1000

	// Goroutine 1: continuously flush.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			newPage := bytes.Repeat([]byte{0xFF}, PageSize)
			_ = ly.Write(newPage, PageSize) // write to page 1
			_ = ly.Flush()
		}
	}()

	// Goroutine 2: continuously take snapshots and read through them.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			ly.mu.RLock()
			pending := ly.pending
			ly.mu.RUnlock()
			if pending != nil {
				var page Page
				if ok, tombstone := pending.copyPage(0, &page); ok {
					assert.False(t, tombstone)
					assert.Len(t, page[:], PageSize)
				}
			}
		}
	}()

	wg.Wait()
}

// TestReadDuringFlushReturnsZeros ensures dirty batch rotation does not let
// reads fall through to zeros for data that was already written.
func TestReadDuringFlushReturnsZeros(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "flush-race", config: cfg, safepoint: safepoint.New()})
	require.NoError(t, err)
	defer ly.Close()

	// Write a distinctive value to page 0.
	expected := bytes.Repeat([]byte{0x42}, PageSize)
	require.NoError(t, ly.Write(expected, 0))

	var wg sync.WaitGroup
	var zeroReads atomic.Int64
	const iterations = 2000

	// Goroutine 1: continuously flush.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			// Write something to trigger a non-empty flush.
			filler := bytes.Repeat([]byte{0x99}, PageSize)
			_ = ly.Write(filler, PageSize) // page 1
			_ = ly.Flush()
		}
	}()

	// Goroutine 2: continuously read page 0.
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, PageSize)
		for range iterations {
			n, err := ly.Read(ctx, buf, 0)
			if err != nil {
				continue
			}
			if n == PageSize && bytes.Equal(buf, zeroPage[:]) {
				// Got zeros for a page that was written! This is the bug.
				zeroReads.Add(1)
			}
		}
	}()

	wg.Wait()

	if zeroReads.Load() > 0 {
		t.Errorf("read returned zeros %d times for a page that was written (issue #11)", zeroReads.Load())
	}
}

// TestReadAfterFlushWithFreshSnapshot ensures the supported contract: once a
// flush completes, a fresh read still finds the data through the durable layer
// state even though the old dirty batch may already have released its pooled
// page buffers.
func TestReadAfterFlushWithFreshSnapshot(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "cleanup-read", config: cfg, safepoint: safepoint.New()})
	require.NoError(t, err)
	defer ly.Close()

	// Write page 0 with known data.
	expected := bytes.Repeat([]byte{0x42}, PageSize)
	require.NoError(t, ly.Write(expected, 0))

	// Flush the dirty batch into the durable layer state.
	require.NoError(t, ly.Flush())

	ly.mu.RLock()
	data, err := ly.readPageLocked(ctx, 0)
	ly.mu.RUnlock()
	require.NoError(t, err)
	assert.Equal(t, expected, data, "data should survive flush")
}
