package storage2

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeriodicFlushTriggersCompaction(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  20 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1, // no background goroutine; we call doPeriodicFlush manually
		L0PagesMax:      50, // low threshold so compaction fires quickly
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write 20 pages per batch, flush 4 times → 80 total L0 page entries
	// (exceeds L0PagesMax = 50).
	for batch := range 4 {
		for i := range 20 {
			page := make([]byte, PageSize)
			page[0] = byte(batch)
			page[1] = byte(i)
			require.NoError(t, ly.Write(page, uint64(i)*PageSize))
		}
		require.NoError(t, ly.Flush())
	}

	ly.mu.RLock()
	l0Before := totalL0Pages(ly.index.L0)
	l0EntriesBefore := len(ly.index.L0)
	ly.mu.RUnlock()
	assert.Equal(t, 4, l0EntriesBefore)
	assert.Equal(t, 80, l0Before)

	// doPeriodicFlush should trigger compaction since we're above L0PagesMax.
	ly.doPeriodicFlush(ctx)

	ly.mu.RLock()
	l0After := len(ly.index.L0)
	l1Ranges := ly.l1Map.Len()
	l2Ranges := ly.l2Map.Len()
	ly.mu.RUnlock()
	assert.Equal(t, 0, l0After, "L0 entries should be cleared after compaction")
	assert.Greater(t, l1Ranges+l2Ranges, 0, "L1/L2 blocks should exist after compaction")

	// Verify data is still readable (last batch wins — batch 3).
	for i := range 20 {
		buf := make([]byte, PageSize)
		n, err := ly.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, byte(3), buf[0], "page %d: expected last batch marker", i)
		assert.Equal(t, byte(i), buf[1], "page %d: expected page marker", i)
	}
}

// TestCompactL0PartialFailure verifies that a compaction that fails partway
// through (e.g. S3 fault) does not corrupt the in-memory state. Before the
// fix, partially-updated l1Map/l2Map would cause reads to return wrong data.
func TestCompactL0PartialFailure(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write pages in two different block addresses (block 0 and block 1).
	// Block 0 = pages 0..1023, block 1 = pages 1024..2047.
	expected := make(map[uint64][]byte)
	for i := range 5 {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i)
		offset := uint64(i) * PageSize
		require.NoError(t, ly.Write(page, offset))
		expected[offset] = page
	}
	for i := range 5 {
		page := make([]byte, PageSize)
		page[0] = 0xBB
		page[1] = byte(i)
		offset := uint64(BlockPages+i) * PageSize // block 1
		require.NoError(t, ly.Write(page, offset))
		expected[offset] = page
	}
	require.NoError(t, ly.Flush())

	// Write tombstones (zeros) over block 1 pages.
	for i := range 5 {
		offset := uint64(BlockPages+i) * PageSize
		require.NoError(t, ly.Write(make([]byte, PageSize), offset))
		expected[offset] = make([]byte, PageSize)
	}
	require.NoError(t, ly.Flush())

	ly.mu.RLock()
	l0Pages := totalL0Pages(ly.index.L0)
	ly.mu.RUnlock()
	require.GreaterOrEqual(t, l0Pages, 10, "need enough L0 pages to trigger compaction")

	// Inject a PUT fault that fires on the 2nd L1 block upload during compaction.
	// Block 0's L1 upload succeeds, block 1's fails — partial compaction.
	var puts atomic.Int64
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		ShouldFault: func(key string) bool {
			if strings.Contains(key, "/l1/") {
				return puts.Add(1) > 1
			}
			return false
		},
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// CompactL0 should fail partway through.
	err = ly.CompactL0()
	require.Error(t, err, "compaction should fail due to injected fault")

	// Clear faults.
	store.ClearAllFaults()

	// Verify all pages are still readable with correct data.
	for offset, exp := range expected {
		buf := make([]byte, PageSize)
		n, readErr := ly.Read(ctx, buf, offset)
		require.NoError(t, readErr, "read at offset %d", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, exp, buf, "data mismatch at offset %d", offset)
	}

	// L0 entries should NOT have been cleared.
	ly.mu.RLock()
	l0After := len(ly.index.L0)
	ly.mu.RUnlock()
	assert.Greater(t, l0After, 0, "L0 entries should still exist after failed compaction")
}

// TestCompactAfterClonePunchHole reproduces a sim-found bug where:
// 1. Parent writes data to page P, flushes
// 2. Clone inherits parent's L0 entries
// 3. Clone punches page P (tombstone), flushes
// 4. CompactL0 on clone → page P should be zeros in L1
// 5. Read page P → must be zeros, not the parent's old data
func TestCompactAfterClonePunchHole(t *testing.T) {
	ctx := t.Context()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}
	m := newTestManager(t, loophole.NewMemStore(), cfg)

	// Create parent, write data to several pages including page 5.
	parent, err := m.NewVolume(loophole.CreateParams{Volume: "parent", Size: 256 * PageSize})
	require.NoError(t, err)

	for i := range 10 {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Verify parent has data for page 5.
	buf := make([]byte, PageSize)
	_, err = parent.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0])
	assert.Equal(t, byte(5), buf[1])

	// Clone from parent.
	clone := cloneOpen(t, parent, "child")

	// Verify clone inherited the data.
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0], "clone should see parent data before punch")

	// Punch page 5 in clone (tombstone).
	require.NoError(t, clone.PunchHole(5*PageSize, PageSize))
	require.NoError(t, clone.Flush())

	// Verify page 5 is zero after punch (before compaction).
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), buf, "page 5 should be zero after punch")

	// Write enough extra pages to trigger compaction.
	for i := 10; i < 25; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xBB
		page[1] = byte(i)
		require.NoError(t, clone.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, clone.Flush())

	// Run compaction on clone's layer.
	vol := clone.(*volume)
	err = vol.layer.CompactL0()
	require.NoError(t, err)

	// After compaction, L0 should be cleared.
	vol.layer.mu.RLock()
	l0After := len(vol.layer.index.L0)
	vol.layer.mu.RUnlock()
	assert.Equal(t, 0, l0After, "L0 should be cleared after compaction")

	// The critical check: page 5 must still be zeros after compaction
	// cleared L0 (tombstone is now in L1, not L0).
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), buf, "page 5 must be zero after compaction")

	// Also check the L1 block directly (not through page cache).
	vol.layer.mu.RLock()
	l1Map := vol.layer.l1Map
	vol.layer.mu.RUnlock()
	if layer, seq := l1Map.Find(PageIdx(5).Block()); layer != "" {
		data, found, err := vol.layer.readFromBlock(ctx, "l1", layer, seq, 5)
		require.NoError(t, err)
		if found {
			assert.Equal(t, make([]byte, PageSize), data, "L1 block should have zeros for page 5")
		}
	}
}

// TestCompactTwiceAfterClonePunchHole is a variant where the first compaction
// moves parent data from L0 to L1, then the punch happens, then a second
// compaction must correctly apply the tombstone over the L1 data.
func TestCompactTwiceAfterClonePunchHole(t *testing.T) {
	ctx := t.Context()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}
	m := newTestManager(t, loophole.NewMemStore(), cfg)

	// Create parent with data for page 5.
	parent, err := m.NewVolume(loophole.CreateParams{Volume: "parent", Size: 256 * PageSize})
	require.NoError(t, err)

	for i := range 10 {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Clone from parent.
	clone := cloneOpen(t, parent, "child")

	// Write enough extra pages to trigger the FIRST compaction, moving
	// inherited data (including page 5) from L0 to L1.
	for i := 10; i < 25; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xCC
		page[1] = byte(i)
		require.NoError(t, clone.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, clone.Flush())

	vol := clone.(*volume)
	err = vol.layer.CompactL0()
	require.NoError(t, err)

	// Verify page 5 has parent data in L1 after first compaction.
	buf := make([]byte, PageSize)
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0], "page 5 should still have parent data after first compact")

	// Now punch page 5 (tombstone).
	require.NoError(t, clone.PunchHole(5*PageSize, PageSize))
	require.NoError(t, clone.Flush())

	// Verify page 5 is zero (tombstone in L0 shadows L1 data).
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), buf, "page 5 should be zero after punch (before second compact)")

	// Write more pages to trigger the SECOND compaction.
	for i := 30; i < 45; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xDD
		page[1] = byte(i)
		require.NoError(t, clone.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, clone.Flush())

	err = vol.layer.CompactL0()
	require.NoError(t, err)

	// After second compaction, L0 should be cleared.
	vol.layer.mu.RLock()
	l0After := len(vol.layer.index.L0)
	vol.layer.mu.RUnlock()
	assert.Equal(t, 0, l0After, "L0 should be cleared")

	// The critical check: page 5 must be zeros.
	// The second compaction loaded L1 (with page 5 = 0xAA from first compact),
	// overlaid the L0 tombstone (zeros), and wrote a new L1 block.
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), buf, "page 5 must be zero after second compaction")
}

// TestCompactOverwritesClonedL1Block reproduces a sim-found bug (seed
// 2212817198790539364) where re-compaction on a parent layer overwrites an L1
// block that a cloned child still references. The L1 block key is deterministic
// (layerID + writeLeaseSeq + blockAddr), so a second CompactL0 on the parent
// writes to the same S3 key, mutating "immutable" data the child depends on.
//
// Timeline:
//  1. Parent writes data to page 5, flushes
//  2. Write enough to trigger compaction → L1 block created with page 5 data
//  3. Clone from parent → child inherits L1 map pointing to that L1 block
//  4. Parent punches page 5, flushes
//  5. Write enough to trigger second compaction → OVERWRITES L1 block with zeros
//  6. Child reads page 5 → should see original data, but gets zeros
func TestCompactOverwritesClonedL1Block(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}
	m := newTestManager(t, store, cfg)

	// Create parent, write data to several pages including page 5.
	parent, err := m.NewVolume(loophole.CreateParams{Volume: "parent", Size: 256 * PageSize})
	require.NoError(t, err)

	for i := range 10 {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Write more to exceed L0PagesMax and trigger compaction.
	for i := 10; i < 25; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xBB
		page[1] = byte(i)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Compact parent → L1 block created containing page 5 (0xAA, 5).
	parentVol := parent.(*volume)
	err = parentVol.layer.CompactL0()
	require.NoError(t, err)

	// Verify parent has page 5 data in L1.
	buf := make([]byte, PageSize)
	_, err = parent.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0])
	assert.Equal(t, byte(5), buf[1])

	// Clone from parent → child inherits L1 map entry for block 0.
	clone := cloneOpen(t, parent, "child")
	// Do NOT read page 5 from clone yet — we want to test the case where
	// the child has no cached data and must read from the L1 block.

	// Parent punches page 5.
	require.NoError(t, parent.PunchHole(5*PageSize, PageSize))
	require.NoError(t, parent.Flush())

	// Write more pages on parent to trigger second compaction.
	for i := 30; i < 45; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xCC
		page[1] = byte(i)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Second compaction on parent — overwrites L1 block with zeros for page 5.
	err = parentVol.layer.CompactL0()
	require.NoError(t, err)

	// Parent should see zeros for page 5 (punched).
	_, err = parent.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), buf, "parent page 5 should be zeros after punch+compact")

	// The critical check: child must still see the original data.
	// Before the fix, the second compaction overwrote the L1 block at the
	// same S3 key, so the child sees zeros instead of 0xAA.
	_, err = clone.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0],
		"child page 5 must still have original data — parent compaction must not overwrite shared L1 blocks")
	assert.Equal(t, byte(5), buf[1],
		"child page 5 must still have original data")
}

// TestCompactL0DropsConcurrentTombstone reproduces a sim-found bug where
// CompactL0 clears ALL L0 entries (ly.index.L0 = nil) at the end, including
// entries added by concurrent flushes that happened AFTER the compaction
// snapshot was taken. If a tombstone was flushed concurrently, it gets silently
// dropped, causing reads to return stale data from the L1 block.
//
// Timeline:
//  1. Write data + flush several times → L0 entries exceed L0PagesMax
//  2. CompactL0 starts, snapshots L0 entries
//  3. While CompactL0 uploads L1 blocks, a concurrent PunchHole + Flush adds
//     a tombstone L0 entry
//  4. CompactL0 finishes, sets L0 = nil → tombstone entry is lost
//  5. Read the punched page → should be zeros, but returns stale data
func TestCompactL0DropsConcurrentTombstone(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write 5 pages across 3 batches → 15 total L0 page entries (> L0PagesMax=10).
	for batch := range 3 {
		for i := range 5 {
			page := make([]byte, PageSize)
			page[0] = byte(batch)
			page[1] = byte(i)
			require.NoError(t, ly.Write(page, uint64(i)*PageSize))
		}
		require.NoError(t, ly.Flush())
	}

	ly.mu.RLock()
	l0Pages := totalL0Pages(ly.index.L0)
	ly.mu.RUnlock()
	require.GreaterOrEqual(t, l0Pages, 10, "need enough L0 pages to trigger compaction")

	// Use a Hook on PutReader (L1 block upload) to pause CompactL0 midway
	// and inject a concurrent tombstone flush.
	var hookOnce sync.Once
	compactPaused := make(chan struct{})
	compactResume := make(chan struct{})

	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		ShouldFault: func(key string) bool {
			return strings.Contains(key, "/l1/")
		},
		Hook: func() {
			hookOnce.Do(func() {
				close(compactPaused)
				<-compactResume
			})
		},
	})

	// Start CompactL0 in background goroutine.
	compactDone := make(chan error, 1)
	go func() {
		compactDone <- ly.CompactL0()
	}()

	// Wait for CompactL0 to reach its first L1 block upload (after snapshot).
	<-compactPaused

	// Clear faults so our tombstone flush can use PutReader normally.
	store.ClearAllFaults()

	// Punch page 0 and flush — this adds a new L0 entry with a tombstone
	// that CompactL0 doesn't know about (it already took its snapshot).
	require.NoError(t, ly.PunchHole(0, PageSize))
	require.NoError(t, ly.Flush())

	// Let CompactL0 resume.
	close(compactResume)

	// Wait for CompactL0 to finish.
	err = <-compactDone
	require.NoError(t, err)

	// After compaction: the tombstone L0 entry should NOT have been dropped.
	// Read page 0 — it was punched, so it must be zeros.
	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, make([]byte, PageSize), buf,
		"page 0 should be zeros after punch — compaction must not drop concurrent L0 entries")
}

// TestCompactL0CancelledByContext verifies that compaction respects context
// cancellation: it bails out early, leaves L0 entries intact, and data remains
// readable. This is the shutdown path — Close() cancels the periodic flush
// goroutine's context, and compaction should stop promptly.
func TestCompactL0CancelledByContext(t *testing.T) {
	readCtx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	ly, err := openLayer(readCtx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write pages across two different blocks so compaction has multiple
	// block groups to iterate over.
	expected := make(map[uint64][]byte)
	for i := range 5 {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i)
		offset := uint64(i) * PageSize
		require.NoError(t, ly.Write(page, offset))
		expected[offset] = page
	}
	for i := range 5 {
		page := make([]byte, PageSize)
		page[0] = 0xBB
		page[1] = byte(i)
		offset := uint64(BlockPages+i) * PageSize // block 1
		require.NoError(t, ly.Write(page, offset))
		expected[offset] = page
	}
	require.NoError(t, ly.Flush())

	ly.mu.RLock()
	l0Before := len(ly.index.L0)
	ly.mu.RUnlock()
	require.Greater(t, l0Before, 0)

	// Cancel the context before starting compaction.
	ctx, cancel := context.WithCancel(readCtx)
	cancel()

	ly.compactMu.Lock()
	err = ly.compactL0Locked(ctx, false)
	ly.compactMu.Unlock()
	require.ErrorIs(t, err, context.Canceled)

	// L0 entries must be unchanged — compaction bailed out before swapping.
	ly.mu.RLock()
	l0After := len(ly.index.L0)
	ly.mu.RUnlock()
	assert.Equal(t, l0Before, l0After, "L0 entries should be unchanged after cancelled compaction")

	// All data must still be readable.
	for offset, exp := range expected {
		buf := make([]byte, PageSize)
		n, readErr := ly.Read(readCtx, buf, offset)
		require.NoError(t, readErr, "read at offset %d", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, exp, buf, "data mismatch at offset %d", offset)
	}
}

// TestSnapshotS3OpCount measures the exact number of S3 operations triggered
// by a single Snapshot call (which internally does branch + relayer). This
// helps us understand why the simulation fuzzer is slow with large configs.
func TestSnapshotS3OpCount(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1, // no background flush
		L0PagesMax:      1000,
	}
	m := newTestManager(t, store, cfg)

	// Create volume and write one page of data.
	v, err := m.NewVolume(loophole.CreateParams{Volume: "root", Size: 1024 * 1024})
	require.NoError(t, err)

	page := make([]byte, PageSize)
	page[0] = 0xAA
	require.NoError(t, v.Write(page, 0))

	// Reset counters after setup.
	store.ResetCounts()

	// Snapshot: this triggers branch() → Flush + Snapshot(child) + relayer().
	require.NoError(t, snapshotVolume(t, v, "snap-1"))

	t.Logf("Snapshot S3 ops: Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNotExists=%d List=%d Delete=%d",
		store.Count(loophole.OpGet),
		store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS),
		store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists),
		store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))

	totalPuts := store.Count(loophole.OpPutReader) +
		store.Count(loophole.OpPutIfNotExists) +
		store.Count(loophole.OpPutBytesCAS) +
		store.Count(loophole.OpPutBytes)
	totalGets := store.Count(loophole.OpGet)
	t.Logf("Total: %d PUTs, %d GETs = %d S3 ops", totalPuts, totalGets, totalPuts+totalGets)

	// Now do a second snapshot with no new writes (empty memtable).
	store.ResetCounts()
	require.NoError(t, snapshotVolume(t, v, "snap-2"))

	t.Logf("Snapshot (no dirty data) S3 ops: Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNotExists=%d List=%d Delete=%d",
		store.Count(loophole.OpGet),
		store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS),
		store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists),
		store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))

	totalPuts2 := store.Count(loophole.OpPutReader) +
		store.Count(loophole.OpPutIfNotExists) +
		store.Count(loophole.OpPutBytesCAS) +
		store.Count(loophole.OpPutBytes)
	totalGets2 := store.Count(loophole.OpGet)
	t.Logf("Total: %d PUTs, %d GETs = %d S3 ops", totalPuts2, totalGets2, totalPuts2+totalGets2)

	// Now measure Clone (same as Snapshot + creates a volume ref).
	store.ResetCounts()
	clone := cloneOpen(t, v, "clone-1")

	t.Logf("Clone S3 ops: Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNotExists=%d List=%d Delete=%d",
		store.Count(loophole.OpGet),
		store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS),
		store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists),
		store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))

	totalPuts3 := store.Count(loophole.OpPutReader) +
		store.Count(loophole.OpPutIfNotExists) +
		store.Count(loophole.OpPutBytesCAS) +
		store.Count(loophole.OpPutBytes)
	totalGets3 := store.Count(loophole.OpGet)
	t.Logf("Total: %d PUTs, %d GETs = %d S3 ops", totalPuts3, totalGets3, totalPuts3+totalGets3)

	_ = clone
}
