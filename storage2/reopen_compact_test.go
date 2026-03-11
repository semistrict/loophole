package storage2

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReopenAfterCompactionThenCompactAgain reproduces a bug where:
//
//  1. Process A creates a volume, writes enough data to trigger L0→L1/L2
//     compaction, then exits (closing the volume/manager).
//  2. Process B opens the same volume (new manager, new write lease seq),
//     writes more data, and triggers another compaction.
//  3. Process B's compaction reads the l2Map inherited from process A's
//     index. The l2Map references L2 blocks keyed with process A's
//     writeLeaseSeq. If those blocks are missing or the key doesn't match,
//     compaction fails with "not found".
//
// In production this manifests as:
//
//	read existing L2 block: get layers/.../l2/0000000000000000-<blockaddr>: not found
//
// while the store contains the same blockaddr at a different writeLeaseSeq.
func TestReopenAfterCompactionThenCompactAgain(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      50,
	}

	// --- Process A: create volume, write data, compact, close ---

	cacheDir1 := t.TempDir()
	dc1, err := NewPageCache(filepath.Join(cacheDir1, "diskcache"))
	require.NoError(t, err)
	m1 := NewVolumeManager(store, cacheDir1, cfg, nil, dc1)

	vol1, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "test-vol",
		Size:     uint64(BlockPages*4) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	// Write enough pages across multiple blocks to trigger L2 promotion
	// during compaction. L1PromoteThreshold = BlockPages/4 = 256, so
	// writing a full block (1024 pages) guarantees L2 promotion.
	nPages := BlockPages + 100 // more than one full block
	expected := make(map[uint64]byte)
	for i := 0; i < nPages; i++ {
		page := make([]byte, PageSize)
		page[0] = byte(i % 251)
		page[1] = byte(i / 251)
		offset := uint64(i) * PageSize
		require.NoError(t, vol1.Write(page, offset))
		expected[offset] = page[0]
	}
	require.NoError(t, vol1.Flush())

	// Compact explicitly — this creates L1 and possibly L2 blocks.
	v1 := vol1.(*volume)
	err = v1.layer.ForceCompactL0()
	require.NoError(t, err)

	// Verify L0 is cleared and L1/L2 exist.
	v1.layer.mu.RLock()
	l0After := len(v1.layer.index.L0)
	l1Ranges := v1.layer.l1Map.Len()
	l2Ranges := v1.layer.l2Map.Len()
	v1.layer.mu.RUnlock()
	assert.Equal(t, 0, l0After, "L0 should be cleared after compaction")
	t.Logf("After first compaction: L1 ranges=%d, L2 ranges=%d", l1Ranges, l2Ranges)

	// Verify all data is readable before close.
	for offset, marker := range expected {
		buf := make([]byte, PageSize)
		n, err := vol1.Read(ctx, buf, offset)
		require.NoError(t, err, "read at offset %d before close", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, marker, buf[0], "data mismatch at offset %d before close", offset)
	}

	// Close manager (simulates process A exiting cleanly).
	require.NoError(t, m1.Close(ctx))
	require.NoError(t, dc1.Close())

	// --- Process B: reopen volume, write more data, compact again ---

	cacheDir2 := t.TempDir()
	dc2, err := NewPageCache(filepath.Join(cacheDir2, "diskcache"))
	require.NoError(t, err)
	defer dc2.Close()
	m2 := NewVolumeManager(store, cacheDir2, cfg, nil, dc2)
	defer m2.Close(ctx)

	vol2, err := m2.OpenVolume("test-vol")
	require.NoError(t, err)

	// Verify all original data is still readable after reopen.
	for offset, marker := range expected {
		buf := make([]byte, PageSize)
		n, err := vol2.Read(ctx, buf, offset)
		require.NoError(t, err, "read at offset %d after reopen", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, marker, buf[0], "data mismatch at offset %d after reopen", offset)
	}

	// Write more data (overlapping + new pages) to generate new L0 entries.
	expected2 := make(map[uint64]byte)
	for k, v := range expected {
		expected2[k] = v
	}
	for i := 0; i < nPages; i++ {
		page := make([]byte, PageSize)
		page[0] = byte((i + 100) % 251)
		page[1] = byte((i + 100) / 251)
		offset := uint64(i) * PageSize
		require.NoError(t, vol2.Write(page, offset))
		expected2[offset] = page[0]
	}
	require.NoError(t, vol2.Flush())

	// This is the critical operation: compact on the reopened volume.
	// Process B has a different writeLeaseSeq than process A.
	// Compaction must be able to read the L2 blocks written by process A.
	v2 := vol2.(*volume)
	err = v2.layer.ForceCompactL0()
	require.NoError(t, err, "compaction after reopen must succeed")

	// Verify all data (new overwrites) is correct after second compaction.
	for offset, marker := range expected2 {
		buf := make([]byte, PageSize)
		n, err := vol2.Read(ctx, buf, offset)
		require.NoError(t, err, "read at offset %d after second compaction", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, marker, buf[0], "data mismatch at offset %d after second compaction", offset)
	}
}

// TestReopenAfterInterruptedCompaction simulates a process crash during
// compaction: compaction runs partway (some blocks uploaded, some not),
// then the process crashes. A new process opens the volume and must be
// able to read all data and compact successfully.
//
// This is the exact scenario from the Firecracker+loophole integration:
// the import tool exits while background compaction is in-flight. The
// compaction had partially updated the in-memory l1Map/l2Map and a
// concurrent flush saved the index with those partial references. On
// reopen, the index references blocks that were never uploaded.
func TestReopenAfterInterruptedCompaction(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      50,
	}

	// --- Process A: create volume, write data, flush ---

	cacheDir1 := t.TempDir()
	dc1, err := NewPageCache(filepath.Join(cacheDir1, "diskcache"))
	require.NoError(t, err)
	m1 := NewVolumeManager(store, cacheDir1, cfg, nil, dc1)

	vol1, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "crash-vol",
		Size:     uint64(BlockPages*4) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	// Write enough data to trigger compaction with L2 promotion.
	nPages := BlockPages + 100
	expected := make(map[uint64]byte)
	for i := 0; i < nPages; i++ {
		page := make([]byte, PageSize)
		page[0] = byte(i % 251)
		offset := uint64(i) * PageSize
		require.NoError(t, vol1.Write(page, offset))
		expected[offset] = page[0]
	}
	require.NoError(t, vol1.Flush())

	// Simulate compaction that gets interrupted: inject a fault that lets
	// the first L1 block upload succeed but fails on the second one.
	// This means compaction partially updates newL1Map/newL2Map but never
	// reaches saveIndex — the error causes compactL0Locked to return early
	// WITHOUT swapping the maps, so the index should be safe.
	var putCount atomic.Int64
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		ShouldFault: func(key string) bool {
			if strings.Contains(key, "/l1/") || strings.Contains(key, "/l2/") {
				n := putCount.Add(1)
				t.Logf("PutReader block #%d: %s", n, key)
				return n > 1 // fail on 2nd block upload
			}
			return false
		},
		Err: fmt.Errorf("injected crash"),
	})

	// Compaction should fail partway through.
	v1 := vol1.(*volume)
	err = v1.layer.ForceCompactL0()
	t.Logf("Interrupted compaction result: %v (expected error)", err)
	require.Error(t, err, "compaction should fail due to injected fault")

	// Clear faults.
	store.ClearAllFaults()

	// Close (simulates process crash/restart — index was NOT updated by
	// the failed compaction, so L0 entries are still intact).
	require.NoError(t, m1.Close(ctx))
	require.NoError(t, dc1.Close())

	// --- Process B: reopen volume, verify data, write more, compact ---

	cacheDir2 := t.TempDir()
	dc2, err := NewPageCache(filepath.Join(cacheDir2, "diskcache"))
	require.NoError(t, err)
	defer dc2.Close()
	m2 := NewVolumeManager(store, cacheDir2, cfg, nil, dc2)
	defer m2.Close(ctx)

	vol2, err := m2.OpenVolume("crash-vol")
	require.NoError(t, err)

	// All original data must be readable (it's still in L0).
	for offset, marker := range expected {
		buf := make([]byte, PageSize)
		n, err := vol2.Read(ctx, buf, offset)
		require.NoError(t, err, "read at offset %d after reopen", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, marker, buf[0], "data mismatch at offset %d after reopen", offset)
	}

	// Write more data and flush.
	for i := 0; i < 100; i++ {
		page := make([]byte, PageSize)
		page[0] = byte((i + 50) % 251)
		offset := uint64(i) * PageSize
		require.NoError(t, vol2.Write(page, offset))
		expected[offset] = page[0]
	}
	require.NoError(t, vol2.Flush())

	// Compact again — must succeed. The store has orphaned block files
	// from the interrupted first compaction, but the index doesn't
	// reference them, so compaction should work cleanly.
	v2 := vol2.(*volume)
	err = v2.layer.ForceCompactL0()
	require.NoError(t, err, "compaction after interrupted compaction + reopen must succeed")

	// Verify all data.
	for offset, marker := range expected {
		buf := make([]byte, PageSize)
		n, err := vol2.Read(ctx, buf, offset)
		require.NoError(t, err, "read at offset %d after final compaction", offset)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, marker, buf[0], "data mismatch at offset %d after final compaction", offset)
	}
}

// TestConcurrentFlushDuringCompactionSavesStaleIndex reproduces the actual
// production bug: compaction swaps the in-memory l1Map/l2Map, then a
// concurrent flush calls saveIndex which persists those new map entries.
// If the process crashes after saveIndex but before compaction removes the
// now-compacted L0 entries, the reopened index has L1/L2 refs AND the old
// L0 entries — which is fine. But if the process crashes AFTER the L0
// entries are removed but BEFORE all blocks finish uploading, the index
// references blocks that don't exist.
//
// More precisely: compaction uploads blocks one at a time. After each
// block, it updates newL1Map/newL2Map locally. At the end of ALL blocks,
// it atomically swaps ly.l1Map/ly.l2Map and removes processed L0 entries.
// A concurrent flush that runs AFTER the swap calls saveIndex, persisting
// the new maps. If the process then crashes before compaction's own
// saveIndex (or before it returns), the saved index references all the
// blocks — which is fine because they were all uploaded before the swap.
//
// The ACTUAL bug in the Firecracker case was different: the import tool's
// background periodic flush triggered compaction, which ran to completion
// and saved the index. Then the import tool wrote MORE data, triggering
// another flush + compaction. The second compaction was interrupted by
// process exit. The index from the FIRST compaction had L2 refs. The
// second compaction, when it ran in FC's process, tried to read those L2
// blocks but they had been overwritten by the second (interrupted)
// compaction in the import process.
//
// This test reproduces that: two compactions, second one interrupted,
// L2 blocks from first compaction corrupted by partial second compaction.
// TestSecondCompactionInterruptCorruptsL2 verifies that a failed second
// compaction (e.g. process crash) preserves the first compaction's L2 data.
//
// The production bug was in FileStore using os.WriteFile (which truncates
// before writing), fixed by switching to atomicWriteFile (write-to-temp-
// then-rename). This test verifies the correct behavior: when the second
// compaction's L2 upload fails, the first compaction's L2 blocks remain
// intact and readable after reopen.
func TestSecondCompactionInterruptCorruptsL2(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      50,
	}

	cacheDir1 := t.TempDir()
	dc1, err := NewPageCache(filepath.Join(cacheDir1, "diskcache"))
	require.NoError(t, err)
	m1 := NewVolumeManager(store, cacheDir1, cfg, nil, dc1)

	vol1, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "corrupt-vol",
		Size:     uint64(BlockPages*4) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	// --- First batch: write full block (1024 pages), flush, compact → L2 ---

	for i := 0; i < BlockPages; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xAA
		page[1] = byte(i % 256)
		require.NoError(t, vol1.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, vol1.Flush())

	v1 := vol1.(*volume)
	require.NoError(t, v1.layer.ForceCompactL0())

	v1.layer.mu.RLock()
	l2Count := v1.layer.l2Map.Len()
	v1.layer.mu.RUnlock()
	t.Logf("After first compaction: L2 ranges=%d", l2Count)
	require.Greater(t, l2Count, 0, "should have L2 blocks after compacting a full block")

	// Verify a page is readable.
	buf := make([]byte, PageSize)
	_, err = vol1.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), buf[0])

	// --- Second batch: overwrite >= L1PromoteThreshold pages in block 0 ---
	// L1PromoteThreshold = BlockPages/4 = 256. Write 300 pages so the
	// second compaction triggers L2 promotion (writes to the same L2 key).

	secondBatchSize := L1PromoteThreshold + 44 // 300 pages
	for i := 0; i < secondBatchSize; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xBB
		page[1] = byte(i % 256)
		require.NoError(t, vol1.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, vol1.Flush())

	// Inject fault on L2 PutReader: the second compaction fails to upload
	// the new L2 block. With the atomicWriteFile fix in FileStore, the
	// old L2 data is preserved (write-to-temp-then-rename means a failed
	// write never touches the original file). MemStore's Err fault also
	// preserves old data (operation doesn't execute on error).
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		ShouldFault: func(key string) bool {
			return strings.Contains(key, "/l2/")
		},
		Err: fmt.Errorf("injected crash"),
	})

	// Second compaction — should fail because L2 upload faults.
	err = v1.layer.ForceCompactL0()
	t.Logf("Second compaction result: %v", err)
	require.Error(t, err, "second compaction should fail due to injected fault")

	store.ClearAllFaults()

	// Close process A (simulates restart — index was NOT updated by the
	// failed compaction because maps weren't swapped).
	require.NoError(t, m1.Close(ctx))
	require.NoError(t, dc1.Close())

	// --- Process B: reopen and verify ---

	cacheDir2 := t.TempDir()
	dc2, err := NewPageCache(filepath.Join(cacheDir2, "diskcache"))
	require.NoError(t, err)
	defer dc2.Close()
	m2 := NewVolumeManager(store, cacheDir2, cfg, nil, dc2)
	defer m2.Close(ctx)

	vol2, err := m2.OpenVolume("corrupt-vol")
	require.NoError(t, err)

	// Read all pages in block 0. The second compaction failed, so the
	// index should still have the first compaction's L2 ref. BUT the
	// Hook deleted the L2 data — so this read may fail with "not found".
	// Pages 0..299 should be 0xBB (from L0, second batch was flushed
	// but compaction failed — data is still in L0).
	// Pages 300..1023 should be 0xAA (from L2... which was deleted).
	for i := 0; i < BlockPages; i++ {
		buf := make([]byte, PageSize)
		_, err := vol2.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err, "read page %d after reopen", i)
		if i < secondBatchSize {
			assert.Equal(t, byte(0xBB), buf[0], "page %d should be 0xBB (second batch)", i)
		} else {
			assert.Equal(t, byte(0xAA), buf[0], "page %d should be 0xAA (first batch)", i)
		}
	}

	// Now compact on the reopened volume — this is where the production
	// bug manifests. The index references L2 blocks that were deleted by
	// the simulated truncation.
	v2 := vol2.(*volume)

	// Write enough data to trigger L2 promotion again.
	for i := 0; i < L1PromoteThreshold+10; i++ {
		page := make([]byte, PageSize)
		page[0] = 0xCC
		require.NoError(t, vol2.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, vol2.Flush())

	err = v2.layer.ForceCompactL0()
	require.NoError(t, err, "compaction on reopened volume must succeed even after L2 data loss")

	// Verify final data.
	for i := 0; i < BlockPages; i++ {
		buf := make([]byte, PageSize)
		_, err := vol2.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err, "read page %d after final compaction", i)
		if i < L1PromoteThreshold+10 {
			assert.Equal(t, byte(0xCC), buf[0], "page %d should be 0xCC (third batch)", i)
		} else if i < secondBatchSize {
			assert.Equal(t, byte(0xBB), buf[0], "page %d should be 0xBB (second batch)", i)
		} else {
			assert.Equal(t, byte(0xAA), buf[0], "page %d should be 0xAA (first batch)", i)
		}
	}
}
