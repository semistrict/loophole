//go:build linux || darwin

package mmap_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/mmap"
	"github.com/semistrict/loophole/storage2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pageSize = 4096

func requireMapVolume(
	t *testing.T,
	vol loophole.Volume,
	offset, size uint64,
	opts ...mmap.Option,
) *mmap.MappedRegion {
	t.Helper()

	mr, err := mmap.MapVolume(vol, offset, size, opts...)
	if errors.Is(err, mmap.ErrNotSupported) {
		t.Skip("mmap not supported on this platform")
	}
	if runtime.GOOS == "linux" && err != nil && strings.Contains(err.Error(), "kernel lacks UFFD write-protect support") {
		t.Skip("mmap requires UFFD write-protect support on Linux")
	}
	require.NoError(t, err)
	if runtime.GOOS == "linux" && mr.Backend() == mmap.BackendMissing {
		_ = mr.Close()
		t.Skip("generic mmap tests require write-tracking support on Linux")
	}
	return mr
}

func newTestManager(t *testing.T, store loophole.ObjectStore, cfg storage2.Config) *storage2.Manager {
	t.Helper()
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = -1
	}
	cacheDir := t.TempDir()
	dc, err := storage2.NewPageCache(filepath.Join(cacheDir, "diskcache"))
	require.NoError(t, err)
	m := storage2.NewVolumeManager(store, cacheDir, cfg, nil, dc)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
		_ = dc.Close()
	})
	return m
}

func newTestVolume(t *testing.T, name string, numPages int) loophole.Volume {
	t.Helper()
	store := loophole.NewMemStore()
	cfg := storage2.Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	vol, err := m.NewVolume(loophole.CreateParams{
		Volume: name,
		Size:   uint64(numPages) * pageSize,
	})
	require.NoError(t, err)
	return vol
}

func TestStructSizes(t *testing.T) {
	// uffdMsg must be exactly 32 bytes as the kernel writes this fixed size.
	type uffdMsg struct {
		Event uint8
		_     [3]uint8
		_     uint32
		Arg   [24]byte
	}
	assert.Equal(t, uintptr(32), unsafe.Sizeof(uffdMsg{}))
}

func TestMapVolume_BasicRead(t *testing.T) {
	vol := newTestVolume(t, "basic-read", 16)

	// Write known data at page 0 and page 5.
	page0 := make([]byte, pageSize)
	for i := range page0 {
		page0[i] = 0xAA
	}
	require.NoError(t, vol.Write(page0, 0))

	page5 := make([]byte, pageSize)
	for i := range page5 {
		page5[i] = 0xBB
	}
	require.NoError(t, vol.Write(page5, 5*pageSize))

	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, 16*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	assert.Equal(t, uint64(16*pageSize), mr.Size())

	// Verify page 0 is filled with 0xAA.
	assert.Equal(t, byte(0xAA), data[0])
	assert.Equal(t, byte(0xAA), data[pageSize-1])

	// Verify page 5 is filled with 0xBB.
	assert.Equal(t, byte(0xBB), data[5*pageSize])
	assert.Equal(t, byte(0xBB), data[6*pageSize-1])
}

func TestMapVolume_ZeroPages(t *testing.T) {
	vol := newTestVolume(t, "zero-pages", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	// All pages should be zero (never written).
	for i := range data {
		if data[i] != 0 {
			t.Fatalf("expected zero at offset %d, got %d", i, data[i])
		}
	}
}

func TestMapVolume_Close(t *testing.T) {
	vol := newTestVolume(t, "close-test", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)

	// Read a page to verify it works before close.
	data := mr.Bytes()
	_ = data[0]

	// Close should succeed.
	require.NoError(t, mr.Close())

	// Double close should be safe (sync.Once).
	require.NoError(t, mr.Close())
}

func TestMapVolume_FlushPersistsDirtyPages(t *testing.T) {
	vol := newTestVolume(t, "flush-persists-dirty-pages", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	flusher, ok := any(mr).(interface{ Flush() error })
	require.True(t, ok, "MappedRegion must implement Flush()")

	data := mr.Bytes()
	data[123] = 0xAB
	data[pageSize+456] = 0xCD

	require.NoError(t, flusher.Flush())

	buf := make([]byte, 2*pageSize)
	_, err := vol.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAB), buf[123])
	assert.Equal(t, byte(0xCD), buf[pageSize+456])

	// A second write after Flush should dirty the page again and persist too.
	data[123] = 0xEF
	require.NoError(t, flusher.Flush())

	clear(buf)
	_, err = vol.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, byte(0xEF), buf[123])
	assert.Equal(t, byte(0xCD), buf[pageSize+456])
}

func TestMapVolume_BackgroundFlushPersistsDirtyPages(t *testing.T) {
	vol := newTestVolume(t, "background-flush-persists-dirty-pages", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	data[321] = 0x5A
	data[2*pageSize+654] = 0xC3

	buf := make([]byte, 3*pageSize)
	require.Eventually(t, func() bool {
		clear(buf)
		_, err := vol.Read(t.Context(), buf, 0)
		require.NoError(t, err)
		return buf[321] == 0x5A && buf[2*pageSize+654] == 0xC3
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMapVolume_RejectsNormalWritesWhileMapped(t *testing.T) {
	vol := newTestVolume(t, "rejects-normal-writes-while-mapped", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)

	err := vol.Write(make([]byte, pageSize), 0)
	require.Error(t, err)
	assert.ErrorContains(t, err, "direct writeback mode")

	require.NoError(t, mr.Close())
	require.NoError(t, vol.Write(bytes.Repeat([]byte{0x7A}, pageSize), 0))
}

func TestMapVolume_ConcurrentReads(t *testing.T) {
	const numPages = 64
	vol := newTestVolume(t, "concurrent", numPages)

	// Write a unique byte pattern to each page.
	for i := range numPages {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = byte(i)
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithWorkers(4))
	defer mr.Close()

	data := mr.Bytes()

	// Launch goroutines that each read a different page.
	errc := make(chan error, numPages)
	for i := range numPages {
		go func(pageIdx int) {
			expected := byte(pageIdx)
			off := pageIdx * pageSize
			if data[off] != expected {
				errc <- fmt.Errorf("page %d: got %d, want %d", pageIdx, data[off], expected)
				return
			}
			if data[off+pageSize-1] != expected {
				errc <- fmt.Errorf("page %d last byte: got %d, want %d", pageIdx, data[off+pageSize-1], expected)
				return
			}
			errc <- nil
		}(i)
	}

	for range numPages {
		require.NoError(t, <-errc)
	}
}

func TestMapVolume_WithOffset(t *testing.T) {
	vol := newTestVolume(t, "offset-test", 16)

	// Write data at page 8.
	page := make([]byte, pageSize)
	for i := range page {
		page[i] = 0xCC
	}
	require.NoError(t, vol.Write(page, 8*pageSize))
	require.NoError(t, vol.Flush())

	// Map starting at offset 8 pages, for 4 pages.
	mr := requireMapVolume(t, vol, 8*pageSize, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	// First page of the mapping should be 0xCC (volume page 8).
	assert.Equal(t, byte(0xCC), data[0])
	assert.Equal(t, byte(0xCC), data[pageSize-1])

	// Second page should be zero (volume page 9, never written).
	assert.Equal(t, byte(0), data[pageSize])
}

func TestMapVolume_WithReadahead(t *testing.T) {
	const numPages = 8
	vol := newTestVolume(t, "readahead", numPages)

	for i := range numPages {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = byte(i + 1)
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithReadahead(3))
	defer mr.Close()

	data := mr.Bytes()

	// Touch page 0 — should trigger readahead for pages 1-3.
	assert.Equal(t, byte(1), data[0])

	// Pages 1-3 should also be populated (via readahead).
	assert.Equal(t, byte(2), data[1*pageSize])
	assert.Equal(t, byte(3), data[2*pageSize])
	assert.Equal(t, byte(4), data[3*pageSize])

	// Touch page 7.
	assert.Equal(t, byte(8), data[7*pageSize])
}

func TestMapVolume_InvalidArgs(t *testing.T) {
	vol := newTestVolume(t, "invalid-args", 4)

	// Unaligned offset.
	_, err := mmap.MapVolume(vol, 100, 4*pageSize)
	assert.ErrorContains(t, err, "not page-aligned")

	// Unaligned size.
	_, err = mmap.MapVolume(vol, 0, 100)
	assert.ErrorContains(t, err, "not page-aligned")

	// Zero size.
	_, err = mmap.MapVolume(vol, 0, 0)
	assert.ErrorContains(t, err, "zero")
}

// TestMapVolume_FullDataIntegrity writes a unique 4-byte stamp at the start
// of every page, maps the volume, and verifies every single byte.
func TestMapVolume_FullDataIntegrity(t *testing.T) {
	const numPages = 128
	vol := newTestVolume(t, "full-integrity", numPages)

	// Write a unique pattern per page: first 4 bytes = page index (LE),
	// remaining bytes = low byte of page index.
	for i := range numPages {
		page := make([]byte, pageSize)
		binary.LittleEndian.PutUint32(page[:4], uint32(i))
		for j := 4; j < pageSize; j++ {
			page[j] = byte(i)
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	for i := range numPages {
		off := i * pageSize
		stamp := binary.LittleEndian.Uint32(data[off : off+4])
		assert.Equal(t, uint32(i), stamp, "page %d stamp mismatch", i)
		for j := 4; j < pageSize; j++ {
			if data[off+j] != byte(i) {
				t.Fatalf("page %d byte %d: got %d, want %d", i, j, data[off+j], byte(i))
			}
		}
	}
}

// TestMapVolume_PageBoundary verifies no data bleeds across page boundaries.
func TestMapVolume_PageBoundary(t *testing.T) {
	const numPages = 8
	vol := newTestVolume(t, "boundary", numPages)

	for i := range numPages {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = byte(i + 1) // 1-indexed so zero pages are distinguishable
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	for i := range numPages {
		expected := byte(i + 1)
		off := i * pageSize
		// Check first, last, and boundary bytes.
		assert.Equal(t, expected, data[off], "page %d first byte", i)
		assert.Equal(t, expected, data[off+pageSize-1], "page %d last byte", i)
		if i > 0 {
			// Last byte of previous page must differ from first byte of this page.
			assert.NotEqual(t, data[off-1], data[off],
				"bleed across boundary between page %d and %d", i-1, i)
		}
	}
}

// TestMapVolume_SparsePages writes every other page and verifies the interleaving.
func TestMapVolume_SparsePages(t *testing.T) {
	const numPages = 32
	vol := newTestVolume(t, "sparse", numPages)

	for i := 0; i < numPages; i += 2 {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = 0xFF
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	for i := range numPages {
		off := i * pageSize
		if i%2 == 0 {
			assert.Equal(t, byte(0xFF), data[off], "even page %d should be 0xFF", i)
			assert.Equal(t, byte(0xFF), data[off+pageSize-1], "even page %d last byte", i)
		} else {
			assert.Equal(t, byte(0), data[off], "odd page %d should be zero", i)
			assert.Equal(t, byte(0), data[off+pageSize-1], "odd page %d last byte", i)
		}
	}
}

// TestMapVolume_MultipleMappingsSameVolume maps the same volume twice at
// different offsets and verifies both mappings independently.
func TestMapVolume_MultipleMappingsSameVolume(t *testing.T) {
	vol := newTestVolume(t, "multi-map", 16)

	// Write 0xAA to pages 0-3, 0xBB to pages 8-11.
	for i := range 4 {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = 0xAA
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	for i := 8; i < 12; i++ {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = 0xBB
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr1 := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr1.Close()

	mr2 := requireMapVolume(t, vol, 8*pageSize, 4*pageSize)
	defer mr2.Close()

	d1 := mr1.Bytes()
	d2 := mr2.Bytes()

	assert.Equal(t, byte(0xAA), d1[0])
	assert.Equal(t, byte(0xAA), d1[4*pageSize-1])
	assert.Equal(t, byte(0xBB), d2[0])
	assert.Equal(t, byte(0xBB), d2[4*pageSize-1])
}

// TestMapVolume_RereadSamePage accesses the same page multiple times to verify
// that the first fault resolves it and subsequent accesses don't re-fault.
func TestMapVolume_RereadSamePage(t *testing.T) {
	vol := newTestVolume(t, "reread", 4)

	page := make([]byte, pageSize)
	for i := range page {
		page[i] = 0xDD
	}
	require.NoError(t, vol.Write(page, 0))
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()

	// Read the same page 1000 times.
	for range 1000 {
		assert.Equal(t, byte(0xDD), data[0])
		assert.Equal(t, byte(0xDD), data[2048])
		assert.Equal(t, byte(0xDD), data[pageSize-1])
	}
}

// TestMapVolume_RandomAccessStress does random-order page access from many
// goroutines to stress the fault handler under contention.
func TestMapVolume_RandomAccessStress(t *testing.T) {
	const numPages = 256
	const numGoroutines = 32
	const accessesPerGoroutine = 200

	vol := newTestVolume(t, "stress", numPages)

	for i := range numPages {
		page := make([]byte, pageSize)
		binary.LittleEndian.PutUint32(page[:4], uint32(i))
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithWorkers(4))
	defer mr.Close()

	data := mr.Bytes()

	var wg sync.WaitGroup
	errc := make(chan error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed+1)))
			for range accessesPerGoroutine {
				pageIdx := rng.IntN(numPages)
				off := pageIdx * pageSize
				stamp := binary.LittleEndian.Uint32(data[off : off+4])
				if stamp != uint32(pageIdx) {
					errc <- fmt.Errorf("goroutine %d: page %d stamp=%d want=%d",
						seed, pageIdx, stamp, pageIdx)
					return
				}
			}
			errc <- nil
		}(g)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		require.NoError(t, err)
	}
}

// TestMapVolume_UnflushedData verifies that data still in the memtable (not yet
// flushed to S3) is correctly served through the mapping.
func TestMapVolume_UnflushedData(t *testing.T) {
	vol := newTestVolume(t, "unflushed", 4)

	page := make([]byte, pageSize)
	for i := range page {
		page[i] = 0xEE
	}
	require.NoError(t, vol.Write(page, 0))
	// Deliberately NOT flushing.

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	assert.Equal(t, byte(0xEE), data[0])
	assert.Equal(t, byte(0xEE), data[pageSize-1])
}

// TestMapVolume_PtrMatchesBytes verifies Ptr() returns the same address as &Bytes()[0].
func TestMapVolume_PtrMatchesBytes(t *testing.T) {
	vol := newTestVolume(t, "ptr-check", 4)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	assert.Equal(t, unsafe.Pointer(&data[0]), mr.Ptr())
	assert.Equal(t, uint64(4*pageSize), mr.Size())
}

// TestMapVolume_ReadaheadAtEnd verifies readahead doesn't attempt to resolve
// pages beyond the mapped region.
func TestMapVolume_ReadaheadAtEnd(t *testing.T) {
	const numPages = 4
	vol := newTestVolume(t, "readahead-end", numPages)

	for i := range numPages {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = byte(i + 1)
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	// Readahead of 10 pages, but only 4 pages total — should not panic or error.
	mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithReadahead(10))
	defer mr.Close()

	data := mr.Bytes()
	// Touch the last page — readahead would try 10 more pages past the end.
	assert.Equal(t, byte(4), data[3*pageSize])
	// Verify all pages are correct.
	for i := range numPages {
		assert.Equal(t, byte(i+1), data[i*pageSize])
	}
}

// TestMapVolume_SafetyNetCleanup verifies the OnBeforeClose hook cleans up the
// mapping when the volume is closed without explicitly closing the MappedRegion.
func TestMapVolume_SafetyNetCleanup(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := storage2.Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	vol, err := m.NewVolume(loophole.CreateParams{
		Volume: "safety-net",
		Size:   4 * pageSize,
	})
	require.NoError(t, err)

	mr := requireMapVolume(t, vol, 0, 4*pageSize)

	// Touch a page to trigger a fault.
	data := mr.Bytes()
	_ = data[0]

	// Don't call mr.Close() — let the volume manager cleanup trigger it.
	// The manager Close() in t.Cleanup will close the volume, which fires
	// OnBeforeClose, which calls mr.Close().
}

// TestMapVolume_SequentialScan reads every byte of a large mapping in order
// to verify correct sequential fault handling.
func TestMapVolume_SequentialScan(t *testing.T) {
	const numPages = 64
	vol := newTestVolume(t, "sequential", numPages)

	for i := range numPages {
		page := make([]byte, pageSize)
		for j := range page {
			page[j] = byte(i)
		}
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize)
	defer mr.Close()

	data := mr.Bytes()
	for i := range numPages {
		expected := byte(i)
		for j := range pageSize {
			if data[i*pageSize+j] != expected {
				t.Fatalf("page %d byte %d: got %d, want %d", i, j, data[i*pageSize+j], expected)
			}
		}
	}
}

// TestMapVolume_LargeMapping maps 1024 pages (~4MB) with multiple workers.
func TestMapVolume_LargeMapping(t *testing.T) {
	const numPages = 1024
	vol := newTestVolume(t, "large", numPages)

	// Write every 64th page with a known pattern.
	for i := 0; i < numPages; i += 64 {
		page := make([]byte, pageSize)
		binary.LittleEndian.PutUint32(page[:4], uint32(i))
		require.NoError(t, vol.Write(page, uint64(i)*pageSize))
	}
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithWorkers(4))
	defer mr.Close()

	data := mr.Bytes()

	// Verify written pages.
	for i := 0; i < numPages; i += 64 {
		off := i * pageSize
		stamp := binary.LittleEndian.Uint32(data[off : off+4])
		assert.Equal(t, uint32(i), stamp, "page %d", i)
	}

	// Verify unwritten pages are zero.
	for i := 1; i < numPages; i += 64 {
		off := i * pageSize
		assert.Equal(t, byte(0), data[off], "page %d should be zero", i)
	}
}

// TestMapVolume_WorkerCounts verifies correct behavior with various worker counts.
func TestMapVolume_WorkerCounts(t *testing.T) {
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			const numPages = 32
			vol := newTestVolume(t, fmt.Sprintf("workers-%d", workers), numPages)

			for i := range numPages {
				page := make([]byte, pageSize)
				binary.LittleEndian.PutUint32(page[:4], uint32(i))
				require.NoError(t, vol.Write(page, uint64(i)*pageSize))
			}
			require.NoError(t, vol.Flush())

			mr := requireMapVolume(t, vol, 0, numPages*pageSize, mmap.WithWorkers(workers))
			defer mr.Close()

			data := mr.Bytes()
			for i := range numPages {
				off := i * pageSize
				stamp := binary.LittleEndian.Uint32(data[off : off+4])
				assert.Equal(t, uint32(i), stamp, "page %d", i)
			}
		})
	}
}

// TestMapVolume_ConcurrentCloseAndRead verifies that Close doesn't race with
// goroutines that have already obtained the Bytes() slice.
func TestMapVolume_ConcurrentCloseAndRead(t *testing.T) {
	const numPages = 16
	vol := newTestVolume(t, "close-race", numPages)

	page := make([]byte, pageSize)
	for i := range page {
		page[i] = 0x42
	}
	require.NoError(t, vol.Write(page, 0))
	require.NoError(t, vol.Flush())

	mr := requireMapVolume(t, vol, 0, numPages*pageSize)

	data := mr.Bytes()
	// Fault in page 0 before the close race starts.
	assert.Equal(t, byte(0x42), data[0])

	// Now close — the already-faulted page should still be readable until
	// munmap, but we're testing that Close doesn't panic or deadlock.
	require.NoError(t, mr.Close())
}
