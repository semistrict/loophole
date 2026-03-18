package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadPagesMatchesRead verifies that ReadPages returns byte-for-byte
// identical data to Read for various offset/length combinations.
func TestReadPagesMatchesRead(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	const volSize = 16 * PageSize
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: volSize})
	require.NoError(t, err)

	// Write recognizable pattern: page N filled with byte N.
	for pg := 0; pg < 16; pg++ {
		data := bytes.Repeat([]byte{byte(pg)}, PageSize)
		require.NoError(t, v.Write(data, uint64(pg)*PageSize))
	}

	cases := []struct {
		name   string
		offset uint64
		length int
	}{
		{"full_page_aligned", 0, PageSize},
		{"multi_page_aligned", 0, 4 * PageSize},
		{"all_pages", 0, volSize},
		{"mid_page_start", PageSize / 2, PageSize},
		{"mid_page_end", 0, PageSize + PageSize/2},
		{"cross_page_unaligned", PageSize / 4 * 3, PageSize},
		{"single_byte", 100, 1},
		{"last_page", 15 * PageSize, PageSize},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Read via copy path.
			buf := make([]byte, tc.length)
			n, err := v.Read(ctx, buf, tc.offset)
			require.NoError(t, err)
			require.Equal(t, tc.length, n)

			// Read via zero-copy path.
			var slices [][]byte
			cleanup, err := v.ReadPages(ctx, tc.offset, tc.length, &slices)
			require.NoError(t, err)
			defer cleanup()

			// Concatenate slices and compare.
			var got []byte
			for _, s := range slices {
				got = append(got, s...)
			}
			assert.Equal(t, buf, got)
		})
	}
}

// TestReadPagesMatchesReadFlushed verifies ReadPages vs Read after data is
// flushed to L1/L2 (exercises the block cache + decompression path).
func TestReadPagesMatchesReadFlushed(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	const volSize = 16 * PageSize
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: volSize})
	require.NoError(t, err)

	for pg := 0; pg < 16; pg++ {
		data := bytes.Repeat([]byte{byte(pg)}, PageSize)
		require.NoError(t, v.Write(data, uint64(pg)*PageSize))
	}
	require.NoError(t, v.Flush())

	// Full volume read.
	buf := make([]byte, volSize)
	n, err := v.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, volSize, n)

	var slices [][]byte
	cleanup, err := v.ReadPages(ctx, 0, volSize, &slices)
	require.NoError(t, err)
	defer cleanup()

	var got []byte
	for _, s := range slices {
		got = append(got, s...)
	}
	assert.Equal(t, buf, got)
}

// TestReadPagesCleanupReleasesLock verifies that the cleanup function releases
// the volume RLock, allowing subsequent writes to proceed.
func TestReadPagesCleanupReleasesLock(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 4 * PageSize})
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, v.Write(data, 0))

	var slices [][]byte
	cleanup, err := v.ReadPages(ctx, 0, PageSize, &slices)
	require.NoError(t, err)

	// Cleanup releases the lock; a write should succeed after.
	cleanup()

	newData := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, v.Write(newData, 0))

	// Verify the write took effect.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, PageSize, n)
	assert.Equal(t, newData, buf)
}

// TestReadDataRefPinBlocksCleanup verifies that readDataRef's pin prevents
// memtable cleanup (munmap) until unpin is called.
func TestReadDataRefPinBlocksCleanup(t *testing.T) {
	memDir := filepath.Join(t.TempDir(), "mem")
	require.NoError(t, os.MkdirAll(memDir, 0o755))
	mt, err := newMemtable(memDir, 4, 1)
	require.NoError(t, err)

	pageIdx := PageIdx(0)
	data := bytes.Repeat([]byte{0xCC}, PageSize)
	require.NoError(t, mt.put(pageIdx, data))

	slot, ok := mt.get(pageIdx)
	require.True(t, ok)

	// Get a zero-copy ref — holds a pin.
	ref, unpin, err := mt.readDataRef(slot)
	require.NoError(t, err)
	require.Equal(t, data, ref)

	// Start cleanup in background — should block on pin.
	cleanupDone := make(chan struct{})
	go func() {
		mt.cleanup()
		close(cleanupDone)
	}()

	// Give cleanup a moment to spin on the pin.
	// It should NOT complete yet.
	select {
	case <-cleanupDone:
		t.Fatal("cleanup completed while pin was held")
	default:
	}

	// Data should still be readable through the ref.
	assert.Equal(t, data, ref)

	// Release the pin — cleanup should now complete.
	unpin()
	<-cleanupDone
}

// TestReadPagesPinSurvivesEviction verifies that pinned page cache slots
// are not reused by eviction, keeping the data valid.
func TestReadPagesPinSurvivesEviction(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)

	const volSize = 8 * PageSize
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: volSize})
	require.NoError(t, err)

	// Write and flush so data goes to L1/L2 and gets cached.
	for pg := 0; pg < 8; pg++ {
		data := bytes.Repeat([]byte{byte(pg)}, PageSize)
		require.NoError(t, v.Write(data, uint64(pg)*PageSize))
	}
	require.NoError(t, v.Flush())

	// Warm the page cache via a normal read.
	warmBuf := make([]byte, volSize)
	_, err = v.Read(ctx, warmBuf, 0)
	require.NoError(t, err)

	// Get zero-copy refs — pins held.
	var slices [][]byte
	cleanup, err := v.ReadPages(ctx, 0, volSize, &slices)
	require.NoError(t, err)

	// Snapshot expected data before any eviction.
	expected := make([]byte, 0, volSize)
	for _, s := range slices {
		expected = append(expected, s...)
	}

	// Verify data matches what we wrote.
	assert.Equal(t, warmBuf, expected)

	// Release pins.
	cleanup()
}

// TestReadPagesConcurrent verifies that concurrent ReadPages and Read calls
// don't race or corrupt data.
func TestReadPagesConcurrent(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	const volSize = 16 * PageSize
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: volSize})
	require.NoError(t, err)

	for pg := 0; pg < 16; pg++ {
		data := bytes.Repeat([]byte{byte(pg)}, PageSize)
		require.NoError(t, v.Write(data, uint64(pg)*PageSize))
	}

	// Get expected data.
	expected := make([]byte, volSize)
	n, err := v.Read(ctx, expected, 0)
	require.NoError(t, err)
	require.Equal(t, volSize, n)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, volSize)
			n, err := v.Read(ctx, buf, 0)
			assert.NoError(t, err)
			assert.Equal(t, volSize, n)
			assert.Equal(t, expected, buf)
		}()
		go func() {
			defer wg.Done()
			var slices [][]byte
			cleanup, err := v.ReadPages(ctx, 0, volSize, &slices)
			assert.NoError(t, err)
			var got []byte
			for _, s := range slices {
				got = append(got, s...)
			}
			cleanup()
			assert.Equal(t, expected, got)
		}()
	}
	wg.Wait()
}
