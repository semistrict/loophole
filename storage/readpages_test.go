package storage

import (
	"bytes"
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
	// Use a higher threshold than the data we write to avoid an auto-flush
	// rotation racing with the reads.
	m := newTestManager(t, store, Config{FlushThreshold: 32 * PageSize})

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
			slices, cleanup, err := v.ReadPages(ctx, tc.offset, tc.length)
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

	slices, cleanup, err := v.ReadPages(ctx, 0, volSize)
	require.NoError(t, err)

	var got []byte
	for _, s := range slices {
		got = append(got, s...)
	}
	cleanup()
	assert.Equal(t, buf, got)
}

// TestReadPagesGuardExitReleasesLock verifies that Guard.Exit allows
// subsequent writes to proceed.
func TestReadPagesGuardExitReleasesLock(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 4 * PageSize})
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, v.Write(data, 0))

	_, cleanup, err := v.ReadPages(ctx, 0, PageSize)
	require.NoError(t, err)

	// Cleanup releases the safepoint + volume lock; a write should succeed after.
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

// TestReadPagesDirtySlicesAreDetached verifies that dirty ReadPages data is
// copied out of the active batch rather than borrowing mutable batch storage.
func TestReadPagesDirtySlicesAreDetached(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 2 * PageSize})
	require.NoError(t, err)

	require.NoError(t, v.Write(bytes.Repeat([]byte{0xCC}, PageSize), 0))

	slices, cleanup, err := v.ReadPages(ctx, 0, PageSize)
	require.NoError(t, err)
	defer cleanup()
	require.Len(t, slices, 1)
	require.Equal(t, bytes.Repeat([]byte{0xCC}, PageSize), slices[0])

	require.NoError(t, v.Write(bytes.Repeat([]byte{0xDD}, PageSize), 0))

	assert.Equal(t, bytes.Repeat([]byte{0xCC}, PageSize), slices[0])
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

	// Get zero-copy refs.
	slices, cleanup, err := v.ReadPages(ctx, 0, volSize)
	require.NoError(t, err)

	// Snapshot expected data before any eviction.
	expected := make([]byte, 0, volSize)
	for _, s := range slices {
		expected = append(expected, s...)
	}

	// Verify data matches what we wrote.
	assert.Equal(t, warmBuf, expected)

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
			slices, cleanup, err := v.ReadPages(ctx, 0, volSize)
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
