package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testConfig = Config{
	FlushThreshold: 16 * PageSize,
	FlushInterval:  -1,
}

func requireBytesEqualWithPageDebug(t *testing.T, ly *layer, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	pageCount := (max(len(got), len(want)) + PageSize - 1) / PageSize
	for i := 0; i < pageCount; i++ {
		start := i * PageSize
		gotEnd := min(start+PageSize, len(got))
		wantEnd := min(start+PageSize, len(want))
		if start >= len(got) || start >= len(want) || !bytes.Equal(got[start:gotEnd], want[start:wantEnd]) {
			t.Logf("first mismatching page=%d got=%x want=%x", i, got[start:min(start+32, len(got))], want[start:min(start+32, len(want))])
			t.Log(ly.DebugPage(t.Context(), PageIdx(i)))
			t.Fatalf("data mismatch at page %d", i)
		}
	}
	t.Fatal("data mismatch")
}

func formatTestStore(t *testing.T, store *blob.Store) {
	t.Helper()
	_, _, err := FormatVolumeSet(context.Background(), store)
	require.NoError(t, err)
}

func newTestManager(t *testing.T, store *blob.Store, config Config) *Manager {
	t.Helper()
	formatTestStore(t, store)
	if config.FlushInterval == 0 {
		config.FlushInterval = -1
	}
	m := &Manager{
		BlobStore: store,
		config:    config,
	}
	t.Cleanup(func() {
		_ = m.Close()
	})
	return m
}

func TestLayerReadWrite(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		// Keep the mixed-write test focused on direct-L2 eligibility rather than
		// per-page background flush interleavings.
		FlushThreshold: 2 * BlockSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write some data.
	data := bytes.Repeat([]byte("hello world!"), 400)
	require.NoError(t, ly.Write(data, 0))

	// Read it back.
	buf := make([]byte, len(data))
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)

	// Read at an offset.
	buf2 := make([]byte, 100)
	_, err = ly.Read(ctx, buf2, 100)
	require.NoError(t, err)
	assert.Equal(t, data[100:200], buf2)
}

func TestLayerFlushAndRead(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 2 * BlockSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write a full page.
	page := make([]byte, PageSize)
	for i := range page {
		page[i] = byte(i % 251)
	}
	require.NoError(t, ly.Write(page, 0))
	require.NoError(t, ly.Flush())

	// Read back — should come from flushed L1 data.
	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page, buf)
}

func TestLayerPunchHole(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 2 * BlockSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write 2 pages.
	data := bytes.Repeat([]byte{0xFF}, 2*PageSize)
	require.NoError(t, ly.Write(data, 0))

	// Punch hole over the first page.
	require.NoError(t, ly.PunchHole(0, PageSize))

	// First page should be zeros.
	buf := make([]byte, PageSize)
	_, err = ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, zeroPage[:], buf)

	// Second page should still be 0xFF.
	_, err = ly.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{0xFF}, PageSize), buf)
}

func TestLayerFlushAndReadPages(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write pages and flush.
	pages := 20
	expected := make(map[uint64][]byte)
	for i := range pages {
		page := make([]byte, PageSize)
		for j := range page {
			page[j] = byte((i + j) % 256)
		}
		pageAddr := uint64(i)
		require.NoError(t, ly.Write(page, pageAddr*PageSize))
		expected[pageAddr] = page
	}

	require.NoError(t, ly.Flush())

	// Verify all pages still readable after flush.
	for pageAddr, exp := range expected {
		buf := make([]byte, PageSize)
		n, err := ly.Read(ctx, buf, pageAddr*PageSize)
		require.NoError(t, err)
		assert.Equal(t, PageSize, n)
		assert.Equal(t, exp, buf, "page %d data mismatch", pageAddr)
	}
}

func TestLayerSnapshot(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	parent, err := openLayer(ctx, layerParams{store: store, id: "parent", config: cfg})
	require.NoError(t, err)
	defer parent.Close()

	// Write data to parent.
	page := make([]byte, PageSize)
	for i := range page {
		page[i] = byte(i % 200)
	}
	require.NoError(t, parent.Write(page, 0))

	// Snapshot → child.
	require.NoError(t, parent.Snapshot("child"))

	// Open child layer and verify it can read the data.
	child, err := openLayer(ctx, layerParams{store: store, id: "child", config: cfg})
	require.NoError(t, err)
	defer child.Close()

	buf := make([]byte, PageSize)
	n, err := child.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page, buf)

	// Write new data to child — should not affect parent.
	newPage := bytes.Repeat([]byte{0xAB}, PageSize)
	require.NoError(t, child.Write(newPage, 0))

	// Reopen parent to verify data persisted.
	parent2, err := openLayer(ctx, layerParams{store: store, id: "parent", config: cfg})
	require.NoError(t, err)
	defer parent2.Close()

	n, err = parent2.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, page, buf[:n])
}

func TestLayerMixedWrite(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	leadingBytes := PageSize / 2
	data := make([]byte, leadingBytes+BlockSize+PageSize)
	for i := range data {
		data[i] = byte((i * 13) % 256)
	}
	require.NoError(t, ly.Write(data[:leadingBytes], 0))
	require.NoError(t, ly.Write(data[leadingBytes:], uint64(leadingBytes)))

	buf := make([]byte, len(data))
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	requireBytesEqualWithPageDebug(t, ly, buf, data)
}

func TestLayerMixedWriteAfterExplicitFlush(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	leadingBytes := PageSize / 2
	data := make([]byte, leadingBytes+BlockSize+PageSize)
	for i := range data {
		data[i] = byte((i * 13) % 256)
	}
	require.NoError(t, ly.Write(data[:leadingBytes], 0))
	require.NoError(t, ly.Flush())
	require.NoError(t, ly.Write(data[leadingBytes:], uint64(leadingBytes)))

	buf := make([]byte, len(data))
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	requireBytesEqualWithPageDebug(t, ly, buf, data)
}

func TestBlockRoundTrip(t *testing.T) {
	ctx := t.Context()

	var pages []blockPage
	for i := range 10 {
		data := make([]byte, PageSize)
		for j := range data {
			data[j] = byte((i*17 + j) % 256)
		}
		pages = append(pages, blockPage{offset: uint16(i * 3), data: data})
	}

	blob, err := buildBlock(42, pages, false)
	require.NoError(t, err)

	pb, err := parseBlock(blob, false)
	require.NoError(t, err)

	for _, p := range pages {
		pageIdx := BlockIdx(42).PageIdx(p.offset)
		data, found, err := pb.findPage(ctx, pageIdx)
		require.NoError(t, err)
		assert.True(t, found, "page %d not found", pageIdx)
		assert.Equal(t, p.data, data, "page %d data mismatch", pageIdx)
	}

	// Non-existent page.
	_, found, err := pb.findPage(ctx, 42*BlockPages+999)
	require.NoError(t, err)
	assert.False(t, found)
}
