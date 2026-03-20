package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testConfig = Config{
	FlushThreshold: 16 * PageSize,
	FlushInterval:  -1,
}

func newTestManager(t *testing.T, store objstore.ObjectStore, config Config) *Manager {
	t.Helper()
	return newTestManagerWithCache(t, store, config, nil)
}

func newTestManagerWithCache(t *testing.T, store objstore.ObjectStore, config Config, dc PageCache) *Manager {
	t.Helper()
	if config.FlushInterval == 0 {
		config.FlushInterval = -1
	}
	m := &Manager{
		ObjectStore: store,
		config:      config,
		diskCache:   dc,
	}
	t.Cleanup(func() {
		_ = m.Close()
	})
	return m
}

func TestLayerReadWrite(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
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
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
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
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
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
	store := objstore.NewMemStore()

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
	store := objstore.NewMemStore()

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

func TestLayerDirectL2Write(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Fresh layer should allow direct L2 writes.
	require.True(t, ly.canDirectL2())

	// Write exactly 3 full blocks (3 * 4MB = 12MB) at offset 0.
	// This should take the direct L2 path for all 3 blocks.
	numBlocks := 3
	data := make([]byte, numBlocks*BlockSize)
	for i := range data {
		data[i] = byte((i * 7) % 256)
	}
	require.NoError(t, ly.Write(data, 0))

	// The layer should still have no L1 entries — everything went
	// straight to L2.
	ly.mu.RLock()
	l1Count := ly.l1Map.Len()
	ly.mu.RUnlock()
	assert.Equal(t, 0, l1Count, "expected no L1 entries")

	// Verify each block index is present in the L2 map.
	for blk := range numBlocks {
		ly.mu.RLock()
		layerID, _ := ly.l2Map.Find(BlockIdx(blk))
		ly.mu.RUnlock()
		assert.NotEmpty(t, layerID, "block %d missing from L2 map", blk)
	}

	// Read back every block and verify byte-for-byte.
	buf := make([]byte, len(data))
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)

	// Verify individual page reads at block boundaries.
	pageBuf := make([]byte, PageSize)
	for blk := range numBlocks {
		off := uint64(blk * BlockSize)
		_, err := ly.Read(ctx, pageBuf, off)
		require.NoError(t, err)
		assert.Equal(t, data[off:off+PageSize], pageBuf, "block %d first page mismatch", blk)

		// Last page of the block.
		lastPageOff := off + uint64((BlockPages-1)*PageSize)
		_, err = ly.Read(ctx, pageBuf, lastPageOff)
		require.NoError(t, err)
		assert.Equal(t, data[lastPageOff:lastPageOff+PageSize], pageBuf, "block %d last page mismatch", blk)
	}
}

func TestLayerDirectL2ThenNormalWrite(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write one full block via direct L2.
	blockData := make([]byte, BlockSize)
	for i := range blockData {
		blockData[i] = byte(i % 251)
	}
	require.NoError(t, ly.Write(blockData, 0))

	// Now write a small amount — this goes through the memtable path since
	// the layer is no longer "fresh" (l2Map is non-empty, but canDirectL2
	// checks memtable.size which is 0, and l0/l1 are empty, so it would
	// still try direct L2 for aligned writes). Write a non-aligned small
	// chunk to force the memtable path.
	extra := bytes.Repeat([]byte{0xAA}, 100)
	extraOff := uint64(BlockSize) // start of second block, but only 100 bytes
	require.NoError(t, ly.Write(extra, extraOff))

	// Read back the first block.
	buf := make([]byte, BlockSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, BlockSize, n)
	assert.Equal(t, blockData, buf)

	// Read back the partial write.
	buf2 := make([]byte, 100)
	_, err = ly.Read(ctx, buf2, extraOff)
	require.NoError(t, err)
	assert.Equal(t, extra, buf2)
}

func TestLayerDirectL2MixedWrite(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 64 * 1024,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-layer", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write data that spans: partial block + full block + partial block.
	// The full block in the middle should NOT go through direct L2 because
	// the leading partial write will populate the memtable first, making
	// canDirectL2() return false.
	leadingBytes := PageSize / 2 // half a page
	data := make([]byte, leadingBytes+BlockSize+PageSize)
	for i := range data {
		data[i] = byte((i * 13) % 256)
	}
	require.NoError(t, ly.Write(data, 0))

	// Read it all back.
	buf := make([]byte, len(data))
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
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
