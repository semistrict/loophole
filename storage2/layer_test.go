package storage2

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testConfig = Config{
	FlushThreshold:  16 * PageSize,
	MaxFrozenTables: 2,
	FlushInterval:   -1,
}

func newTestManager(t *testing.T, store loophole.ObjectStore, config Config) *Manager {
	t.Helper()
	if config.FlushInterval == 0 {
		config.FlushInterval = -1
	}
	cacheDir := t.TempDir()
	dc, err := NewPageCache(filepath.Join(cacheDir, "diskcache"))
	if err != nil {
		t.Fatalf("create page cache: %v", err)
	}
	m := NewVolumeManager(store, cacheDir, config, nil, dc)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
		_ = dc.Close()
	})
	return m
}

func TestLayerReadWrite(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
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

func TestLayerFlushAndReadFromL0(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write a full page.
	page := make([]byte, PageSize)
	for i := range page {
		page[i] = byte(i % 251)
	}
	require.NoError(t, ly.Write(page, 0))
	require.NoError(t, ly.Flush())

	// Read back — should come from L0 now.
	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page, buf)
}

func TestLayerPunchHole(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
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

func TestLayerCompactL0(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  4 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	ly, err := openLayer(ctx, store, "test-layer", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer ly.Close()

	// Write pages and flush to L0.
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

	ly.mu.RLock()
	l0Count := len(ly.index.L0)
	ly.mu.RUnlock()
	assert.Greater(t, l0Count, 0, "expected L0 entries after flush")

	// CompactL0 may skip if below trigger threshold.
	_ = ly.CompactL0(ctx)

	// Verify all pages still readable.
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
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  64 * 1024,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	parent, err := openLayer(ctx, store, "parent", cfg, nil, t.TempDir())
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
	child, err := openLayer(ctx, store, "child", cfg, nil, t.TempDir())
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

	// Reopen parent to verify L0 data persisted.
	parent2, err := openLayer(ctx, store, "parent", cfg, nil, t.TempDir())
	require.NoError(t, err)
	defer parent2.Close()

	n, err = parent2.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, page, buf[:n])
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

	blob, err := buildBlock(42, pages)
	require.NoError(t, err)

	pb, err := parseBlock(blob)
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
