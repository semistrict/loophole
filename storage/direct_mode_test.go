package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeDirectWritebackRejectsNormalWrite(t *testing.T) {
	m := newTestManager(t, objstore.NewMemStore(), Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	})
	vol, err := m.NewVolume(CreateParams{
		Volume: "direct-mode-rejects-normal-write",
		Size:   16 * PageSize,
	})
	require.NoError(t, err)

	require.NoError(t, vol.EnableDirectWriteback())

	err = vol.Write(bytes.Repeat([]byte{0xAB}, PageSize), 0)
	require.Error(t, err)
	assert.ErrorContains(t, err, "direct writeback mode")

	require.NoError(t, vol.DisableDirectWriteback())
	require.NoError(t, vol.Write(bytes.Repeat([]byte{0xCD}, PageSize), 0))
}

func TestDirectFlush(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-direct-flush", config: cfg, workDir: t.TempDir()})
	require.NoError(t, err)
	defer ly.Close()

	// Write pages across two different blocks (block 0 and block 1).
	page0 := make([]byte, PageSize)
	for i := range page0 {
		page0[i] = byte(i % 251)
	}
	page1 := make([]byte, PageSize)
	for i := range page1 {
		page1[i] = byte((i + 37) % 251)
	}

	require.NoError(t, ly.Write(page0, 0))
	require.NoError(t, ly.Write(page1, BlockPages*PageSize)) // first page of block 1
	require.NoError(t, ly.Flush())

	// Verify data is readable.
	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page0, buf)

	buf2 := make([]byte, PageSize)
	n, err = ly.Read(ctx, buf2, BlockPages*PageSize)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page1, buf2)

	// Verify L1 map is populated.
	ly.mu.RLock()
	l1Ranges := ly.l1Map.Ranges()
	ly.mu.RUnlock()

	assert.NotEmpty(t, l1Ranges, "flush should populate L1 map")
}

func TestDirectFlushMultipleFlushes(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-direct-multi", config: cfg, workDir: t.TempDir()})
	require.NoError(t, err)
	defer ly.Close()

	// First write + flush.
	page0v1 := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, ly.Write(page0v1, 0))
	require.NoError(t, ly.Flush())

	// Second write to same page + flush (tests read-modify-write merge).
	page0v2 := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, ly.Write(page0v2, 0))
	require.NoError(t, ly.Flush())

	// Should read the latest value.
	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, PageSize, n)
	assert.Equal(t, page0v2, buf)
}

func TestDirectFlushL2Promotion(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold:   int64(L1PromoteThreshold) * PageSize,
		FlushInterval:    -1,
		MaxMemtableSlots: L1PromoteThreshold,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "test-direct-promote", config: cfg, workDir: t.TempDir()})
	require.NoError(t, err)
	defer ly.Close()

	// Write enough pages to a single block to trigger L2 promotion.
	for i := range L1PromoteThreshold {
		page := bytes.Repeat([]byte{byte(i)}, PageSize)
		require.NoError(t, ly.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, ly.Flush())

	// Verify L2 is populated.
	ly.mu.RLock()
	l2Ranges := ly.l2Map.Ranges()
	ly.mu.RUnlock()

	assert.NotEmpty(t, l2Ranges, "should promote to L2 when page count >= L1PromoteThreshold")

	// Verify all pages readable.
	for i := range L1PromoteThreshold {
		buf := make([]byte, PageSize)
		_, err := ly.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err)
		expected := bytes.Repeat([]byte{byte(i)}, PageSize)
		assert.Equal(t, expected, buf, "page %d mismatch", i)
	}
}

func TestVolumeDirectWritebackFlushesExistingMemtableBeforeEntering(t *testing.T) {
	ctx := t.Context()
	m := newTestManager(t, objstore.NewMemStore(), Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  -1,
	})
	vol, err := m.NewVolume(CreateParams{
		Volume: "direct-mode-flushes-existing-memtable",
		Size:   16 * PageSize,
	})
	require.NoError(t, err)

	page0 := bytes.Repeat([]byte{0x11}, PageSize)
	require.NoError(t, vol.Write(page0, 0))

	require.NoError(t, vol.EnableDirectWriteback())
	defer func() {
		require.NoError(t, vol.DisableDirectWriteback())
	}()

	page1 := bytes.Repeat([]byte{0x22}, PageSize)
	require.NoError(t, vol.WritePagesDirect([]DirectPage{{
		Offset: PageSize,
		Data:   page1,
	}}))

	buf := make([]byte, 2*PageSize)
	_, err = vol.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, page0, buf[:PageSize])
	assert.Equal(t, page1, buf[PageSize:2*PageSize])
}
