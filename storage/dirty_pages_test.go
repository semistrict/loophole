package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDirtyBatch(maxEntries int) *dirtyBatch {
	return newDirtyBatch(Config{
		FlushThreshold:    1 << 20,
		FlushInterval:     -1,
		MaxDirtyPageSlots: maxEntries,
	})
}

func filledPage(b byte) Page {
	var page Page
	for i := range page {
		page[i] = b
	}
	return page
}

func TestDirtyBatchTombstoneConsumesNoBytes(t *testing.T) {
	mt := testDirtyBatch(1024)

	for i := range 100 {
		require.NoError(t, mt.stageTombstone(PageIdx(i)))
	}

	assert.Equal(t, int64(0), mt.bytes())
	assert.Equal(t, 100, mt.pages())

	for i := range 100 {
		rec, ok := mt.lookup(PageIdx(i))
		assert.True(t, ok)
		assert.True(t, rec.tombstone)
	}
}

func TestDirtyBatchTombstoneOverwritesData(t *testing.T) {
	mt := testDirtyBatch(1024)
	require.NoError(t, mt.stagePage(PageIdx(5), filledPage(0xAB)))
	assert.Equal(t, int64(PageSize), mt.bytes())

	require.NoError(t, mt.stageTombstone(PageIdx(5)))
	rec, ok := mt.lookup(PageIdx(5))
	require.True(t, ok)
	assert.True(t, rec.tombstone)
	assert.Equal(t, int64(0), mt.bytes())
}

func TestDirtyBatchTombstoneReadReturnsZeros(t *testing.T) {
	mt := testDirtyBatch(1024)
	require.NoError(t, mt.stageTombstone(PageIdx(7)))

	rec, ok := mt.lookup(PageIdx(7))
	require.True(t, ok)
	assert.Equal(t, zeroPage[:], rec.bytes())
}

func TestDirtyBatchEntriesAreSorted(t *testing.T) {
	mt := testDirtyBatch(1024)
	require.NoError(t, mt.stageTombstone(PageIdx(3)))
	require.NoError(t, mt.stagePage(PageIdx(5), filledPage(0xCC)))
	require.NoError(t, mt.stageTombstone(PageIdx(7)))

	ents := mt.entries()
	require.Len(t, ents, 3)
	assert.Equal(t, PageIdx(3), ents[0].pageIdx)
	assert.True(t, ents[0].record.tombstone)
	assert.Equal(t, PageIdx(5), ents[1].pageIdx)
	assert.False(t, ents[1].record.tombstone)
	assert.Equal(t, PageIdx(7), ents[2].pageIdx)
	assert.True(t, ents[2].record.tombstone)
}

func TestDirtyBatchTombstoneThenRealWrite(t *testing.T) {
	mt := testDirtyBatch(1024)
	require.NoError(t, mt.stageTombstone(PageIdx(10)))
	require.NoError(t, mt.stagePage(PageIdx(10), filledPage(0xDD)))

	rec, ok := mt.lookup(PageIdx(10))
	require.True(t, ok)
	assert.False(t, rec.tombstone)
	assert.Equal(t, bytes.Repeat([]byte{0xDD}, PageSize), rec.bytes())
}

func TestDirtyBatchEntryCap(t *testing.T) {
	mt := testDirtyBatch(10)
	for i := range 10 {
		require.NoError(t, mt.stageTombstone(PageIdx(i)))
	}
	err := mt.stageTombstone(PageIdx(10))
	assert.ErrorIs(t, err, errDirtyBatchFull)
}

func TestDirtyBatchClosedRejectsWrites(t *testing.T) {
	mt := testDirtyBatch(8)
	mt.markClosed()
	err := mt.stagePage(0, filledPage(0xAA))
	assert.ErrorIs(t, err, errDirtyBatchClosed)
}

func TestDirtyBatchRejectedPageReturnsBufferToPool(t *testing.T) {
	mt := testDirtyBatch(1)
	require.NoError(t, mt.stagePage(0, filledPage(0x11)))

	rec := newPageRecord(filledPage(0x22))
	require.NotNil(t, rec.page)

	_, err := mt.stageRecord(1, rec)
	require.ErrorIs(t, err, errDirtyBatchFull)
	assert.Nil(t, rec.page, "rejected page buffer should be returned to the pool")

	mt.markClosed()
	rec = newPageRecord(filledPage(0x33))
	require.NotNil(t, rec.page)

	_, err = mt.stageRecord(2, rec)
	require.ErrorIs(t, err, errDirtyBatchClosed)
	assert.Nil(t, rec.page, "closed-batch rejection should return the staged page buffer to the pool")
}
