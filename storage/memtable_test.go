package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtableTombstoneNoSlotConsumed(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 1024)
	require.NoError(t, err)
	defer mt.cleanup()

	for i := range 100 {
		require.NoError(t, mt.putTombstone(PageIdx(i)))
	}

	assert.Equal(t, 0, mt.nextSlot)
	assert.Equal(t, int64(0), mt.size.Load())
	assert.Equal(t, 100, len(mt.index))

	for i := range 100 {
		slot, ok := mt.get(PageIdx(i))
		assert.True(t, ok)
		assert.Equal(t, tombstoneSlot, slot)
	}
}

func TestMemtableTombstoneOverwritesData(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 1024)
	require.NoError(t, err)
	defer mt.cleanup()

	data := bytes.Repeat([]byte{0xAB}, PageSize)
	require.NoError(t, mt.put(PageIdx(5), data))
	assert.Equal(t, 1, mt.nextSlot)
	assert.Equal(t, int64(PageSize), mt.size.Load())

	require.NoError(t, mt.putTombstone(PageIdx(5)))
	slot, ok := mt.get(PageIdx(5))
	assert.True(t, ok)
	assert.Equal(t, tombstoneSlot, slot)

	// Slot 0 is wasted but nextSlot didn't advance further.
	assert.Equal(t, 1, mt.nextSlot)
}

func TestMemtableTombstoneReadReturnsZeros(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 1024)
	require.NoError(t, err)
	defer mt.cleanup()

	require.NoError(t, mt.putTombstone(PageIdx(7)))

	data, err := mt.readData(tombstoneSlot)
	require.NoError(t, err)
	assert.Equal(t, zeroPage[:], data)

	sp := safepoint.New()
	g := sp.Enter()
	ref, err := mt.readDataRef(g, tombstoneSlot)
	require.NoError(t, err)
	assert.Equal(t, zeroPage[:], ref)
	g.Exit()
}

func TestMemtableTombstoneInEntries(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 1024)
	require.NoError(t, err)
	defer mt.cleanup()

	require.NoError(t, mt.putTombstone(PageIdx(3)))
	data := bytes.Repeat([]byte{0xCC}, PageSize)
	require.NoError(t, mt.put(PageIdx(5), data))
	require.NoError(t, mt.putTombstone(PageIdx(7)))

	ents := mt.entries()
	require.Len(t, ents, 3)
	assert.Equal(t, PageIdx(3), ents[0].pageIdx)
	assert.Equal(t, tombstoneSlot, ents[0].slot)
	assert.Equal(t, PageIdx(5), ents[1].pageIdx)
	assert.NotEqual(t, tombstoneSlot, ents[1].slot)
	assert.Equal(t, PageIdx(7), ents[2].pageIdx)
	assert.Equal(t, tombstoneSlot, ents[2].slot)
}

func TestMemtableTombstoneThenRealWrite(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 1024)
	require.NoError(t, err)
	defer mt.cleanup()

	require.NoError(t, mt.putTombstone(PageIdx(10)))
	assert.Equal(t, 0, mt.nextSlot)

	data := bytes.Repeat([]byte{0xDD}, PageSize)
	require.NoError(t, mt.put(PageIdx(10), data))

	assert.Equal(t, 1, mt.nextSlot)
	slot, ok := mt.get(PageIdx(10))
	assert.True(t, ok)
	assert.NotEqual(t, tombstoneSlot, slot)

	got, err := mt.readData(slot)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestMemtableTombstoneFullGuard(t *testing.T) {
	mt, err := newMemtable(t.TempDir(), 0, 10)
	require.NoError(t, err)
	defer mt.cleanup()

	for i := range 10 {
		require.NoError(t, mt.putTombstone(PageIdx(i)))
	}

	err = mt.putTombstone(PageIdx(10))
	assert.ErrorIs(t, err, errMemtableFull)
}
