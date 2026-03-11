package storage2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockRangeMapSplitPreservesWriteLeaseSeq(t *testing.T) {
	// Start with a contiguous range [0, 10) with writeLeaseSeq=5.
	m := newBlockRangeMap([]blockRange{
		{Start: 0, End: 10, Layer: "layer-A", WriteLeaseSeq: 5},
	})

	// Remove block 4 — this splits the range into [0,4) and [5,10).
	// Both halves must retain WriteLeaseSeq=5.
	m = m.Remove(4)

	layer, seq := m.Find(0)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(5), seq, "left half must retain WriteLeaseSeq")

	layer, seq = m.Find(5)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(5), seq, "right half must retain WriteLeaseSeq")

	layer, _ = m.Find(4)
	assert.Equal(t, "", layer, "removed block should not be found")
}

func TestBlockRangeMapSetSplitPreservesWriteLeaseSeq(t *testing.T) {
	// Range [0, 10) owned by layer-A with seq=3.
	m := newBlockRangeMap([]blockRange{
		{Start: 0, End: 10, Layer: "layer-A", WriteLeaseSeq: 3},
	})

	// Set block 5 to a different layer — this removes block 5 from
	// the original range (splitting it), then inserts the new mapping.
	m = m.Set(5, "layer-B", 7)

	// Block 5 should now be layer-B with seq=7.
	layer, seq := m.Find(5)
	assert.Equal(t, "layer-B", layer)
	assert.Equal(t, uint64(7), seq)

	// Block 3 (left of split) must still be layer-A with seq=3.
	layer, seq = m.Find(3)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(3), seq, "left split must retain original WriteLeaseSeq")

	// Block 7 (right of split) must still be layer-A with seq=3.
	layer, seq = m.Find(7)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(3), seq, "right split must retain original WriteLeaseSeq")
}

func TestBlockRangeMapTrimPreservesWriteLeaseSeq(t *testing.T) {
	m := newBlockRangeMap([]blockRange{
		{Start: 0, End: 5, Layer: "layer-A", WriteLeaseSeq: 2},
	})

	// Remove the first block — trims start.
	m = m.Remove(0)
	layer, seq := m.Find(1)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(2), seq, "trimmed range must retain WriteLeaseSeq")

	// Remove the last block — trims end.
	m = m.Remove(4)
	layer, seq = m.Find(3)
	assert.Equal(t, "layer-A", layer)
	assert.Equal(t, uint64(2), seq, "trimmed range must retain WriteLeaseSeq")
}
