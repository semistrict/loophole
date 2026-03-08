package storage2

import (
	"sort"
)

// layerIndex is the on-disk and in-memory representation of a layer's state.
// Stored as index.json per layer.
type layerIndex struct {
	NextSeq uint64       `json:"next_seq"`
	L0      []l0Entry    `json:"l0,omitempty"`
	L1      []blockRange `json:"l1,omitempty"`
	L2      []blockRange `json:"l2,omitempty"`
}

// l0Entry describes a single L0 flush file and the pages it contains.
type l0Entry struct {
	Key        string    `json:"key"`
	Pages      []PageIdx `json:"pages"`
	Tombstones []PageIdx `json:"tombstones,omitempty"`
	Size       int64     `json:"size"`
}

// blockRange maps a contiguous range of block indices [Start, End) to
// the layer that owns the blobs. The blob key for block index N in
// layer L is "layers/{L}/l1/{N}" or "layers/{L}/l2/{N}".
type blockRange struct {
	Start BlockIdx `json:"start"` // inclusive
	End   BlockIdx `json:"end"`   // exclusive
	Layer string   `json:"layer"` // layer ID that owns the blobs
}

// l0HasPage checks if an L0 entry contains the given page.
func l0HasPage(e *l0Entry, pageIdx PageIdx) bool {
	for _, p := range e.Pages {
		if p == pageIdx {
			return true
		}
	}
	return false
}

// l0HasTombstone checks if an L0 entry has a tombstone for the given page.
func l0HasTombstone(e *l0Entry, pageIdx PageIdx) bool {
	for _, t := range e.Tombstones {
		if t == pageIdx {
			return true
		}
	}
	return false
}

// totalL0Pages returns the total number of page entries across all L0 files.
func totalL0Pages(entries []l0Entry) int {
	n := 0
	for i := range entries {
		n += len(entries[i].Pages) + len(entries[i].Tombstones)
	}
	return n
}

// blockRangeMap provides O(log n) lookup of block address → layer ID.
type blockRangeMap struct {
	ranges []blockRange // sorted by Start
}

// newBlockRangeMap creates a blockRangeMap from sorted ranges.
func newBlockRangeMap(ranges []blockRange) *blockRangeMap {
	sorted := make([]blockRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	return &blockRangeMap{ranges: sorted}
}

// Find returns the layer ID for the given block index, or "" if not found.
func (m *blockRangeMap) Find(block BlockIdx) string {
	if m == nil || len(m.ranges) == 0 {
		return ""
	}
	i := sort.Search(len(m.ranges), func(i int) bool {
		return m.ranges[i].End > block
	})
	if i < len(m.ranges) && m.ranges[i].Start <= block {
		return m.ranges[i].Layer
	}
	return ""
}

// Set returns a new blockRangeMap with block mapped to layerID.
// The original map is not modified (copy-on-write for snapshot safety).
func (m *blockRangeMap) Set(block BlockIdx, layerID string) *blockRangeMap {
	return &blockRangeMap{ranges: setBlockRange(m.ranges, block, layerID)}
}

// Remove returns a new blockRangeMap with block removed.
// The original map is not modified (copy-on-write for snapshot safety).
func (m *blockRangeMap) Remove(block BlockIdx) *blockRangeMap {
	return &blockRangeMap{ranges: removeBlockAddr(m.ranges, block)}
}

// Ranges returns the underlying sorted ranges (for serialization).
func (m *blockRangeMap) Ranges() []blockRange {
	if m == nil {
		return nil
	}
	return m.ranges
}

// setBlockRange inserts or updates a single block address in sorted ranges.
func setBlockRange(ranges []blockRange, blockAddr BlockIdx, layerID string) []blockRange {
	// Remove old mapping first (if any).
	ranges = removeBlockAddr(ranges, blockAddr)

	// Find insertion point.
	i := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].Start > blockAddr
	})

	// Try to merge with adjacent ranges.
	canMergePrev := i > 0 && ranges[i-1].End == blockAddr && ranges[i-1].Layer == layerID
	canMergeNext := i < len(ranges) && ranges[i].Start == blockAddr+1 && ranges[i].Layer == layerID

	switch {
	case canMergePrev && canMergeNext:
		// Merge prev + new + next into one range.
		ranges[i-1].End = ranges[i].End
		ranges = append(ranges[:i], ranges[i+1:]...)
	case canMergePrev:
		ranges[i-1].End = blockAddr + 1
	case canMergeNext:
		ranges[i].Start = blockAddr
	default:
		// Insert new single-block range.
		newRange := blockRange{Start: blockAddr, End: blockAddr + 1, Layer: layerID}
		ranges = append(ranges, blockRange{})
		copy(ranges[i+1:], ranges[i:])
		ranges[i] = newRange
	}

	return ranges
}

// removeBlockAddr removes a single block address from sorted ranges,
// potentially splitting a range.
func removeBlockAddr(ranges []blockRange, blockAddr BlockIdx) []blockRange {
	i := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].End > blockAddr
	})
	if i >= len(ranges) || ranges[i].Start > blockAddr {
		return ranges // not found
	}

	r := ranges[i]
	switch {
	case r.Start == blockAddr && r.End == blockAddr+1:
		// Exact single-block range — remove it.
		return append(ranges[:i], ranges[i+1:]...)
	case r.Start == blockAddr:
		// Trim start.
		ranges[i].Start = blockAddr + 1
	case r.End == blockAddr+1:
		// Trim end.
		ranges[i].End = blockAddr
	default:
		// Split range into two.
		left := blockRange{Start: r.Start, End: blockAddr, Layer: r.Layer}
		right := blockRange{Start: blockAddr + 1, End: r.End, Layer: r.Layer}
		ranges[i] = left
		ranges = append(ranges[:i+1], append([]blockRange{right}, ranges[i+1:]...)...)
	}
	return ranges
}
