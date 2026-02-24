package loophole

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NewLayer / block ownership map tests ---

func TestNewLayerLoadsState(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	assert.False(t, layer.Frozen())
}

func TestNewLayerFrozen(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "frozen-1", frozenLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "frozen-1")
	require.NoError(t, err)
	assert.True(t, layer.Frozen())
}

func TestBlockOwnershipMapThisLayer(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	blocks := map[BlockIdx][]byte{
		0: make([]byte, 64),
		5: make([]byte, 64),
	}
	seedLayer(t, store, "layer-a", defaultLayerState(), blocks)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	assert.Equal(t, "layer-a", layer.FindBlockOwner(0))
	assert.Equal(t, "layer-a", layer.FindBlockOwner(5))
	assert.Equal(t, "", layer.FindBlockOwner(99))
}

func TestBlockOwnershipMapAncestors(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Root layer has blocks 0 and 1.
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: make([]byte, 64),
		1: make([]byte, 64),
	})

	// Child layer has block 1 (overrides root) and block 2.
	childState := LayerState{
		RefLayers: []string{"root"},
	}
	seedLayer(t, store, "child", childState, map[BlockIdx][]byte{
		1: make([]byte, 64),
		2: make([]byte, 64),
	})

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	assert.Equal(t, "root", layer.FindBlockOwner(0), "block 0 inherited from root")
	assert.Equal(t, "child", layer.FindBlockOwner(1), "block 1 overridden by child")
	assert.Equal(t, "child", layer.FindBlockOwner(2), "block 2 owned by child")
}

func TestBlockOwnershipMapTombstonesDeleteAncestorBlocks(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: make([]byte, 64),
	})

	childState := LayerState{
		RefLayers: []string{"root"},
	}
	seedLayer(t, store, "child", childState, map[BlockIdx][]byte{
		0: {}, // tombstone: size == 0
	})

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)
	assert.Equal(t, "", layer.FindBlockOwner(0), "tombstone should remove block from ownership map")
}

func TestBlockOwnershipMapThreeGenerations(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Grandparent: blocks 0, 1, 2.
	seedLayer(t, store, "gp", frozenLayerState(), map[BlockIdx][]byte{
		0: make([]byte, 64),
		1: make([]byte, 64),
		2: make([]byte, 64),
	})

	// Parent: overrides block 1, tombstones block 2.
	parentState := LayerState{
		RefLayers: []string{"gp"},

		FrozenAt: "2025-01-01T00:00:00Z",
	}
	seedLayer(t, store, "parent", parentState, map[BlockIdx][]byte{
		1: make([]byte, 64),
		2: {}, // tombstone
	})

	// Child: new block 3.
	childState := LayerState{
		RefLayers: []string{"parent", "gp"},
	}
	seedLayer(t, store, "child", childState, map[BlockIdx][]byte{
		3: make([]byte, 64),
	})

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	assert.Equal(t, "gp", layer.FindBlockOwner(0))
	assert.Equal(t, "parent", layer.FindBlockOwner(1))
	assert.Equal(t, "", layer.FindBlockOwner(2), "tombstoned by parent")
	assert.Equal(t, "child", layer.FindBlockOwner(3))
}

// --- Freeze tests ---

func TestFreezeMarksLayerFrozen(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	require.NoError(t, layer.Freeze(t.Context()))
	assert.True(t, layer.Frozen())

	// Verify state.json in S3 has frozen_at set.
	data, ok := store.GetObject("layers/layer-a/state.json")
	require.True(t, ok, "state.json missing after Freeze")
	var s LayerState
	require.NoError(t, json.Unmarshal(data, &s))
	assert.NotEmpty(t, s.FrozenAt)
}

func TestFreezeAlreadyFrozenFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "f", frozenLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "f")
	require.NoError(t, err)

	assert.Error(t, layer.Freeze(t.Context()))
}

func TestFreezeClosedLayerPanics(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer.Close(t.Context()))

	assert.Panics(t, func() { layer.Freeze(t.Context()) })
}

// --- Flush / tombstone tests ---

func TestFlushWritesTombstoneOnlyWhenAncestorOwnsBlock(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Root layer has block 0.
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: make([]byte, 64),
	})

	// Child layer inherits block 0 from root.
	childState := LayerState{
		RefLayers: []string{"root"},
	}
	seedLayer(t, store, "child", childState, nil)

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	// Write block 0 (ancestor-owned) to all zeros → should produce a tombstone in S3.
	require.NoError(t, layer.Write(t.Context(), 0, make([]byte, 64)))
	// Write block 1 (no ancestor) with data, then overwrite with zeros → should delete, not tombstone.
	require.NoError(t, layer.Write(t.Context(), 64, []byte("data that will be zeroed out, padded to fill block.......pad!")))
	require.NoError(t, layer.Flush(t.Context()))
	// Now block 1 exists in S3 for this layer.
	require.NoError(t, layer.Write(t.Context(), 64, make([]byte, 64)))

	require.NoError(t, layer.Flush(t.Context()))

	// Block 0: ancestor owns it → tombstone (zero-length object) in S3.
	data0, ok0 := store.GetObject("layers/child/0000000000000000")
	assert.True(t, ok0, "block 0 should have a tombstone in S3")
	assert.Empty(t, data0, "block 0 should be zero-length (tombstone)")

	// Block 1: no ancestor owns it → should be deleted from S3 entirely.
	_, ok1 := store.GetObject("layers/child/0000000000000001")
	assert.False(t, ok1, "block 1 should be deleted from S3, not tombstoned")
}

func TestPunchHoleAndWriteZerosAreIdentical(t *testing.T) {
	// Set up two identical volumes, apply zeros via Write to one
	// and PunchHole to the other, then compare S3 state.
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	// Root layer with blocks 0..3.
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: make([]byte, 64),
		1: make([]byte, 64),
		2: make([]byte, 64),
		3: make([]byte, 64),
	})

	// Two child layers inheriting from root.
	for _, id := range []string{"write-zeros", "punch-hole"} {
		childState := LayerState{
			RefLayers: []string{"root"},
		}
		seedLayer(t, store, id, childState, nil)
	}

	writeLayer, err := NewLayer(t.Context(), vm, "write-zeros")
	require.NoError(t, err)

	punchLayer, err := NewLayer(t.Context(), vm, "punch-hole")
	require.NoError(t, err)

	// Zero out blocks 1 and 2 (block-aligned) via each method.
	require.NoError(t, writeLayer.Write(t.Context(), 64, make([]byte, 128)))
	require.NoError(t, writeLayer.Flush(t.Context()))

	require.NoError(t, punchLayer.PunchHole(t.Context(), 64, 128))
	require.NoError(t, punchLayer.Flush(t.Context()))

	// Both should produce identical S3 state: tombstones for blocks 1 and 2.
	for _, blk := range []string{"0000000000000001", "0000000000000002"} {
		wData, wOk := store.GetObject("layers/write-zeros/" + blk)
		pData, pOk := store.GetObject("layers/punch-hole/" + blk)
		assert.Equal(t, wOk, pOk, "block %s existence should match", blk)
		assert.Equal(t, wData, pData, "block %s content should match", blk)
	}

	// Both layers should read back identical data.
	for _, layer := range []*Layer{writeLayer, punchLayer} {
		buf := make([]byte, 256)
		_, err := layer.Read(t.Context(), 0, buf)
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 256), buf, "layer %s should read all zeros", layer.id)
	}
}

// --- CreateChild tests ---

func TestCreateChildWritesChildState(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "parent", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "parent")
	require.NoError(t, err)
	require.NoError(t, layer.Freeze(t.Context()))

	require.NoError(t, layer.CreateChild(t.Context(), "child-1"))

	// Child state.json should exist with correct ancestors.
	data, ok := store.GetObject("layers/child-1/state.json")
	require.True(t, ok, "child state.json missing")
	var childState LayerState
	require.NoError(t, json.Unmarshal(data, &childState))
	assert.Equal(t, []string{"parent"}, childState.RefLayers)

	// Parent's children list should include child-1.
	parentData, ok := store.GetObject("layers/parent/state.json")
	require.True(t, ok)
	var parentState LayerState
	require.NoError(t, json.Unmarshal(parentData, &parentState))
	assert.Contains(t, parentState.Children, "child-1")
}

func TestCreateChildAncestorChain(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	seedLayer(t, store, "gp", frozenLayerState(), nil)
	parentState := LayerState{
		RefLayers: []string{"gp"},
	}
	seedLayer(t, store, "parent", parentState, nil)

	layer, err := NewLayer(t.Context(), vm, "parent")
	require.NoError(t, err)
	require.NoError(t, layer.Freeze(t.Context()))
	require.NoError(t, layer.CreateChild(t.Context(), "child"))

	data, ok := store.GetObject("layers/child/state.json")
	require.True(t, ok)
	var cs LayerState
	require.NoError(t, json.Unmarshal(data, &cs))
	assert.Equal(t, []string{"parent", "gp"}, cs.RefLayers)
}

func TestMultipleChildrenRecordedInParent(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "parent", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "parent")
	require.NoError(t, err)
	require.NoError(t, layer.Freeze(t.Context()))

	for i := 0; i < 3; i++ {
		require.NoError(t, layer.CreateChild(t.Context(), fmt.Sprintf("child-%d", i)))
	}

	data, ok := store.GetObject("layers/parent/state.json")
	require.True(t, ok)
	var s LayerState
	require.NoError(t, json.Unmarshal(data, &s))
	assert.Len(t, s.Children, 3)
}

// --- Mount / Unmount tests ---

func TestMountClaimsLease(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer.Mount(t.Context()))

	data, ok := store.GetObject("layers/layer-a/state.json")
	require.True(t, ok)
	var s LayerState
	require.NoError(t, json.Unmarshal(data, &s))
	assert.Equal(t, vm.lease.Token(), s.LeaseToken)
}

func TestMountFrozenLayerIsNoop(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "f", frozenLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "f")
	require.NoError(t, err)
	assert.NoError(t, layer.Mount(t.Context()))
}

func TestMountByAnotherVMFails(t *testing.T) {
	store := NewMemStore()
	vm1 := newTestVM(t, store)
	vm2 := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer1, err := NewLayer(t.Context(), vm1, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer1.Mount(t.Context()))

	layer2, err := NewLayer(t.Context(), vm2, "layer-a")
	require.NoError(t, err)
	assert.Error(t, layer2.Mount(t.Context()))
}

func TestUnmountReleasesClaim(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer.Mount(t.Context()))
	require.NoError(t, layer.Unmount(t.Context()))

	data, ok := store.GetObject("layers/layer-a/state.json")
	require.True(t, ok)
	var s LayerState
	require.NoError(t, json.Unmarshal(data, &s))
	assert.Empty(t, s.LeaseToken)
}

// --- Close tests ---

func TestCloseIsIdempotent(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)

	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer.Mount(t.Context()))
	require.NoError(t, layer.Close(t.Context()))
	assert.NoError(t, layer.Close(t.Context()))
}

// --- listBlocks tests ---

func TestListBlocksParseHexKeys(t *testing.T) {
	store := NewMemStore()
	store.PutBytes(t.Context(), "0000000000000000", make([]byte, 64))
	store.PutBytes(t.Context(), "000000000000000a", make([]byte, 64))
	store.PutBytes(t.Context(), "00000000000000ff", []byte{}) // tombstone
	store.PutBytes(t.Context(), "state.json", []byte("{}"))   // should be skipped

	blocks, tombstones, err := listBlocks(t.Context(), store)
	require.NoError(t, err)

	assert.Len(t, blocks, 3)
	assert.Contains(t, blocks, BlockIdx(0))
	assert.Contains(t, blocks, BlockIdx(10))
	assert.Contains(t, blocks, BlockIdx(255))

	assert.Len(t, tombstones, 1)
	assert.Contains(t, tombstones, BlockIdx(255))
}

// --- readLayerState tests ---

func TestReadLayerState(t *testing.T) {
	store := NewMemStore()
	state := LayerState{
		RefLayers: []string{"parent-1"},
		FrozenAt:  "2025-01-01T00:00:00Z",
	}
	data, _ := json.Marshal(state)
	store.PutBytes(t.Context(), "state.json", data)

	got, _, err := readLayerState(t.Context(), store)
	require.NoError(t, err)
	assert.Equal(t, []string{"parent-1"}, got.RefLayers)
	assert.Equal(t, "2025-01-01T00:00:00Z", got.FrozenAt)
}

// --- Integration: NewLayer with blocks from multiple ancestors ---

func TestNewLayerBuildsOwnershipMapFromMultipleAncestors(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// root: blocks 0..4
	rootBlocks := make(map[BlockIdx][]byte)
	for i := BlockIdx(0); i < 5; i++ {
		rootBlocks[i] = make([]byte, 64)
	}
	seedLayer(t, store, "root", frozenLayerState(), rootBlocks)

	// mid: overrides block 2, tombstones block 3
	midState := LayerState{
		RefLayers: []string{"root"},

		FrozenAt: "2025-01-01T00:00:00Z",
	}
	seedLayer(t, store, "mid", midState, map[BlockIdx][]byte{
		2: make([]byte, 64),
		3: {}, // tombstone
	})

	// leaf: adds block 10
	leafState := LayerState{
		RefLayers: []string{"mid", "root"},
	}
	seedLayer(t, store, "leaf", leafState, map[BlockIdx][]byte{
		10: make([]byte, 64),
	})

	layer, err := NewLayer(t.Context(), vm, "leaf")
	require.NoError(t, err)

	tests := []struct {
		block BlockIdx
		want  string
	}{
		{0, "root"},
		{1, "root"},
		{2, "mid"},
		{3, ""}, // tombstoned by mid
		{4, "root"},
		{10, "leaf"},
		{99, ""}, // never written
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, layer.FindBlockOwner(tt.block), "block %d", tt.block)
	}
}

// --- CopyFrom tests ---

func TestCopyFromFullBlockIsCoW(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source layer with a full block of data.
	srcData := bytes.Repeat([]byte("S"), 64)
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: srcData,
	})

	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Copy full block 0 from src to dst at same offset.
	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// The block should be a CoW reference, not a dirty data copy.
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	_, hasOpenBlock := dst.openBlocks[0]
	dst.mu.Unlock()
	assert.False(t, isDirty, "full-block CoW should not mark block dirty")
	assert.False(t, hasOpenBlock, "full-block CoW should not have open block file")

	// refBlockIndex should point block 0 at "src".
	assert.Equal(t, "src", dst.refBlockIndex[0])

	// Reading through the layer should return the source data.
	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, srcData, buf)
}

func TestCopyFromPartialBlockCopiesData(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	srcData := bytes.Repeat([]byte("P"), 64)
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: srcData,
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Copy 32 bytes from offset 16 in src to offset 16 in dst (partial block).
	n, err := dst.CopyFrom(t.Context(), src, 16, 16, 32)
	require.NoError(t, err)
	assert.Equal(t, uint64(32), n)

	// Partial block copy should be dirty (actual data copy).
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	dst.mu.Unlock()
	assert.True(t, isDirty, "partial block copy should mark block dirty")

	// Read back and verify: bytes 0-15 zeros, bytes 16-47 = "P"s, bytes 48-63 zeros.
	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 16), buf[:16])
	assert.Equal(t, bytes.Repeat([]byte("P"), 32), buf[16:48])
	assert.Equal(t, make([]byte, 16), buf[48:])
}

func TestCopyFromMultipleBlocks(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source layer with blocks 0, 1, 2.
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("A"), 64),
		1: bytes.Repeat([]byte("B"), 64),
		2: bytes.Repeat([]byte("C"), 64),
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Copy all 3 blocks (192 bytes, block-aligned).
	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 192)
	require.NoError(t, err)
	assert.Equal(t, uint64(192), n)

	// All 3 should be CoW references.
	for _, blk := range []BlockIdx{0, 1, 2} {
		assert.Equal(t, "src", dst.refBlockIndex[blk], "block %d should reference src", blk)
	}

	// Read all data back.
	buf := make([]byte, 192)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("A"), 64), buf[0:64])
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), buf[64:128])
	assert.Equal(t, bytes.Repeat([]byte("C"), 64), buf[128:192])
}

func TestCopyFromMixedPartialAndFull(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source: 3 blocks of data.
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("A"), 64),
		1: bytes.Repeat([]byte("B"), 64),
		2: bytes.Repeat([]byte("C"), 64),
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Copy from offset 32 for 128 bytes:
	//   - partial tail of block 0 (32 bytes) → data copy into dst block 0
	//   - full block 1 (64 bytes) → CoW
	//   - partial head of block 2 (32 bytes) → data copy into dst block 2
	n, err := dst.CopyFrom(t.Context(), src, 32, 32, 128)
	require.NoError(t, err)
	assert.Equal(t, uint64(128), n)

	// Block 0: partial → dirty (data copy).
	dst.mu.Lock()
	_, blk0Dirty := dst.dirty[0]
	_, blk2Dirty := dst.dirty[2]
	dst.mu.Unlock()
	assert.True(t, blk0Dirty, "partial block 0 should be dirty")
	assert.True(t, blk2Dirty, "partial block 2 should be dirty")

	// Block 1: full → CoW reference.
	assert.Equal(t, "src", dst.refBlockIndex[1], "full block 1 should be CoW")

	// Read back the full range.
	buf := make([]byte, 192)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)

	// bytes 0-31: zeros (not copied)
	assert.Equal(t, make([]byte, 32), buf[:32])
	// bytes 32-63: "A"s (from partial copy of src block 0)
	assert.Equal(t, bytes.Repeat([]byte("A"), 32), buf[32:64])
	// bytes 64-127: "B"s (CoW of src block 1)
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), buf[64:128])
	// bytes 128-159: "C"s (from partial copy of src block 2)
	assert.Equal(t, bytes.Repeat([]byte("C"), 32), buf[128:160])
	// bytes 160-191: zeros (not copied)
	assert.Equal(t, make([]byte, 32), buf[160:192])
}

func TestCopyFromUnwrittenBlockYieldsZeros(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source layer with no blocks written.
	seedLayer(t, store, "src", frozenLayerState(), nil)
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Write some data to dst block 0, then CopyFrom src (which is empty).
	require.NoError(t, dst.Write(t.Context(), 0, bytes.Repeat([]byte("X"), 64)))

	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// The block should now read as zeros (unwritten src block).
	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 64), buf)
}

func TestCopyFromDifferentBlockOffsets(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source layer with block 5.
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		5: bytes.Repeat([]byte("Z"), 64),
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Copy block 5 from src to block 10 in dst.
	// Since block indices differ, this must be a data copy (not CoW).
	n, err := dst.CopyFrom(t.Context(), src, 5*64, 10*64, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// Different block indices → data copy, so block 10 should be dirty.
	dst.mu.Lock()
	_, isDirty := dst.dirty[10]
	dst.mu.Unlock()
	assert.True(t, isDirty, "different block offsets require data copy")

	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 10*64, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("Z"), 64), buf)
}

func TestCopyFromFrozenDestFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	seedLayer(t, store, "src", frozenLayerState(), nil)
	seedLayer(t, store, "dst", frozenLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	_, err = dst.CopyFrom(t.Context(), src, 0, 0, 64)
	assert.Error(t, err)
}

func TestCopyFromReplacesExistingDirtyBlock(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("N"), 64),
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Write to dst block 0 first.
	require.NoError(t, dst.Write(t.Context(), 0, bytes.Repeat([]byte("O"), 64)))

	// Now CoW-copy from src — should replace the dirty block.
	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// Block should no longer be dirty (it's a ref now).
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	dst.mu.Unlock()
	assert.False(t, isDirty)

	// Should read the source data.
	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("N"), 64), buf)
}

func TestCopyFromInheritedBlock(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Root has block 0. Source child inherits it.
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("R"), 64),
	})
	srcState := LayerState{
		RefLayers: []string{"root"},
		FrozenAt:  "2025-01-01T00:00:00Z",
	}
	seedLayer(t, store, "src", srcState, nil)
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// CopyFrom should follow the ref chain: src -> root for block 0.
	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// The ref should point at "root" (the actual owner), not "src".
	assert.Equal(t, "root", dst.refBlockIndex[0])

	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("R"), 64), buf)
}

func TestCopyFromPreservesInCreateChild(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source layer with data.
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("D"), 64),
	})

	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// CoW copy from src into dst.
	_, err = dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)

	// Freeze dst and create a child.
	require.NoError(t, dst.Freeze(t.Context()))
	require.NoError(t, dst.CreateChild(t.Context(), "grandchild"))

	// The grandchild's state should include "src" in its RefLayers.
	data, ok := store.GetObject("layers/grandchild/state.json")
	require.True(t, ok)
	var childState LayerState
	require.NoError(t, json.Unmarshal(data, &childState))

	// RefLayers should contain dst and src (since dst references src via refBlockIndex).
	assert.Contains(t, childState.RefLayers, "dst")
	assert.Contains(t, childState.RefLayers, "src")

	// The grandchild should be able to read the data.
	grandchild, err := NewLayer(t.Context(), vm, "grandchild")
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = grandchild.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("D"), 64), buf)
}

func TestCopyFromUnfrozenSourceDirtyBlockDataCopies(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source is NOT frozen and has dirty (unflushed) blocks.
	seedLayer(t, store, "src", defaultLayerState(), nil)
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Write to src without flushing — block is dirty/open, not in S3.
	require.NoError(t, src.Write(t.Context(), 0, bytes.Repeat([]byte("U"), 64)))

	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// Must be a data copy, not a CoW ref — src is not frozen.
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	refOwner := dst.refBlockIndex[0]
	dst.mu.Unlock()
	assert.True(t, isDirty, "unfrozen source block must be data-copied")
	assert.Empty(t, refOwner, "must not reference an unfrozen layer")

	// Data should still be correct.
	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("U"), 64), buf)
}

func TestCopyFromUnfrozenSourceFlushedBlockDataCopies(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source is NOT frozen but has flushed blocks (in localIndex/S3).
	seedLayer(t, store, "src", defaultLayerState(), nil)
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Write and flush — block is in S3 but src is still not frozen.
	require.NoError(t, src.Write(t.Context(), 0, bytes.Repeat([]byte("F"), 64)))
	require.NoError(t, src.Flush(t.Context()))

	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// Must still be a data copy — src is not frozen, even though the
	// block is in S3. Referencing it would be unsafe because src could
	// overwrite it before freezing.
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	refOwner := dst.refBlockIndex[0]
	dst.mu.Unlock()
	assert.True(t, isDirty, "unfrozen source block must be data-copied even if flushed")
	assert.Empty(t, refOwner, "must not reference an unfrozen layer")

	buf := make([]byte, 64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("F"), 64), buf)
}

func TestCopyFromUnfrozenSourceRefBlockIsCoW(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Frozen grandparent with data.
	seedLayer(t, store, "gp", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("G"), 64),
		1: bytes.Repeat([]byte("H"), 64),
	})

	// Source inherits from frozen grandparent. Source is NOT frozen itself,
	// but block 0 comes from "gp" via refBlockIndex — which is frozen.
	srcState := LayerState{RefLayers: []string{"gp"}}
	seedLayer(t, store, "src", srcState, nil)
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	// Also write a dirty block to src so it's clearly not frozen.
	require.NoError(t, src.Write(t.Context(), 2*64, bytes.Repeat([]byte("W"), 64)))

	// Copy blocks 0 and 1 (from frozen ref) and block 2 (dirty on src).
	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 3*64)
	require.NoError(t, err)
	assert.Equal(t, uint64(3*64), n)

	// Blocks 0 and 1: owned by "gp" (frozen) → CoW ref.
	assert.Equal(t, "gp", dst.refBlockIndex[0], "ref block from frozen layer should be CoW")
	assert.Equal(t, "gp", dst.refBlockIndex[1], "ref block from frozen layer should be CoW")

	// Block 2: owned by "src" (not frozen) → data copy.
	dst.mu.Lock()
	_, blk2Dirty := dst.dirty[2]
	blk2Ref := dst.refBlockIndex[2]
	dst.mu.Unlock()
	assert.True(t, blk2Dirty, "dirty block from unfrozen src must be data-copied")
	assert.Empty(t, blk2Ref, "must not reference unfrozen layer for dirty block")

	// Verify all data reads correctly.
	buf := make([]byte, 3*64)
	_, err = dst.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("G"), 64), buf[0:64])
	assert.Equal(t, bytes.Repeat([]byte("H"), 64), buf[64:128])
	assert.Equal(t, bytes.Repeat([]byte("W"), 64), buf[128:192])
}

func TestCopyFromFrozenSourceOwnBlockIsCoW(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	// Source is frozen with its own blocks in S3.
	seedLayer(t, store, "src", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("X"), 64),
	})
	seedLayer(t, store, "dst", defaultLayerState(), nil)

	src, err := NewLayer(t.Context(), vm, "src")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "dst")
	require.NoError(t, err)

	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, uint64(64), n)

	// Source is frozen, so even its own blocks can be referenced.
	assert.Equal(t, "src", dst.refBlockIndex[0], "frozen source's own block should be CoW")
	dst.mu.Lock()
	_, isDirty := dst.dirty[0]
	dst.mu.Unlock()
	assert.False(t, isDirty, "CoW ref should not be dirty")
}
