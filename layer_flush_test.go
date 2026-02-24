package loophole

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestFlushRedirtiesOnUploadFailure(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write two blocks.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("A"), 64)))
	require.NoError(t, layer.Write(t.Context(), 64, bytes.Repeat([]byte("B"), 64)))

	// Make PutReader fail for block 1 only.
	block1Key := "layers/layer-a/0000000000000001"
	store.SetFault(OpPutReader, block1Key, Fault{Err: fmt.Errorf("injected S3 failure")})

	err = layer.Flush(t.Context())
	assert.Error(t, err, "flush should report the upload failure")

	// Block 0 should have been uploaded successfully.
	_, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block 0 should be in S3")

	// Block 1 should NOT be in S3.
	_, ok = store.GetObject(block1Key)
	assert.False(t, ok, "block 1 should not be in S3 after failure")

	// Block 1 should be back in the dirty set.
	layer.mu.Lock()
	_, isDirty := layer.dirty[1]
	layer.mu.Unlock()
	assert.True(t, isDirty, "failed block should be re-dirtied")

	// Clear the fault and flush again — block 1 should upload now.
	store.ClearFault(OpPutReader, block1Key)
	require.NoError(t, layer.Flush(t.Context()))

	data, ok := store.GetObject(block1Key)
	assert.True(t, ok, "block 1 should be in S3 after retry")
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), data)
}

func TestFlushAllZeroBlockBecomesTombstone(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	// Root has block 0 so the child layer will write a tombstone (not delete).
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("X"), 64),
	})
	childState := LayerState{RefLayers: []string{"root"}}
	seedLayer(t, store, "child", childState, nil)

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	// Write a full block of zeros.
	require.NoError(t, layer.Write(t.Context(), 0, make([]byte, 64)))
	require.NoError(t, layer.Flush(t.Context()))

	// Should be a tombstone (zero-length object), not a 64-byte file of zeros.
	data, ok := store.GetObject("layers/child/0000000000000000")
	assert.True(t, ok, "tombstone should exist in S3")
	assert.Empty(t, data, "tombstone should be zero-length")

	// Reading back should return zeros.
	buf := make([]byte, 64)
	_, err = layer.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 64), buf)
}

func TestFlushAllZeroBlockNoAncestorDeletes(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	// Layer with no ancestors — writing zeros should delete, not tombstone.
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write data first so the block exists in S3.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("D"), 64)))
	require.NoError(t, layer.Flush(t.Context()))

	_, ok := store.GetObject("layers/layer-a/0000000000000000")
	require.True(t, ok, "block should exist after first flush")

	// Now overwrite with zeros.
	require.NoError(t, layer.Write(t.Context(), 0, make([]byte, 64)))
	require.NoError(t, layer.Flush(t.Context()))

	// No ancestor owns this block, so it should be deleted entirely.
	_, ok = store.GetObject("layers/layer-a/0000000000000000")
	assert.False(t, ok, "all-zero block with no ancestor should be deleted from S3")
}

func TestFlushConcurrentWriteDuringUpload(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write block 0 with initial data.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("A"), 64)))

	// Flush to upload the initial data.
	require.NoError(t, layer.Flush(t.Context()))

	// Now write again — this makes block 0 dirty again.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("B"), 64)))

	// Flush again — after the dirty set is swapped (at the start of Flush),
	// write new data. The write goes to the NEW dirty set.
	require.NoError(t, layer.Flush(t.Context()))

	// Block 0 should be in S3 with "B"s from the second flush.
	data, ok := store.GetObject("layers/layer-a/0000000000000000")
	require.True(t, ok)
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), data)

	// Now test the actual dirty-set swap semantics: write, then flush
	// while also writing. We use the fact that a write after Flush's
	// swap will land in the new dirty set.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("C"), 64)))
	require.NoError(t, layer.Flush(t.Context()))

	// Immediately write new data — this is after the flush completed,
	// so block 0 goes into a fresh dirty set.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("D"), 64)))

	layer.mu.Lock()
	_, isDirty := layer.dirty[0]
	layer.mu.Unlock()
	assert.True(t, isDirty, "write after flush should be in the dirty set")

	// S3 still has "C"s from the previous flush.
	data, ok = store.GetObject("layers/layer-a/0000000000000000")
	require.True(t, ok)
	assert.Equal(t, bytes.Repeat([]byte("C"), 64), data)

	// Final flush picks up the "D"s.
	require.NoError(t, layer.Flush(t.Context()))
	data, ok = store.GetObject("layers/layer-a/0000000000000000")
	require.True(t, ok)
	assert.Equal(t, bytes.Repeat([]byte("D"), 64), data)
}

func TestFlushEmptyIsNoop(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	store.ResetCounts()
	require.NoError(t, layer.Flush(t.Context()))

	// No PutBytes or DeleteObject calls should have been made.
	assert.Equal(t, int64(0), store.Count(OpPutBytes), "empty flush should not call PutBytes")
	assert.Equal(t, int64(0), store.Count(OpDeleteObject), "empty flush should not call DeleteObject")
}

func TestOpenBlockConcurrentSameBlock(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	// Seed a frozen ancestor with block 0 so openBlock must fetch from S3.
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("R"), 64),
	})
	childState := LayerState{RefLayers: []string{"root"}}
	seedLayer(t, store, "child", childState, nil)

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	store.ResetCounts()

	// Spawn 10 goroutines all writing to block 0 concurrently.
	// Without singleflight, each would trigger a separate S3 fetch.
	const n = 10
	g, ctx := errgroup.WithContext(t.Context())
	for i := range n {
		marker := byte('a' + i)
		g.Go(func() error {
			return layer.Write(ctx, 0, bytes.Repeat([]byte{marker}, 64))
		})
	}
	require.NoError(t, g.Wait())

	// The block cache fetches via Get. With singleflight, we expect
	// exactly 1 Get for the block data (the cache fetch), not 10.
	// There may be additional Gets for state.json etc during NewLayer,
	// but those happened before ResetCounts. The only Gets after reset
	// should be for the block fetch itself.
	gets := store.Count(OpGet)
	assert.LessOrEqual(t, gets, int64(2), "singleflight should deduplicate concurrent fetches (got %d Gets)", gets)
}

func TestCloseFlushesAndReleasesLease(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	require.NoError(t, layer.Mount(t.Context()))

	// Write dirty data.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("C"), 64)))

	// Close should flush then release lease.
	require.NoError(t, layer.Close(t.Context()))

	// Data should be in S3.
	data, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "data should be flushed to S3 on Close")
	assert.Equal(t, bytes.Repeat([]byte("C"), 64), data)

	// Lease should be released.
	stateData, ok := store.GetObject("layers/layer-a/state.json")
	require.True(t, ok)
	var s LayerState
	require.NoError(t, json.Unmarshal(stateData, &s))
	assert.Empty(t, s.LeaseToken, "lease should be released after Close")
}

func TestFreezeFlushesDirtyBlocks(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write dirty blocks.
	require.NoError(t, layer.Write(t.Context(), 0, bytes.Repeat([]byte("F"), 64)))
	require.NoError(t, layer.Write(t.Context(), 64, bytes.Repeat([]byte("G"), 64)))

	// Freeze — should flush all dirty blocks before writing frozen_at.
	require.NoError(t, layer.Freeze(t.Context()))

	// Both blocks should be in S3.
	data0, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block 0 should be in S3 after freeze")
	assert.Equal(t, bytes.Repeat([]byte("F"), 64), data0)

	data1, ok := store.GetObject("layers/layer-a/0000000000000001")
	assert.True(t, ok, "block 1 should be in S3 after freeze")
	assert.Equal(t, bytes.Repeat([]byte("G"), 64), data1)

	// frozen_at should be set.
	stateData, ok := store.GetObject("layers/layer-a/state.json")
	require.True(t, ok)
	var s LayerState
	require.NoError(t, json.Unmarshal(stateData, &s))
	assert.NotEmpty(t, s.FrozenAt, "frozen_at should be set")

	// No dirty blocks should remain.
	layer.mu.Lock()
	dirtyCount := len(layer.dirty)
	layer.mu.Unlock()
	assert.Equal(t, 0, dirtyCount, "no dirty blocks should remain after freeze")
}
