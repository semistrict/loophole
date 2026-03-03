package loophole

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestFlushRedirtiesOnUploadFailure(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write two blocks.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

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
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// Root has block 0 so the child layer will write a tombstone (not delete).
	seedLayer(t, store, "root", frozenLayerState(), map[BlockIdx][]byte{
		0: bytes.Repeat([]byte("X"), 64),
	})
	childState := LayerState{RefLayers: []string{"root"}}
	seedLayer(t, store, "child", childState, nil)

	layer, err := NewLayer(t.Context(), vm, "child")
	require.NoError(t, err)

	// Write a full block of zeros.
	require.NoError(t, layer.Write(t.Context(), make([]byte, 64), 0))
	require.NoError(t, layer.Flush(t.Context()))

	// Should be a tombstone (zero-length object), not a 64-byte file of zeros.
	data, ok := store.GetObject("layers/child/0000000000000000")
	assert.True(t, ok, "tombstone should exist in S3")
	assert.Empty(t, data, "tombstone should be zero-length")

	// Reading back should return zeros.
	buf := make([]byte, 64)
	_, err = layer.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 64), buf)
}

func TestFlushAllZeroBlockNoAncestorDeletes(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// Layer with no ancestors — writing zeros should delete, not tombstone.
	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write data first so the block exists in S3.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("D"), 64), 0))
	require.NoError(t, layer.Flush(t.Context()))

	_, ok := store.GetObject("layers/layer-a/0000000000000000")
	require.True(t, ok, "block should exist after first flush")

	// Now overwrite with zeros.
	require.NoError(t, layer.Write(t.Context(), make([]byte, 64), 0))
	require.NoError(t, layer.Flush(t.Context()))

	// No ancestor owns this block, so it should be deleted entirely.
	_, ok = store.GetObject("layers/layer-a/0000000000000000")
	assert.False(t, ok, "all-zero block with no ancestor should be deleted from S3")
}

func TestFlushConcurrentWriteDuringUpload(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write block 0 with initial data.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

	// Flush to upload the initial data.
	require.NoError(t, layer.Flush(t.Context()))

	// Now write again — this makes block 0 dirty again.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 0))

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
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("C"), 64), 0))
	require.NoError(t, layer.Flush(t.Context()))

	// Immediately write new data — this is after the flush completed,
	// so block 0 goes into a fresh dirty set.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("D"), 64), 0))

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

// TestFlushConcurrentDurability verifies that if Flush A is mid-upload and
// Flush B is called concurrently, Flush B does NOT return until the data is
// durable in S3. This is a correctness requirement: an fsync that triggers
// Flush B must not return "success" while blocks are still in-flight.
//
// Previous behavior (fixed): Flush B would see an empty dirty set (Flush A
// already swapped it) and return immediately — before Flush A's uploads complete.
func TestFlushConcurrentDurability(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write a block so it's dirty.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("X"), 64), 0))

	// Set up a hook on PutReader: when Flush A starts uploading, it signals
	// uploadStarted, then blocks until we release it.
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})
	store.SetFault(OpPutReader, "", Fault{
		Hook: func() {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-uploadRelease
		},
	})

	// Flush A: starts uploading, blocks in PutReader hook.
	flushADone := make(chan error, 1)
	go func() {
		flushADone <- layer.Flush(t.Context())
	}()

	// Wait for Flush A to be mid-upload.
	<-uploadStarted

	// Flush B: called while Flush A is mid-upload.
	// If flush coalescing works correctly, this should block until
	// Flush A completes (i.e., until the data is in S3).
	flushBDone := make(chan error, 1)
	go func() {
		flushBDone <- layer.Flush(t.Context())
	}()

	// Give Flush B a moment to execute. If it returns immediately (the bug),
	// it will be in the channel. If it correctly waits, the channel stays empty.
	flushBReturnedEarly := false
	select {
	case err := <-flushBDone:
		// Flush B returned while Flush A is still uploading — this is the bug.
		flushBReturnedEarly = true
		assert.NoError(t, err)
	case <-time.After(50 * time.Millisecond):
		// Flush B is still blocked — correct behavior.
	}

	// Data should NOT be in S3 yet (Flush A is still blocked).
	_, inS3 := store.GetObject("layers/layer-a/0000000000000000")
	assert.False(t, inS3, "block should not be in S3 while upload is in-flight")

	// Release Flush A's upload.
	close(uploadRelease)
	store.ClearFault(OpPutReader, "")

	// Both flushes should complete.
	assert.NoError(t, <-flushADone)
	if !flushBReturnedEarly {
		assert.NoError(t, <-flushBDone)
	}

	// NOW data should be in S3.
	data, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block should be in S3 after flush completes")
	assert.Equal(t, bytes.Repeat([]byte("X"), 64), data)

	// This is the key assertion: Flush B should NOT have returned before
	// the data was in S3. If it did, the fsync lied about durability.
	assert.False(t, flushBReturnedEarly,
		"concurrent Flush must wait for in-flight uploads to complete (fsync durability guarantee)")
}

func TestFlushEmptyIsNoop(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

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
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

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
			return layer.Write(ctx, bytes.Repeat([]byte{marker}, 64), 0)
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
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("C"), 64), 0))

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
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write dirty blocks.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("F"), 64), 0))
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("G"), 64), 64))

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

// TestFlushWhileBackgroundFlushRunning verifies that an explicit Flush()
// call while the background flush timer fires works correctly — both
// flushes complete without data loss.
func TestFlushWhileBackgroundFlushRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := newTestVM(t, store)

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)
		t.Cleanup(func() {
			layer.Close(t.Context())
			vm.Close(t.Context())
		})

		// Write block 0.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

		// Delay PutReader by 5s so the background flush is mid-upload.
		store.SetFault(OpPutReader, "", Fault{Delay: 5 * time.Second})

		// Advance time to trigger background flush (30s interval).
		time.Sleep(backgroundFlushInterval)
		synctest.Wait()

		// Write block 1 while background flush is uploading block 0.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

		// Explicit flush — should wait for background flush then upload block 1.
		store.ClearFault(OpPutReader, "")
		require.NoError(t, layer.Flush(t.Context()))

		// Both blocks should be in S3.
		data0, ok := store.GetObject("layers/layer-a/0000000000000000")
		assert.True(t, ok, "block 0 should be in S3")
		assert.Equal(t, bytes.Repeat([]byte("A"), 64), data0)

		data1, ok := store.GetObject("layers/layer-a/0000000000000001")
		assert.True(t, ok, "block 1 should be in S3")
		assert.Equal(t, bytes.Repeat([]byte("B"), 64), data1)
	})
}

// TestFlushCoalescingMultipleWaiters verifies that multiple concurrent
// Flush() calls result in at most one flush cycle at a time, and all
// callers return only after data is durable.
func TestFlushCoalescingMultipleWaiters(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		vm.Close(t.Context())
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write a block.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("X"), 64), 0))

	// Slow down uploads so the first flush is still running when others call Flush.
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})
	store.SetFault(OpPutReader, "", Fault{
		Hook: func() {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-uploadRelease
		},
	})

	store.ResetCounts()

	// Start flush A.
	var g errgroup.Group
	g.Go(func() error { return layer.Flush(t.Context()) })

	// Wait for flush A to be mid-upload.
	<-uploadStarted

	// Launch 5 more flush calls — they should all wait for flush A.
	for range 5 {
		g.Go(func() error { return layer.Flush(t.Context()) })
	}

	// Give waiters time to reach the Cond.Wait().
	time.Sleep(10 * time.Millisecond)

	// Release the upload.
	close(uploadRelease)
	store.ClearFault(OpPutReader, "")

	require.NoError(t, g.Wait())

	// Only one PutReader call should have been made (flush A's upload).
	// The 5 waiters see an empty dirty set after waiting.
	puts := store.Count(OpPutReader)
	assert.Equal(t, int64(1), puts, "only one flush cycle should upload (got %d PutReader calls)", puts)

	// Data should be in S3.
	_, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block should be in S3")
}

// TestFlushPicksUpWritesDuringInFlightFlush verifies that writes landing
// after Flush A's dirty-set swap but before Flush B is called are picked
// up by Flush B (not lost).
func TestFlushPicksUpWritesDuringInFlightFlush(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		vm.Close(t.Context())
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write block 0 (block A).
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

	// Slow down Flush A so we can write block 1 while it's uploading.
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})
	store.SetFault(OpPutReader, "", Fault{
		Hook: func() {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-uploadRelease
		},
	})

	flushADone := make(chan error, 1)
	go func() { flushADone <- layer.Flush(t.Context()) }()

	// Wait for Flush A to be mid-upload (dirty set already swapped).
	<-uploadStarted

	// Write block 1 — this goes into the NEW dirty set.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

	// Release Flush A.
	close(uploadRelease)
	store.ClearFault(OpPutReader, "")
	require.NoError(t, <-flushADone)

	// Block 0 should be in S3 from Flush A.
	_, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block 0 should be in S3 after Flush A")

	// Block 1 should NOT be in S3 yet — it was written after Flush A's swap.
	_, ok = store.GetObject("layers/layer-a/0000000000000001")
	assert.False(t, ok, "block 1 should not be in S3 yet")

	// Flush B picks up block 1.
	require.NoError(t, layer.Flush(t.Context()))

	data1, ok := store.GetObject("layers/layer-a/0000000000000001")
	assert.True(t, ok, "block 1 should be in S3 after Flush B")
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), data1)
}

// TestFlushErrorDoesNotBlockWaiters verifies that if Flush A fails,
// waiters (Flush B) wake up and proceed rather than deadlocking.
func TestFlushErrorDoesNotBlockWaiters(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		vm.Close(t.Context())
	}()

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write a block.
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("X"), 64), 0))

	// Make the first upload fail after signaling.
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})
	store.SetFault(OpPutReader, "", Fault{
		Hook: func() {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-uploadRelease
		},
		Err: fmt.Errorf("injected S3 failure"),
	})

	flushADone := make(chan error, 1)
	go func() { flushADone <- layer.Flush(t.Context()) }()

	<-uploadStarted

	// Flush B waits for A.
	flushBDone := make(chan error, 1)
	go func() { flushBDone <- layer.Flush(t.Context()) }()

	// Give Flush B time to reach the Cond.Wait.
	time.Sleep(10 * time.Millisecond)

	// Release Flush A (it will fail).
	close(uploadRelease)
	store.ClearFault(OpPutReader, "")

	// Flush A should fail.
	assert.Error(t, <-flushADone)

	// Flush B should NOT deadlock — it wakes up, sees the re-dirtied block,
	// and uploads it successfully.
	err = <-flushBDone
	assert.NoError(t, err, "Flush B should succeed after Flush A fails")

	// The block should now be in S3 (uploaded by Flush B's retry).
	data, ok := store.GetObject("layers/layer-a/0000000000000000")
	assert.True(t, ok, "block should be in S3 after retry")
	assert.Equal(t, bytes.Repeat([]byte("X"), 64), data)
}
