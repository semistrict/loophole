package loophole

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackpressureBlocksWriteAtLimit verifies that writes block when
// the dirty block limit is reached, and unblock after a flush.
func TestBackpressureBlocksWriteAtLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		// maxDirtyBlocks=2: third dirty block should block.
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 2}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Stop background flush so nothing drains dirty automatically.
		layer.stopFlush()
		<-layer.flushStopped

		// Write two blocks — fills the dirty limit.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

		// Third write to a NEW block should block.
		blocked := make(chan error, 1)
		go func() {
			blocked <- layer.Write(t.Context(), bytes.Repeat([]byte("C"), 64), 128)
		}()

		synctest.Wait()
		select {
		case <-blocked:
			t.Fatal("third write should have blocked but returned immediately")
		default:
		}

		// Explicit flush frees dirty slots — write should unblock.
		require.NoError(t, layer.Flush(t.Context()))
		synctest.Wait()
		assert.NoError(t, <-blocked)

		// All three blocks should be readable.
		buf := make([]byte, 64)
		_, err = layer.Read(t.Context(), buf, 128)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte("C"), 64), buf)

		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestBackpressureRewriteSameBlockDoesNotConsumeTwice verifies that
// rewriting an already-dirty block does not consume an additional
// dirty token.
func TestBackpressureRewriteSameBlockDoesNotConsumeTwice(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 1}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write block 0 twice — the second write should NOT block
	// because block 0 is already dirty (same token).
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
	require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 0))

	buf := make([]byte, 64)
	_, err = layer.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), buf)
}

// TestBackpressureWriteUnblocksOnContextCancel verifies that a blocked
// write returns when the context is cancelled.
func TestBackpressureWriteUnblocksOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 1}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Stop background flush so nothing drains dirty.
		layer.stopFlush()
		<-layer.flushStopped

		// Fill the single dirty slot.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

		// Second block write with cancellable context.
		ctx, cancel := context.WithCancel(t.Context())
		blocked := make(chan error, 1)
		go func() {
			blocked <- layer.Write(ctx, bytes.Repeat([]byte("B"), 64), 64)
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		err = <-blocked
		assert.Error(t, err, "blocked write should fail on context cancel")

		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestBackpressureEarlyFlushTriggered verifies that writing past 50%
// of the dirty limit triggers an early flush (before the 30s timer).
func TestBackpressureEarlyFlushTriggered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		// MaxDirtyBlocks=4: soft threshold at 2.
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Write 2 blocks to cross the 50% threshold.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

		// Let the early flush goroutine run.
		synctest.Wait()

		// The blocks should be flushed without waiting for the 30s timer.
		_, ok := store.GetObject("layers/layer-a/0000000000000000")
		assert.True(t, ok, "block 0 should be flushed by early flush trigger")

		_, ok = store.GetObject("layers/layer-a/0000000000000001")
		assert.True(t, ok, "block 1 should be flushed by early flush trigger")

		// Clean up: close layer to stop background flush goroutine.
		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestBackpressurePerLayerIndependent verifies that backpressure on one
// layer does not affect writes to another layer (per-layer dirty limit).
func TestBackpressurePerLayerIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 1}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		seedLayer(t, store, "layer-b", defaultLayerState(), nil)
		layerA, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)
		layerB, err := NewLayer(t.Context(), vm, "layer-b")
		require.NoError(t, err)

		// Stop background flush so dirty stays full.
		layerA.stopFlush()
		<-layerA.flushStopped
		layerB.stopFlush()
		<-layerB.flushStopped

		// Fill layer A's dirty limit.
		require.NoError(t, layerA.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

		// Layer B should still accept writes (independent dirty count).
		require.NoError(t, layerB.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 0))

		layerA.Close(t.Context())
		layerB.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestBackpressurePunchHoleRespectsLimit verifies that PunchHole
// full-block punches also go through backpressure.
func TestBackpressurePunchHoleRespectsLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 1}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Stop background flush so nothing drains dirty.
		layer.stopFlush()
		<-layer.flushStopped

		// Write one block to fill the dirty limit.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

		// PunchHole on a different block should block.
		blocked := make(chan error, 1)
		go func() {
			blocked <- layer.PunchHole(t.Context(), 64, 64)
		}()

		synctest.Wait()
		select {
		case <-blocked:
			t.Fatal("PunchHole should have blocked — dirty limit reached")
		default:
		}

		// Explicit flush frees dirty slot — PunchHole unblocks.
		require.NoError(t, layer.Flush(t.Context()))
		synctest.Wait()
		assert.NoError(t, <-blocked)

		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestBackpressureFlushReleasesCorrectCount verifies that a flush of
// N blocks releases exactly N dirty tokens.
func TestBackpressureFlushReleasesCorrectCount(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 3}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Fill all 3 slots.
	for i := range 3 {
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('A' + i)}, 64), uint64(i)*64))
	}

	// Flush all 3.
	require.NoError(t, layer.Flush(t.Context()))

	// Should be able to dirty 3 more blocks without blocking.
	for i := range 3 {
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('D' + i)}, 64), uint64(i+3)*64))
	}
}

// TestBackpressureFailedUploadRedirties verifies that a failed upload
// re-adds the block to dirty so it counts against the limit.
func TestBackpressureFailedUploadRedirties(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 1}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Stop background flush so we control flushing.
		layer.stopFlush()
		<-layer.flushStopped

		// Write one block to fill the limit.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))

		// Make flush fail.
		store.SetFault(OpPutReader, "", Fault{Err: fmt.Errorf("injected failure")})
		err = layer.Flush(t.Context())
		assert.Error(t, err)

		// Block should be re-dirtied after failure.
		layer.mu.Lock()
		_, isDirty := layer.dirty[BlockIdx(0)]
		layer.mu.Unlock()
		assert.True(t, isDirty, "block should be re-dirtied after failed upload")

		// A write to a NEW block should block because dirty limit is full.
		blocked := make(chan error, 1)
		go func() {
			blocked <- layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64)
		}()

		synctest.Wait()
		select {
		case <-blocked:
			t.Fatal("write should block — failed upload re-dirtied the block")
		default:
		}

		// Successful flush frees the slot — write unblocks.
		store.ClearAllFaults()
		require.NoError(t, layer.Flush(t.Context()))
		synctest.Wait()
		assert.NoError(t, <-blocked)

		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}

// TestFlushOneBlockFailureDoesNotBlockOthers verifies that a persistent
// upload failure for one block does not prevent other blocks from being
// flushed successfully.
func TestFlushOneBlockFailureDoesNotBlockOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 10}
		require.NoError(t, FormatSystem(t.Context(), store, 64))
		require.NoError(t, vm.Connect(t.Context()))

		seedLayer(t, store, "layer-a", defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Write 3 blocks.
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte("C"), 64), 128))

		// Fail uploads for block 1 only.
		block1Key := "layers/layer-a/0000000000000001"
		store.SetFault(OpPutReader, block1Key, Fault{Err: fmt.Errorf("disk on fire")})

		err = layer.Flush(t.Context())
		assert.Error(t, err, "flush should report the failure")

		// Blocks 0 and 2 should have been uploaded despite block 1 failing.
		_, ok := store.GetObject("layers/layer-a/0000000000000000")
		assert.True(t, ok, "block 0 should be flushed even though block 1 failed")

		_, ok = store.GetObject("layers/layer-a/0000000000000002")
		assert.True(t, ok, "block 2 should be flushed even though block 1 failed")

		// Block 1 should still be dirty (re-added after failure).
		layer.mu.Lock()
		_, block1Dirty := layer.dirty[BlockIdx(1)]
		layer.mu.Unlock()
		assert.True(t, block1Dirty, "block 1 should be re-dirtied after failed upload")

		// Clear fault and flush again — block 1 should succeed now.
		store.ClearFault(OpPutReader, block1Key)
		require.NoError(t, layer.Flush(t.Context()))

		_, ok = store.GetObject(block1Key)
		assert.True(t, ok, "block 1 should be flushed after fault is cleared")

		layer.Close(t.Context())
		vm.Close(t.Context())
	})
}
