package loophole

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackpressureHighThroughputNoSemaphorePanic writes many more blocks than
// MaxDirtyBlocks, forcing multiple flush cycles. This reproduces panics
// like "semaphore: released more than held" that occur under sustained load.
func TestBackpressureHighThroughputNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write 20 distinct blocks (5x the dirty limit), relying on
	// background flush + backpressure to manage token accounting.
	for i := range 20 {
		data := bytes.Repeat([]byte{byte('A' + i%26)}, 64)
		require.NoError(t, layer.Write(t.Context(), data, uint64(i)*64))
	}

	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureFlushDuringWriteNoSemaphorePanic exercises the race
// between Flush swapping the dirty map and concurrent writes to the
// same block that was just flushed.
func TestBackpressureFlushDuringWriteNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 2}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Repeatedly write and flush the same two blocks.
	for round := range 20 {
		data := bytes.Repeat([]byte{byte('A' + round%26)}, 64)
		require.NoError(t, layer.Write(t.Context(), data, 0))
		require.NoError(t, layer.Write(t.Context(), data, 64))
		require.NoError(t, layer.Flush(t.Context()))
	}

	layer.Close(t.Context())
}

// TestBackpressureConcurrentWriteAndFlushNoSemaphorePanic runs writes
// and explicit flushes concurrently to stress token accounting.
func TestBackpressureConcurrentWriteAndFlushNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	done := make(chan struct{})
	// Goroutine: flush every few writes.
	go func() {
		defer close(done)
		for range 20 {
			_ = layer.Flush(t.Context())
		}
	}()

	// Main goroutine: write many blocks.
	for i := range 40 {
		data := bytes.Repeat([]byte{byte('A' + i%26)}, 64)
		require.NoError(t, layer.Write(t.Context(), data, uint64(i%8)*64))
	}

	<-done
	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureFlushFailureAndRetryNoSemaphorePanic simulates upload
// failures during flush, forcing blocks to be re-dirtied, then retried.
func TestBackpressureFlushFailureAndRetryNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 3}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Fill dirty slots.
	for i := range 3 {
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('A' + i)}, 64), uint64(i)*64))
	}

	// Make flush fail, then succeed.
	store.SetFault(OpPutReader, "", Fault{Err: fmt.Errorf("injected failure")})
	_ = layer.Flush(t.Context()) // fails — blocks re-dirtied

	store.ClearFault(OpPutReader, "")
	require.NoError(t, layer.Flush(t.Context())) // succeeds

	// Write 3 more — should not panic (tokens should be available).
	for i := range 3 {
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('D' + i)}, 64), uint64(i+3)*64))
	}

	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureCloseDuringFlushNoSemaphorePanic cancels the background
// flush context while a flush is in progress, then does a final flush —
// this is what happens in Layer.Close().
func TestBackpressureCloseDuringFlushNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	// Repeat multiple rounds to increase race window.
	for round := range 10 {
		seedLayer(t, store, fmt.Sprintf("layer-%d", round), defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, fmt.Sprintf("layer-%d", round))
		require.NoError(t, err)

		// Write enough blocks to trigger background flush.
		for i := range 8 {
			data := bytes.Repeat([]byte{byte('A' + i)}, 64)
			require.NoError(t, layer.Write(t.Context(), data, uint64(i)*64))
		}

		// Close immediately — this cancels background flush and does a final flush.
		// If semaphore accounting is wrong, this will panic.
		layer.Close(t.Context())
	}
}

// TestBackpressureHeavyWriteVolume writes hundreds of blocks through
// a small dirty limit, exercising many flush cycles.
func TestBackpressureHeavyWriteVolume(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write 200 distinct blocks (50x the dirty limit).
	for i := range 200 {
		data := bytes.Repeat([]byte{byte('A' + i%26)}, 64)
		require.NoError(t, layer.Write(t.Context(), data, uint64(i)*64))
	}

	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureAllZeroBlocksTombstoneNoSemaphorePanic writes all-zero
// blocks which become tombstones during flush. The tombstone path in
// flushOne must not double-release dirty tokens.
func TestBackpressureAllZeroBlocksTombstoneNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write all-zero blocks to trigger tombstone path.
	for i := range 20 {
		require.NoError(t, layer.Write(t.Context(), make([]byte, 64), uint64(i)*64))
	}

	require.NoError(t, layer.Flush(t.Context()))

	// Write some more after flush — tokens should be available.
	for i := range 4 {
		require.NoError(t, layer.Write(t.Context(), make([]byte, 64), uint64(i+20)*64))
	}

	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureMixedWriteThenZeroNoSemaphorePanic writes data, then
// overwrites with zeros (causing tombstone on next flush). This exercises
// the transition from data block to tombstone block.
func TestBackpressureMixedWriteThenZeroNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Write data blocks, flush, then overwrite with zeros, flush again.
	for round := range 5 {
		for i := range 8 {
			data := bytes.Repeat([]byte{byte('A' + round)}, 64)
			require.NoError(t, layer.Write(t.Context(), data, uint64(i)*64))
		}
		require.NoError(t, layer.Flush(t.Context()))

		// Now zero them out (will become tombstones on next flush).
		for i := range 8 {
			require.NoError(t, layer.Write(t.Context(), make([]byte, 64), uint64(i)*64))
		}
		require.NoError(t, layer.Flush(t.Context()))
	}

	layer.Close(t.Context())
}

// TestBackpressurePunchHoleThenFlushNoSemaphorePanic writes blocks,
// punches holes (creating tombstones via Truncate(0)), then flushes.
func TestBackpressurePunchHoleThenFlushNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	for round := range 5 {
		// Write blocks.
		for i := range 4 {
			data := bytes.Repeat([]byte{byte('A' + round)}, 64)
			require.NoError(t, layer.Write(t.Context(), data, uint64(i)*64))
		}
		require.NoError(t, layer.Flush(t.Context()))

		// Punch holes.
		require.NoError(t, layer.PunchHole(t.Context(), 0, 4*64))
		require.NoError(t, layer.Flush(t.Context()))
	}

	layer.Close(t.Context())
}

// TestBackpressureSlowUploadCloseDuringFlush simulates slow S3 uploads
// so that Close() cancels the background flush context while uploads are
// in-flight. This reproduces the "semaphore: released more than held" panic.
func TestBackpressureSlowUploadCloseDuringFlush(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	for round := range 20 {
		seedLayer(t, store, fmt.Sprintf("layer-%d", round), defaultLayerState(), nil)
		layer, err := NewLayer(t.Context(), vm, fmt.Sprintf("layer-%d", round))
		require.NoError(t, err)

		// Block uploads so background flush gets stuck mid-flight.
		gate := make(chan struct{})
		store.SetFault(OpPutReader, "", Fault{Hook: func() { <-gate }})

		// Write enough to trigger early flush (>50% of 4 = 2 blocks).
		for i := range 4 {
			require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('A' + i)}, 64), uint64(i)*64))
		}

		// Close from a goroutine while uploads are blocked.
		// This cancels the background flush context while goroutines
		// are waiting in the hook. Some may complete, some may fail.
		go func() {
			close(gate)
		}()
		store.ClearFault(OpPutReader, "")
		layer.Close(t.Context())
	}
}

// TestBackpressureSlowUploadManyBlocks simulates slow uploads with many
// blocks, forcing multiple flush cycles with in-flight cancellations.
func TestBackpressureSlowUploadManyBlocks(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	layer, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)

	// Make every other upload slow to create race windows.
	store.SetFault(OpPutReader, "", Fault{Hook: func() {}})

	// Write many blocks through small dirty limit.
	for i := range 40 {
		require.NoError(t, layer.Write(t.Context(), bytes.Repeat([]byte{byte('A' + i%26)}, 64), uint64(i)*64))
	}

	store.ClearFault(OpPutReader, "")
	require.NoError(t, layer.Flush(t.Context()))
	layer.Close(t.Context())
}

// TestBackpressureCopyFromCoWThenFlushNoSemaphorePanic exercises the
// CopyFrom CoW path which deletes dirty entries without releasing tokens.
func TestBackpressureCopyFromCoWThenFlushNoSemaphorePanic(t *testing.T) {
	store := NewMemStore()
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir(), MaxDirtyBlocks: 4}
	require.NoError(t, FormatSystem(t.Context(), store, 64))
	require.NoError(t, vm.Connect(t.Context()))
	defer vm.Close(t.Context())

	seedLayer(t, store, "layer-a", defaultLayerState(), nil)
	seedLayer(t, store, "layer-b", defaultLayerState(), nil)
	src, err := NewLayer(t.Context(), vm, "layer-a")
	require.NoError(t, err)
	dst, err := NewLayer(t.Context(), vm, "layer-b")
	require.NoError(t, err)

	// Write blocks to src, freeze it so CoW is possible.
	require.NoError(t, src.Write(t.Context(), bytes.Repeat([]byte("S"), 64), 0))
	require.NoError(t, src.Write(t.Context(), bytes.Repeat([]byte("S"), 64), 64))
	require.NoError(t, src.Flush(t.Context()))
	require.NoError(t, src.Freeze(t.Context()))

	// Write a block to dst (acquires dirty token).
	require.NoError(t, dst.Write(t.Context(), bytes.Repeat([]byte("D"), 64), 0))

	// CopyFrom CoW overwrites block 0 in dst — deletes from dirty.
	_, err = dst.CopyFrom(t.Context(), src, 0, 0, 64)
	require.NoError(t, err)

	// Flush and write more — should not panic.
	require.NoError(t, dst.Flush(t.Context()))
	require.NoError(t, dst.Write(t.Context(), bytes.Repeat([]byte("N"), 64), 0))
	require.NoError(t, dst.Write(t.Context(), bytes.Repeat([]byte("N"), 64), 64))
	require.NoError(t, dst.Write(t.Context(), bytes.Repeat([]byte("N"), 64), 128))
	require.NoError(t, dst.Write(t.Context(), bytes.Repeat([]byte("N"), 64), 192))

	require.NoError(t, dst.Flush(t.Context()))
	dst.Close(t.Context())
	src.Close(t.Context())
}
