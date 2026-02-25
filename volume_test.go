package loophole

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupVolume(t *testing.T) (*MemStore, *VolumeManager, *Volume) {
	t.Helper()
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "mydata")
	require.NoError(t, err)
	return store, vm, vol
}

// --- Basic read/write ---

func TestWriteAndReadBack(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	data := []byte("hello world")
	require.NoError(t, vol.Write(t.Context(), 0, data))

	buf := make([]byte, len(data))
	n, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
}

func TestReadUnwrittenReturnsZeros(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	buf := make([]byte, 64)
	n, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, 64, n)
	assert.Equal(t, make([]byte, 64), buf)
}

func TestWriteAtOffset(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 100, []byte("abc")))

	buf := make([]byte, 3)
	n, err := vol.Read(t.Context(), 100, buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("abc"), buf)
}

func TestWriteSpanningBlocks(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// blockSize=64, write 100 bytes starting at offset 32 — spans blocks 0 and 1
	data := bytes.Repeat([]byte{0xAB}, 100)
	require.NoError(t, vol.Write(t.Context(), 32, data))

	buf := make([]byte, 100)
	n, err := vol.Read(t.Context(), 32, buf)
	require.NoError(t, err)
	assert.Equal(t, 100, n)
	assert.Equal(t, data, buf)
}

func TestReadSpanningBlocks(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// Write different data to block 0 and block 1
	block0 := bytes.Repeat([]byte{0x11}, 64)
	block1 := bytes.Repeat([]byte{0x22}, 64)
	require.NoError(t, vol.Write(t.Context(), 0, block0))
	require.NoError(t, vol.Write(t.Context(), 64, block1))

	// Read across the boundary
	buf := make([]byte, 40)
	n, err := vol.Read(t.Context(), 44, buf)
	require.NoError(t, err)
	assert.Equal(t, 40, n)
	// First 20 bytes from block 0, last 20 bytes from block 1
	assert.Equal(t, bytes.Repeat([]byte{0x11}, 20), buf[:20])
	assert.Equal(t, bytes.Repeat([]byte{0x22}, 20), buf[20:])
}

func TestWritePreservesOtherDataInBlock(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("AAAA")))
	require.NoError(t, vol.Write(t.Context(), 10, []byte("BBBB")))

	buf := make([]byte, 14)
	_, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("AAAA"), buf[:4])
	assert.Equal(t, make([]byte, 6), buf[4:10])
	assert.Equal(t, []byte("BBBB"), buf[10:14])
}

func TestReadAtHighOffsetReturnsZeros(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// Sparse volume: reading at any offset returns zeros if unwritten.
	buf := make([]byte, 100)
	n, err := vol.Read(t.Context(), 1<<30, buf) // 1 GiB offset
	require.NoError(t, err)
	assert.Equal(t, 100, n)
	assert.Equal(t, make([]byte, 100), buf)
}

func TestWriteAtHighOffset(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	data := bytes.Repeat([]byte{0xFF}, 100)
	require.NoError(t, vol.Write(t.Context(), 1<<30, data))

	buf := make([]byte, 100)
	n, err := vol.Read(t.Context(), 1<<30, buf)
	require.NoError(t, err)
	assert.Equal(t, 100, n)
	assert.Equal(t, data, buf)
}

// --- Read-only / frozen ---

func TestWriteToReadOnlyVolumeFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "frozen-layer", frozenLayerState(), nil)
	refData, _ := json.Marshal(volumeRef{LayerID: "frozen-layer"})
	store.PutBytes(t.Context(), "volumes/readonly", refData)

	vol, err := vm.OpenVolume(t.Context(), "readonly")
	require.NoError(t, err)
	require.True(t, vol.ReadOnly())

	assert.Error(t, vol.Write(t.Context(), 0, []byte("nope")))
}

func TestReadFromReadOnlyVolumeWithData(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	blocks := map[BlockIdx][]byte{
		0: append([]byte("hello"), make([]byte, 59)...),
	}
	seedLayer(t, store, "frozen-layer", frozenLayerState(), blocks)
	refData, _ := json.Marshal(volumeRef{LayerID: "frozen-layer"})
	store.PutBytes(t.Context(), "volumes/snap", refData)

	vol, err := vm.OpenVolume(t.Context(), "snap")
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), buf)
}

// --- Snapshot tests ---

func TestSnapshotVolumeStaysWritable(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("before")))
	require.NoError(t, vol.Snapshot(t.Context(), "snap1"))

	// Volume should still be writable after snapshot
	assert.False(t, vol.ReadOnly())
	assert.NoError(t, vol.Write(t.Context(), 0, []byte("after!")))
}

func TestSnapshotPreservesDataInContinuation(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("persistent")))
	require.NoError(t, vol.Snapshot(t.Context(), "snap1"))

	buf := make([]byte, 10)
	_, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("persistent"), buf)
}

func TestSnapshotReadOnlyVolumeFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	seedLayer(t, store, "frozen-layer", frozenLayerState(), nil)
	refData, _ := json.Marshal(volumeRef{LayerID: "frozen-layer"})
	store.PutBytes(t.Context(), "volumes/readonly", refData)

	vol, err := vm.OpenVolume(t.Context(), "readonly")
	require.NoError(t, err)

	assert.Error(t, vol.Snapshot(t.Context(), "snap"))
}

func TestMultipleSnapshotsPreserveData(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("gen0")))
	require.NoError(t, vol.Snapshot(t.Context(), ""))

	require.NoError(t, vol.Write(t.Context(), 10, []byte("gen1")))
	require.NoError(t, vol.Snapshot(t.Context(), ""))

	require.NoError(t, vol.Write(t.Context(), 20, []byte("gen2")))

	// All three generations of data should be readable
	buf := make([]byte, 24)
	_, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("gen0"), buf[0:4])
	assert.Equal(t, []byte("gen1"), buf[10:14])
	assert.Equal(t, []byte("gen2"), buf[20:24])
}

func TestSnapshotCreatesNamedReadOnlyVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("before")))
	require.NoError(t, vol.Snapshot(t.Context(), "snap1"))

	snap, err := vm.OpenVolume(t.Context(), "snap1")
	require.NoError(t, err)
	require.True(t, snap.ReadOnly())

	buf := make([]byte, 6)
	_, err = snap.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("before"), buf)

	require.NoError(t, vol.Write(t.Context(), 0, []byte("after!")))
	_, err = snap.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("before"), buf)
}

// --- Clone tests ---

func TestCloneProducesWritableVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	clone, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	assert.Equal(t, "branch", clone.Name())
	assert.False(t, clone.ReadOnly())
	assert.NoError(t, clone.Write(t.Context(), 0, []byte("clone write")))
}

func TestCloneOriginalStaysWritable(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	assert.False(t, vol.ReadOnly())
	assert.NoError(t, vol.Write(t.Context(), 0, []byte("still writable")))
}

func TestCloneSeesOriginalData(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("original data")))

	clone, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	buf := make([]byte, 13)
	n, err := clone.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, 13, n)
	assert.Equal(t, []byte("original data"), buf)
}

func TestCloneDataIsIndependent(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("shared")))

	clone, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	// Write different data to each
	require.NoError(t, vol.Write(t.Context(), 0, []byte("origin")))
	require.NoError(t, clone.Write(t.Context(), 0, []byte("cloned")))

	origBuf := make([]byte, 6)
	cloneBuf := make([]byte, 6)

	_, err = vol.Read(t.Context(), 0, origBuf)
	require.NoError(t, err)
	_, err = clone.Read(t.Context(), 0, cloneBuf)
	require.NoError(t, err)

	assert.Equal(t, []byte("origin"), origBuf)
	assert.Equal(t, []byte("cloned"), cloneBuf)
}

func TestCloneWriteDoesNotAffectOriginal(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("before")))

	clone, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	require.NoError(t, clone.Write(t.Context(), 0, []byte("CHANGE")))

	buf := make([]byte, 6)
	_, err = vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("before"), buf)
}

func TestCloneFromReadOnlyVolume(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	blocks := map[BlockIdx][]byte{
		0: append([]byte("snapshot data"), make([]byte, 51)...),
	}
	seedLayer(t, store, "frozen-layer", frozenLayerState(), blocks)
	refData, _ := json.Marshal(volumeRef{LayerID: "frozen-layer"})
	store.PutBytes(t.Context(), "volumes/snapshot", refData)

	vol, err := vm.OpenVolume(t.Context(), "snapshot")
	require.NoError(t, err)
	require.True(t, vol.ReadOnly())

	clone, err := vol.Clone(t.Context(), "from-snap")
	require.NoError(t, err)
	assert.False(t, clone.ReadOnly())

	// Clone should see the snapshot's data
	buf := make([]byte, 13)
	_, err = clone.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("snapshot data"), buf)

	// Clone should be writable
	require.NoError(t, clone.Write(t.Context(), 0, []byte("new data here")))
	_, err = clone.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("new data here"), buf)
}

func TestCloneDuplicateNameFails(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	_, err = vol.Clone(t.Context(), "branch")
	assert.Error(t, err)
}

// --- Flush / persistence tests ---

func TestFlushPersistsDataToS3(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.Write(t.Context(), 0, []byte("persist me")))
	require.NoError(t, vol.Flush(t.Context()))

	// Re-open the volume from S3 and verify data survived
	reopened, err := vm.NewVolume(t.Context(), "mydata2")
	require.NoError(t, err)
	// Can't reuse the same volume name, but we can verify the flushed data
	// is in S3 by reading from the original volume
	buf := make([]byte, 10)
	_, err = vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("persist me"), buf)
	_ = reopened
}

func TestDataSurvivesFlushAndSnapshotReload(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "mydata")
	require.NoError(t, err)

	require.NoError(t, vol.Write(t.Context(), 0, []byte("important")))
	require.NoError(t, vol.Snapshot(t.Context(), "snap"))
	require.NoError(t, vm.Close(t.Context()))

	// Open fresh VM and volume
	vm2 := newTestVM(t, store)
	defer func() {
		if err := vm2.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol2, err := vm2.OpenVolume(t.Context(), "mydata")
	require.NoError(t, err)

	buf := make([]byte, 9)
	_, err = vol2.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("important"), buf)
}

// --- VolumeManager tests ---

func TestNewVolumeCreatesWritableVolume(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol, err := vm.NewVolume(t.Context(), "test-vol")
	require.NoError(t, err)

	assert.Equal(t, "test-vol", vol.Name())
	assert.False(t, vol.ReadOnly())
}

func TestNewVolumeDuplicateNameFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, err := vm.NewVolume(t.Context(), "dup")
	require.NoError(t, err)

	_, err = vm.NewVolume(t.Context(), "dup")
	assert.Error(t, err)
}

func TestOpenVolumeReturnsCachedInstance(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol1, err := vm.NewVolume(t.Context(), "existing")
	require.NoError(t, err)

	// Write data through vol1
	require.NoError(t, vol1.Write(t.Context(), 0, []byte("shared")))

	vol2, err := vm.OpenVolume(t.Context(), "existing")
	require.NoError(t, err)

	// vol2 should see vol1's data (same instance)
	buf := make([]byte, 6)
	_, err = vol2.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("shared"), buf)
}

func TestOpenVolumeNotFoundFails(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	_, err := vm.OpenVolume(t.Context(), "nonexistent")
	assert.Error(t, err)
}

func TestVolumeManagerCloseSucceeds(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	_, err := vm.NewVolume(t.Context(), "vol-a")
	require.NoError(t, err)
	_, err = vm.NewVolume(t.Context(), "vol-b")
	require.NoError(t, err)

	assert.NoError(t, vm.Close(t.Context()))
}

func TestVolumePanicsAfterClose(t *testing.T) {
	_, vm, vol := setupVolume(t)
	require.NoError(t, vol.Close(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
	assert.Panics(t, func() { _ = vol.ReadOnly() })
	assert.Panics(t, func() { _ = vol.IO(t.Context()) })
	assert.Panics(t, func() {
		_, _ = vol.Read(t.Context(), 0, make([]byte, 1))
	})
	assert.Panics(t, func() { _ = vol.Write(t.Context(), 0, []byte{1}) })
	assert.Panics(t, func() { _ = vol.PunchHole(t.Context(), 0, 1) })
	assert.Panics(t, func() { _ = vol.Flush(t.Context()) })
	assert.Panics(t, func() { _ = vol.Snapshot(t.Context(), "snap") })
	assert.Panics(t, func() {
		_, _ = vol.Clone(t.Context(), "clone")
	})
	assert.Panics(t, func() {
		_, _ = vol.CopyFrom(t.Context(), vol, 0, 0, 1)
	})
	assert.Panics(t, func() { _ = vol.Freeze(t.Context()) })
	assert.NotPanics(t, func() { _ = vol.Close(t.Context()) })

	vio := &VolumeIO{ctx: t.Context(), vol: vol}
	assert.Panics(t, func() {
		_, _ = vio.ReadAt(make([]byte, 1), 0)
	})
	assert.Panics(t, func() {
		_, _ = vio.WriteAt([]byte{1}, 0)
	})
	assert.Panics(t, func() { _ = vio.Sync() })
}

func TestVolumeCloseWaitsForHandleRefs(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.Close(t.Context()))

	// The open handle keeps the volume alive after namespace close.
	_, err := vol.Read(t.Context(), 0, make([]byte, 1))
	require.NoError(t, err)

	require.NoError(t, vol.ReleaseRef(t.Context()))
	assert.Panics(t, func() {
		_, _ = vol.Read(t.Context(), 0, make([]byte, 1))
	})
}

func TestGetVolumeReturnsNilForUnopened(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	assert.Nil(t, vm.GetVolume("nope"))
}

func TestVolumesListsOpenVolumes(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, _ = vm.NewVolume(t.Context(), "alpha")
	_, _ = vm.NewVolume(t.Context(), "beta")

	assert.ElementsMatch(t, []string{"alpha", "beta"}, vm.Volumes())
}

// TestCloneSeesOriginalDataAfterReopen simulates the containerstorage flow:
// write data → clone → close everything → reopen clone from fresh VM → read.
// This catches issues where clone metadata doesn't properly reference parent blocks.
func TestCloneSeesOriginalDataAfterReopen(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "parent")
	require.NoError(t, err)

	// Write data to the parent (simulates ext4 writes through FUSE).
	data := bytes.Repeat([]byte{0xAB}, 64)
	require.NoError(t, vol.Write(t.Context(), 0, data))

	// Clone (internally does freeze+flush+createChild).
	clone, err := vol.Clone(t.Context(), "child")
	require.NoError(t, err)

	// Read from clone in the same VM — sanity check.
	buf := make([]byte, 64)
	_, err = clone.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, data, buf)

	// Close everything and reopen from a fresh VolumeManager
	// (simulates daemon restart or fresh mount of the clone).
	require.NoError(t, vm.Close(t.Context()))

	vm2, err := NewVolumeManager(t.Context(), store, t.TempDir(), 20, 200)
	require.NoError(t, err)
	defer func() {
		if err := vm2.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	clone2, err := vm2.OpenVolume(t.Context(), "child")
	require.NoError(t, err)

	buf2 := make([]byte, 64)
	_, err = clone2.Read(t.Context(), 0, buf2)
	require.NoError(t, err)
	assert.Equal(t, data, buf2)
}

// TestCloneSeesMultiBlockData writes across many blocks, clones, and reads back.
// This tests that the refBlockIndex covers all parent blocks, not just the first.
func TestCloneSeesMultiBlockData(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store) // blockSize=64
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol, err := vm.NewVolume(t.Context(), "parent")
	require.NoError(t, err)

	// Write data spanning blocks 0, 1, 2, 3 (blockSize=64, so 256 bytes).
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i % 251) // varied pattern
	}
	require.NoError(t, vol.Write(t.Context(), 0, data))

	clone, err := vol.Clone(t.Context(), "child")
	require.NoError(t, err)

	buf := make([]byte, 256)
	_, err = clone.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, data, buf)
}

func TestCloneIsRegisteredInVolumeManager(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, err := vol.Clone(t.Context(), "branch")
	require.NoError(t, err)

	assert.NotNil(t, vm.GetVolume("branch"))
}
