package loophole

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- AcquireRef / ReleaseRef basics ---

func TestAcquireRefOnLiveVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.ReleaseRef(t.Context()))
}

func TestAcquireRefOnClosedVolumeReturnsError(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.Close(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	err := vol.AcquireRef()
	assert.ErrorIs(t, err, ErrVolumeClosed)
}

func TestMultipleAcquireRefAllSucceed(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	for range 5 {
		require.NoError(t, vol.AcquireRef())
	}
	for range 5 {
		require.NoError(t, vol.ReleaseRef(t.Context()))
	}
}

// --- Close + handle ref interaction ---

func TestCloseWithOutstandingRefKeepsVolumeAlive(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.Close(t.Context()))

	// Volume should still be usable — the FUSE handle keeps it alive.
	require.NoError(t, vol.Write(t.Context(), 0, []byte("still alive")))

	buf := make([]byte, 11)
	_, err := vol.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("still alive"), buf)

	// Dropping the last ref should destroy it.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	assert.Panics(t, func() {
		_, _ = vol.Read(t.Context(), 0, make([]byte, 1))
	})
}

func TestCloseWithMultipleOutstandingRefs(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.Close(t.Context()))

	// Still alive — 3 FUSE refs outstanding.
	require.NoError(t, vol.Write(t.Context(), 0, []byte("ok")))

	// Release two — still alive.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.Write(t.Context(), 0, []byte("ok")))

	// Release the last one — destroyed.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	assert.Panics(t, func() { _ = vol.Name() })
}

func TestCloseWithNoRefsDestroysImmediately(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.Close(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- Double close ---

func TestDoubleCloseIsIdempotent(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.Close(t.Context()))
	// Second close should be a no-op, not an error or panic.
	assert.NoError(t, vol.Close(t.Context()))
	require.NoError(t, vm.Close(t.Context()))
}

func TestVMCloseAfterExplicitCloseIsHarmless(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "v")
	require.NoError(t, err)

	// Explicitly close, then VM shutdown closes everything again.
	require.NoError(t, vol.Close(t.Context()))
	assert.NoError(t, vm.Close(t.Context()))
}

// --- ReleaseRef edge cases ---

func TestReleaseRefOnDestroyedVolumeIsNoop(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.Close(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	// Should not error or panic.
	assert.NoError(t, vol.ReleaseRef(t.Context()))
}

func TestReleaseRefWithoutAcquireDestroysVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	// No AcquireRef was called. ReleaseRef decrements the namespace
	// ref (refs 1→0), which destroys the volume. This is a caller
	// bug but must not crash.
	require.NoError(t, vol.ReleaseRef(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- AcquireRef after Close (but with outstanding refs) ---

func TestAcquireRefAfterCloseWithOutstandingRef(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer vm.Close(t.Context())

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.Close(t.Context()))

	// Volume is still alive (1 FUSE ref). Can we acquire another?
	// This simulates a second FUSE open on a volume that's been
	// removed from the namespace but still has live file handles.
	require.NoError(t, vol.AcquireRef())

	// Release both.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.ReleaseRef(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- Volumes() listing ---

func TestVolumesExcludesClosedVolumes(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer vm.Close(t.Context())

	_, err := vm.NewVolume(t.Context(), "alive")
	require.NoError(t, err)

	vol2, err := vm.NewVolume(t.Context(), "closed")
	require.NoError(t, err)
	require.NoError(t, vol2.Close(t.Context()))

	names := vm.Volumes()
	assert.Contains(t, names, "alive")
	assert.NotContains(t, names, "closed")
}

// --- Flush on destroy ---

func TestDestroyFlushesDirtyData(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "flush-on-destroy")
	require.NoError(t, err)

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.Write(t.Context(), 0, []byte("dirty data here!")))
	require.NoError(t, vol.Close(t.Context()))

	// Data is dirty, not yet in S3. The last ReleaseRef should
	// trigger closeVolume which flushes.
	require.NoError(t, vol.ReleaseRef(t.Context()))

	// Reopen from a fresh VM and verify the data was flushed.
	vm2, err := NewVolumeManager(t.Context(), store, t.TempDir(), 20, 200)
	require.NoError(t, err)
	defer vm2.Close(t.Context())

	vol2, err := vm2.OpenVolume(t.Context(), "flush-on-destroy")
	require.NoError(t, err)

	buf := make([]byte, 16)
	_, err = vol2.Read(t.Context(), 0, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("dirty data here!"), buf)
}
