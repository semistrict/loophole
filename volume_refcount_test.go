package loophole

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- AcquireRef / ReleaseRef basics ---

func TestAcquireRefOnLiveVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.ReleaseRef(t.Context()))
}

func TestAcquireRefOnDestroyedVolumeReturnsError(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	err := vol.AcquireRef()
	assert.ErrorIs(t, err, ErrVolumeClosed)
}

func TestMultipleAcquireRefAllSucceed(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	for range 5 {
		require.NoError(t, vol.AcquireRef())
	}
	for range 5 {
		require.NoError(t, vol.ReleaseRef(t.Context()))
	}
}

// --- Ref counting interaction ---

func TestMultipleRefsKeepVolumeAlive(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.AcquireRef())
	require.NoError(t, vol.AcquireRef())

	// Release the namespace ref (the initial ref from NewVolume).
	require.NoError(t, vol.ReleaseRef(t.Context()))

	// Still alive — 3 acquired refs outstanding.
	require.NoError(t, vol.Write(t.Context(), []byte("ok"), 0))

	// Release two — still alive.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.Write(t.Context(), []byte("ok"), 0))

	// Release the last one — destroyed.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	assert.Panics(t, func() { _ = vol.Name() })
}

func TestReleaseRefDestroysImmediately(t *testing.T) {
	_, vm, vol := setupVolume(t)

	// Releasing the single namespace ref destroys the volume.
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- ReleaseRef edge cases ---

func TestExtraReleaseRefOnDestroyedVolumeIsNoop(t *testing.T) {
	_, vm, vol := setupVolume(t)

	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vm.Close(t.Context()))

	// refs is already 0; extra release should not error or panic.
	assert.NoError(t, vol.ReleaseRef(t.Context()))
}

func TestReleaseRefWithoutAcquireDestroysVolume(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	// No AcquireRef was called. ReleaseRef decrements the namespace
	// ref (refs 1→0), which destroys the volume. This is a caller
	// bug but must not crash.
	require.NoError(t, vol.ReleaseRef(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- AcquireRef while other refs outstanding ---

func TestAcquireRefWhileOtherRefsOutstanding(t *testing.T) {
	_, vm, vol := setupVolume(t)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	require.NoError(t, vol.AcquireRef())

	// Volume still alive (2 refs). Can acquire another.
	require.NoError(t, vol.AcquireRef())

	// Release all three (1 namespace + 2 acquired).
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.ReleaseRef(t.Context()))
	require.NoError(t, vol.ReleaseRef(t.Context()))

	assert.Panics(t, func() { _ = vol.Name() })
}

// --- Volumes() listing ---

func TestVolumesExcludesDestroyedVolumes(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	_, err := vm.NewVolume(t.Context(), "alive", 0)
	require.NoError(t, err)

	vol2, err := vm.NewVolume(t.Context(), "destroyed", 0)
	require.NoError(t, err)
	require.NoError(t, vol2.ReleaseRef(t.Context()))

	names := vm.Volumes()
	assert.Contains(t, names, "alive")
	assert.NotContains(t, names, "destroyed")
}

// --- Flush on destroy ---

func TestDestroyFlushesDirtyData(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)

	vol, err := vm.NewVolume(t.Context(), "flush-on-destroy", 0)
	require.NoError(t, err)

	require.NoError(t, vol.Write(t.Context(), []byte("dirty data here!"), 0))

	// Data is dirty, not yet in S3. ReleaseRef brings refs to 0,
	// triggering destroy → closeVolume which flushes.
	require.NoError(t, vol.ReleaseRef(t.Context()))

	// Reopen from a fresh VM and verify the data was flushed.
	vm2 := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm2.Connect(t.Context()))
	defer func() {
		if err := vm2.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol2, err := vm2.OpenVolume(t.Context(), "flush-on-destroy")
	require.NoError(t, err)

	buf := make([]byte, 16)
	_, err = vol2.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("dirty data here!"), buf)
}
