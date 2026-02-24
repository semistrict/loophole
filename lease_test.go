package loophole

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaseAcquireNoExisting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		err := lm.EnsureStarted(t.Context())
		require.NoError(t, err)

		// Lease file should exist.
		_, ok := store.GetObject("leases/" + lm.Token() + ".json")
		assert.True(t, ok, "lease file should exist after EnsureStarted")

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseCheckAvailableEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.EnsureStarted(t.Context()))

		// Empty token is always available.
		assert.NoError(t, lm.CheckAvailable(t.Context(), ""))

		// Our own token is available.
		assert.NoError(t, lm.CheckAvailable(t.Context(), lm.Token()))

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseCheckAvailableLiveHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		challenger := NewLeaseManager(leases)
		require.NoError(t, challenger.EnsureStarted(t.Context()))

		// Holder's lease is live — challenger should be blocked.
		err := challenger.CheckAvailable(t.Context(), holder.Token())
		assert.ErrorContains(t, err, "lease held by")

		require.NoError(t, holder.Close(t.Context()))
		require.NoError(t, challenger.Close(t.Context()))
	})
}

func TestLeaseCheckAvailableCleanShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		// Clean shutdown deletes the lease file.
		require.NoError(t, holder.Close(t.Context()))

		_, ok := store.GetObject("leases/" + holderToken + ".json")
		assert.False(t, ok, "lease file should be gone after Close")

		// New manager can take over — missing file means clean shutdown.
		challenger := NewLeaseManager(leases)
		require.NoError(t, challenger.EnsureStarted(t.Context()))
		assert.NoError(t, challenger.CheckAvailable(t.Context(), holderToken))

		require.NoError(t, challenger.Close(t.Context()))
	})
}

func TestLeaseCheckAvailableCrashExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		// Simulate crash: stop renewal without deleting lease file.
		holder.mu.Lock()
		if holder.stop != nil {
			holder.stop()
		}
		holder.mu.Unlock()
		<-holder.done

		// Advance time past lease expiry.
		time.Sleep(leaseDuration + time.Second)

		// Lease file still exists but is expired.
		_, ok := store.GetObject("leases/" + holderToken + ".json")
		assert.True(t, ok, "lease file should still exist after crash")

		// New manager can take over expired lease.
		challenger := NewLeaseManager(leases)
		require.NoError(t, challenger.EnsureStarted(t.Context()))
		assert.NoError(t, challenger.CheckAvailable(t.Context(), holderToken))

		require.NoError(t, challenger.Close(t.Context()))
	})
}

func TestLeaseCheckAvailableCrashNotYetExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		// Simulate crash: stop renewal without deleting lease file.
		holder.mu.Lock()
		if holder.stop != nil {
			holder.stop()
		}
		holder.mu.Unlock()
		<-holder.done

		// Don't advance time — lease is still valid.
		challenger := NewLeaseManager(leases)
		require.NoError(t, challenger.EnsureStarted(t.Context()))
		err := challenger.CheckAvailable(t.Context(), holderToken)
		assert.ErrorContains(t, err, "lease held by")

		require.NoError(t, challenger.Close(t.Context()))
	})
}

func TestLeaseRenewalKeepsAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		lm := NewLeaseManager(leases)
		require.NoError(t, lm.EnsureStarted(t.Context()))

		// Advance past one renewal but before expiry.
		time.Sleep(leaseRenewFreq + time.Second)

		// Lease should still be valid (was renewed).
		challenger := NewLeaseManager(leases)
		err := challenger.CheckAvailable(t.Context(), lm.Token())
		assert.ErrorContains(t, err, "lease held by")

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseRenewalSurvivesMultiplePeriods(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		lm := NewLeaseManager(leases)
		require.NoError(t, lm.EnsureStarted(t.Context()))

		// Advance well past the original expiry (3 full lease durations).
		time.Sleep(3 * leaseDuration)

		// Lease should still be valid because renewal kept extending it.
		challenger := NewLeaseManager(leases)
		err := challenger.CheckAvailable(t.Context(), lm.Token())
		assert.ErrorContains(t, err, "lease held by")

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseCloseDeletesFile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.EnsureStarted(t.Context()))
		token := lm.Token()

		_, ok := store.GetObject("leases/" + token + ".json")
		assert.True(t, ok)

		require.NoError(t, lm.Close(t.Context()))

		_, ok = store.GetObject("leases/" + token + ".json")
		assert.False(t, ok, "lease file should be deleted after Close")
	})
}

func TestLeaseCloseWithoutStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		// Close without ever starting — should be a no-op.
		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseEnsureStartedIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		require.NoError(t, lm.EnsureStarted(t.Context()))
		require.NoError(t, lm.EnsureStarted(t.Context()))
		require.NoError(t, lm.EnsureStarted(t.Context()))

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseLayerMountUnmount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm := newTestVM(t, store)
		seedLayer(t, store, "layer-a", defaultLayerState(), nil)

		layer, err := NewLayer(t.Context(), vm, "layer-a")
		require.NoError(t, err)

		// Mount should acquire lease.
		require.NoError(t, layer.Mount(t.Context()))

		// Second VM should fail to mount the same layer.
		vm2 := newTestVM(t, store)
		layer2, err := NewLayer(t.Context(), vm2, "layer-a")
		require.NoError(t, err)
		err = layer2.Mount(t.Context())
		assert.ErrorContains(t, err, "lease held by")

		// Unmount should release.
		require.NoError(t, layer.Unmount(t.Context()))

		// Now vm2 can mount.
		require.NoError(t, layer2.Mount(t.Context()))
		require.NoError(t, layer2.Unmount(t.Context()))

		// Close layers to stop background goroutines.
		require.NoError(t, layer.Close(t.Context()))
		require.NoError(t, layer2.Close(t.Context()))
		require.NoError(t, vm.lease.Close(t.Context()))
		require.NoError(t, vm2.lease.Close(t.Context()))
	})
}

func TestLeaseLayerMountAfterCrash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		vm1 := newTestVM(t, store)
		seedLayer(t, store, "layer-a", defaultLayerState(), nil)

		layer1, err := NewLayer(t.Context(), vm1, "layer-a")
		require.NoError(t, err)
		require.NoError(t, layer1.Mount(t.Context()))

		// Simulate crash: stop background goroutines without unmounting.
		layer1.stopFlush()
		<-layer1.flushStopped
		layer1.closed.Store(true)
		vm1.lease.mu.Lock()
		if vm1.lease.stop != nil {
			vm1.lease.stop()
		}
		vm1.lease.mu.Unlock()
		<-vm1.lease.done

		// Advance past lease expiry.
		time.Sleep(leaseDuration + time.Second)

		// New VM should be able to take over.
		vm2 := newTestVM(t, store)
		layer2, err := NewLayer(t.Context(), vm2, "layer-a")
		require.NoError(t, err)
		require.NoError(t, layer2.Mount(t.Context()))

		// Clean up.
		require.NoError(t, layer2.Close(t.Context()))
		require.NoError(t, vm2.lease.Close(t.Context()))
	})
}
