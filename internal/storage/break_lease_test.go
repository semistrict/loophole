package storage

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"

	"github.com/semistrict/loophole/internal/objstore"
	"github.com/stretchr/testify/require"
)

func TestBreakLeaseGraceful(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := objstore.NewMemStore()
		leases := store.At("leases")

		holder := objstore.NewLeaseSession(leases)
		holder.Handle("release", func(context.Context, json.RawMessage) (any, error) {
			return map[string]string{"status": "released"}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))
		t.Cleanup(func() {
			require.NoError(t, holder.Close(context.Background()))
		})

		require.NoError(t, putBreakLeaseTestVolumeRef(t.Context(), store, "vol", volumeRef{
			LayerID:    "layer-1",
			LeaseToken: holder.Token(),
		}))

		graceful, err := BreakLease(t.Context(), store, "vol", false)
		require.NoError(t, err)
		require.True(t, graceful)

		ref := mustBreakLeaseTestVolumeRef(t.Context(), t, store, "vol")
		require.Empty(t, ref.LeaseToken)

		_, _, err = objstore.ReadJSON[map[string]any](t.Context(), leases, holder.Token()+".json")
		require.ErrorIs(t, err, objstore.ErrNotFound)
	})
}

func TestBreakLeaseForceClearsStaleLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := objstore.NewMemStore()

		require.NoError(t, putBreakLeaseTestVolumeRef(t.Context(), store, "vol", volumeRef{
			LayerID:    "layer-1",
			LeaseToken: "stale-token",
		}))

		graceful, err := BreakLease(t.Context(), store, "vol", true)
		require.NoError(t, err)
		require.False(t, graceful)

		ref := mustBreakLeaseTestVolumeRef(t.Context(), t, store, "vol")
		require.Empty(t, ref.LeaseToken)
	})
}

func TestBreakLeaseRequiresForceWhenHolderMissing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := objstore.NewMemStore()

		require.NoError(t, putBreakLeaseTestVolumeRef(t.Context(), store, "vol", volumeRef{
			LayerID:    "layer-1",
			LeaseToken: "stale-token",
		}))

		graceful, err := BreakLease(t.Context(), store, "vol", false)
		require.Error(t, err)
		require.False(t, graceful)

		ref := mustBreakLeaseTestVolumeRef(t.Context(), t, store, "vol")
		require.Equal(t, "stale-token", ref.LeaseToken)
	})
}

func putBreakLeaseTestVolumeRef(ctx context.Context, store objstore.ObjectStore, name string, ref volumeRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	return store.At("volumes").PutIfNotExists(ctx, name+"/index.json", data)
}

func mustBreakLeaseTestVolumeRef(ctx context.Context, t *testing.T, store objstore.ObjectStore, name string) volumeRef {
	t.Helper()
	ref, _, err := objstore.ReadJSON[volumeRef](ctx, store.At("volumes"), name+"/index.json")
	require.NoError(t, err)
	return ref
}
