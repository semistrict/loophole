package storage

import (
	"context"
	"testing"
	"time"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func TestOSLocalFSMkdirAll(t *testing.T) {
	path := t.TempDir() + "/a/b/c"
	require.NoError(t, osLocalFS{}.MkdirAll(path, 0o700))
}

func TestManagerOpenVolumeBranches(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	v1, err := m.NewVolume(CreateParams{Volume: "one", Size: 8 * PageSize})
	require.NoError(t, err)

	ref1, err := getVolumeRef(ctx, store.At("volumes"), "one")
	require.NoError(t, err)
	v1.releaseLease(ctx)

	same, err := m.openVolume("one", ref1)
	require.NoError(t, err)
	require.Same(t, v1, same)

	mOther := newTestManager(t, store, testConfig)
	v2, err := mOther.NewVolume(CreateParams{Volume: "two", Size: 8 * PageSize})
	require.NoError(t, err)
	require.NoError(t, mOther.CloseVolume(v2.Name()))
	ref2, err := getVolumeRef(ctx, store.At("volumes"), "two")
	require.NoError(t, err)

	_, err = m.openVolume("two", ref2)
	require.Error(t, err)
	require.Contains(t, err.Error(), `manager already has volume "one" open; cannot open "two"`)
}

func TestManagerOpenVolumeRejectsFrozenLayer(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	_, err := m.NewVolume(CreateParams{Volume: "vol", Size: 8 * PageSize})
	require.NoError(t, err)

	ref, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)

	meta := map[string]string{
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"frozen_at":  time.Now().UTC().Format(time.RFC3339),
	}
	require.NoError(t, store.At("layers/"+ref.LayerID).SetMeta(ctx, "index.json", meta))

	require.NoError(t, m.Close())

	m2 := newTestManager(t, store, testConfig)
	_, err = m2.OpenVolume("vol")
	require.Error(t, err)
	require.Contains(t, err.Error(), `points to frozen layer`)
}

func TestVolumeRefreshWrapper(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	writer := newTestManager(t, store, testConfig)
	v, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)

	page0 := make([]byte, PageSize)
	page0[0] = 0xAA
	require.NoError(t, v.Write(page0, 0))
	require.NoError(t, v.Flush())

	follower, err := openLayer(ctx, layerParams{store: store, id: v.layer.id, config: testConfig})
	require.NoError(t, err)
	defer follower.Close()

	page1 := make([]byte, PageSize)
	page1[0] = 0xBB
	require.NoError(t, follower.Write(page1, PageSize))
	require.NoError(t, follower.Flush())

	before, err := v.ReadAt(ctx, PageSize, PageSize)
	require.NoError(t, err)
	require.NotEqual(t, byte(0xBB), before[0])

	require.NoError(t, v.Refresh(ctx))

	after, err := v.ReadAt(ctx, PageSize, PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(0xBB), after[0])
}
