package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func pageWithByte(b byte) []byte {
	page := make([]byte, PageSize)
	page[0] = b
	return page
}

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

	same, err := m.openVolume("one", ref1, false)
	require.NoError(t, err)
	require.Same(t, v1, same)

	mOther := newTestManager(t, store, testConfig)
	v2, err := mOther.NewVolume(CreateParams{Volume: "two", Size: 8 * PageSize})
	require.NoError(t, err)
	require.NoError(t, mOther.CloseVolume(v2.Name()))
	ref2, err := getVolumeRef(ctx, store.At("volumes"), "two")
	require.NoError(t, err)

	_, err = m.openVolume("two", ref2, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), `manager already has volume "one" open; cannot open "two"`)
}

func TestManagerOpenVolumeReadOnlyFollowsActiveWriter(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	writer := newTestManager(t, store, Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  10 * time.Millisecond,
	})
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)

	require.NoError(t, wv.Write(pageWithByte(0xAB), 0))
	require.NoError(t, wv.Flush())

	refBefore, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.Equal(t, wv.leaseToken(), refBefore.LeaseToken)

	reader := newTestManager(t, store, writer.config)
	follower, err := reader.OpenVolumeReadOnly("vol")
	require.NoError(t, err)
	require.True(t, follower.readOnly)
	require.Nil(t, follower.layer.flushNotify)
	require.Nil(t, follower.layer.writeNotify)

	buf := make([]byte, PageSize)
	_, err = follower.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0xAB), buf[0])

	refAfter, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.Equal(t, refBefore.LeaseToken, refAfter.LeaseToken)
	require.NoError(t, reader.Close())
}

func TestReadOnlyFollowerSeesCommittedSnapshotUntilRefresh(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  10 * time.Millisecond,
	}

	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)
	require.NoError(t, wv.Write(pageWithByte(0x11), 0))
	require.NoError(t, wv.Flush())

	reader := newTestManager(t, store, cfg)
	follower, err := reader.OpenVolumeReadOnly("vol")
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	require.NoError(t, wv.Write(pageWithByte(0x22), PageSize))
	require.NoError(t, wv.Flush())

	buf := make([]byte, PageSize)
	_, err = follower.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(0x00), buf[0])

	require.NoError(t, follower.Refresh(ctx))
	_, err = follower.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(0x22), buf[0])
}

func TestReadOnlyFollowerClosePreservesWriterLease(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  10 * time.Millisecond,
	}

	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)

	refBefore, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.Equal(t, wv.leaseToken(), refBefore.LeaseToken)

	reader := newTestManager(t, store, cfg)
	follower, err := reader.OpenVolumeReadOnly("vol")
	require.NoError(t, err)
	require.NotNil(t, follower)
	require.NoError(t, reader.Close())

	refAfter, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.Equal(t, refBefore.LeaseToken, refAfter.LeaseToken)
	require.Equal(t, wv.leaseToken(), refAfter.LeaseToken)
}

func TestReadOnlyFollowerTracksLatestVolumeRefOnFreshOpen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  10 * time.Millisecond,
	}

	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)
	require.NoError(t, wv.Write(pageWithByte(0x31), 0))
	require.NoError(t, wv.Flush())

	refBefore, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)

	require.NoError(t, wv.Clone("vol-clone"))
	require.NoError(t, wv.Write(pageWithByte(0x32), PageSize))
	require.NoError(t, wv.Flush())

	refAfter, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.NotEqual(t, refBefore.LayerID, refAfter.LayerID)

	reader := newTestManager(t, store, cfg)
	follower, err := reader.OpenVolumeReadOnly("vol")
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	buf := make([]byte, PageSize)
	_, err = follower.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0x31), buf[0])
	_, err = follower.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(0x32), buf[0])
}

func TestMultipleReadOnlyFollowersDoNotInterfereWithWriter(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 4 * PageSize,
		FlushInterval:  10 * time.Millisecond,
	}

	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)
	require.NoError(t, wv.Write(pageWithByte(0x41), 0))
	require.NoError(t, wv.Flush())

	refBefore, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		reader := newTestManager(t, store, cfg)
		follower, err := reader.OpenVolumeReadOnly("vol")
		require.NoError(t, err, "reader %d", i)
		buf := make([]byte, PageSize)
		_, err = follower.Read(ctx, buf, 0)
		require.NoError(t, err, "reader %d", i)
		require.Equal(t, byte(0x41), buf[0], "reader %d", i)
		require.NoError(t, reader.Close(), "reader %d", i)
	}

	refAfter, err := getVolumeRef(ctx, store.At("volumes"), "vol")
	require.NoError(t, err)
	require.Equal(t, refBefore.LeaseToken, refAfter.LeaseToken)
}

func TestReadOnlyVolumeRejectsMutation(t *testing.T) {
	store := objstore.NewMemStore()

	writer := newTestManager(t, store, testConfig)
	v, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 16 * PageSize})
	require.NoError(t, err)

	followerMgr := newTestManager(t, store, testConfig)
	follower, err := followerMgr.OpenVolumeReadOnly("vol")
	require.NoError(t, err)
	defer func() { require.NoError(t, followerMgr.Close()) }()

	require.EqualError(t, follower.Write(make([]byte, PageSize), 0), `volume "vol" is read-only`)
	require.EqualError(t, follower.PunchHole(0, PageSize), `volume "vol" is read-only`)
	require.EqualError(t, follower.Flush(), `volume "vol" is read-only`)
	_, err = follower.CopyFrom(v, 0, 0, PageSize)
	require.EqualError(t, err, `volume "vol" is read-only`)
	require.EqualError(t, follower.EnableDirectWriteback(), `volume "vol" is read-only`)
}

func TestOpenVolumeReadOnlyRejectsSecondVolumeInSameManager(t *testing.T) {
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	_, err := m.NewVolume(CreateParams{Volume: "one", Size: 8 * PageSize})
	require.NoError(t, err)

	other := newTestManager(t, store, testConfig)
	_, err = other.NewVolume(CreateParams{Volume: "two", Size: 8 * PageSize})
	require.NoError(t, err)

	_, err = m.OpenVolumeReadOnly("two")
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf(`manager already has volume %q open; cannot open %q`, "one", "two"))
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
