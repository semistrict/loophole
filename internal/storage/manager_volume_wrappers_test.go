package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/semistrict/loophole/internal/objstore"
	"github.com/stretchr/testify/require"
)

func TestManagerCloseVolumeAndWaitClosed(t *testing.T) {
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 8 * PageSize, Type: VolumeTypeExt4})
	require.NoError(t, err)
	require.Same(t, v, m.Volume())
	require.Equal(t, []string{"vol"}, m.Volumes())
	require.Equal(t, PageSize, m.PageSize())

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- m.WaitClosed(context.Background(), "vol")
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("WaitClosed returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, m.CloseVolume("vol"))
	require.NoError(t, <-waitDone)
	require.Nil(t, m.Volume())
	require.Nil(t, m.GetVolume("vol"))
	require.Nil(t, m.Volumes())
	require.NoError(t, m.WaitClosed(context.Background(), "vol"))
}

func TestVolumeHelpersAndMetadataWrappers(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	v, err := m.NewVolume(CreateParams{
		Volume: "vol",
		Size:   8 * PageSize,
		Type:   VolumeTypeExt4,
		Labels: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	require.Equal(t, VolumeTypeExt4, v.VolumeType())

	page := make([]byte, PageSize)
	for i := range page {
		page[i] = 0xAB
	}
	require.NoError(t, v.Write(page, 0))
	require.NoError(t, v.ZeroRange(0, PageSize))

	buf, err := v.ReadAt(ctx, 0, PageSize)
	require.NoError(t, err)
	require.Equal(t, zeroPage[:], buf)

	v.layer.stopPeriodicFlush()
	v.layer.flushNotify = make(chan struct{}, 1)
	require.NoError(t, v.FlushLocal())
	select {
	case <-v.layer.flushNotify:
	default:
		t.Fatal("FlushLocal did not notify flush loop")
	}

	v.layer.flushNotify <- struct{}{}
	require.NoError(t, v.FlushLocal())
	select {
	case <-v.layer.flushNotify:
	default:
		t.Fatal("FlushLocal should preserve a pending notification")
	}

	v.directRefs = 1
	require.NoError(t, v.FlushLocal())
	select {
	case <-v.layer.flushNotify:
		t.Fatal("FlushLocal should be a no-op in direct mode")
	default:
	}
	v.directRefs = 0
	v.layer.flushNotify = nil
	v.layer.writeNotify = nil
	v.layer.startPeriodicFlush(context.Background())

	require.NoError(t, UpdateLabels(ctx, m.Store(), "vol", map[string]string{"env": "updated"}))

	info, err := GetVolumeInfo(ctx, m.Store(), "vol")
	require.NoError(t, err)
	require.Equal(t, "vol", info.Name)
	require.Equal(t, uint64(8*PageSize), info.Size)
	require.Equal(t, VolumeTypeExt4, info.Type)
	require.Equal(t, map[string]string{"env": "updated"}, info.Labels)

	wrappedInfo, err := v.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, info, wrappedInfo)

	cpID, err := v.Checkpoint()
	require.NoError(t, err)

	checkpoints, err := v.ListCheckpoints(ctx)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, cpID, checkpoints[0].ID)

	require.NoError(t, Clone(ctx, m.Store(), "vol", cpID, "clone"))

	m2 := newTestManager(t, m.Store(), testConfig)
	clone, err := m2.OpenVolume("clone")
	require.NoError(t, err)
	require.Equal(t, uint64(8*PageSize), clone.Size())
	require.Equal(t, VolumeTypeExt4, clone.VolumeType())

	debugInfo, ok := DebugInfo(v)
	require.True(t, ok)
	require.Equal(t, "vol", debugInfo.Name)
	require.Equal(t, VolumeTypeExt4, debugInfo.Type)
	require.Equal(t, int32(1), debugInfo.Refs)
	require.NotEmpty(t, debugInfo.Layer.LayerID)

	emptyInfo, ok := DebugInfo(nil)
	require.False(t, ok)
	require.Equal(t, VolumeDebugInfo{}, emptyInfo)
}

func TestVolumeHandleLeaseRelease(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 4 * PageSize})
	require.NoError(t, err)

	var released bool
	v.SetOnRemoteRelease(func(context.Context) {
		released = true
	})

	resp, err := v.handleLeaseRelease(ctx, json.RawMessage(`{"volume":"vol"}`))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"status": "ok"}, resp)
	require.True(t, released)
	require.Nil(t, m.Volume())

	_, err = v.handleLeaseRelease(ctx, json.RawMessage(`{"volume":"other"}`))
	require.Error(t, err)
}

func TestReleaseRefFlushesBeforeShutdown(t *testing.T) {
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 4 * PageSize})
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("abc12345"), PageSize/8)
	require.NoError(t, v.Write(payload, 0))
	require.NoError(t, v.ReleaseRef())

	m2 := newTestManager(t, store, testConfig)
	reopened, err := m2.OpenVolume("vol")
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.ReleaseRef()) }()

	got, err := reopened.ReadAt(context.Background(), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
