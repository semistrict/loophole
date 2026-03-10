package snapshotter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	snapshotspb "github.com/semistrict/loophole/internal/containerdapi/services/snapshots/v1"
	"github.com/semistrict/loophole/snapshotter"
	"github.com/semistrict/loophole/storage2"
)

const testVolumeSize = 128 * 1024 * 1024 // 128 MB

func newTestSnapshotter(t *testing.T) *snapshotter.Snapshotter {
	t.Helper()
	store := loophole.NewMemStore()
	vm := storage2.NewVolumeManager(store, t.TempDir(), storage2.Config{}, nil, nil)
	t.Cleanup(func() { vm.Close(t.Context()) })

	backend := fsbackend.NewLwext4(vm)
	t.Cleanup(func() { backend.Close(t.Context()) })

	snapshotDir := t.TempDir()
	ss := snapshotter.New(backend, snapshotDir)
	ss.DefaultVolumeSize = testVolumeSize
	return ss
}

func TestPrepareAndStat(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	resp, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{
		Key:    "layer-1",
		Labels: map[string]string{"test": "true"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Mounts, 1)
	require.Equal(t, "bind", resp.Mounts[0].Type)
	require.Contains(t, resp.Mounts[0].Options, "rw")

	// Stat returns the snapshot metadata.
	stat, err := ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "layer-1"})
	require.NoError(t, err)
	require.Equal(t, "layer-1", stat.Info.Name)
	require.Equal(t, snapshotspb.Kind_ACTIVE, stat.Info.Kind)
	require.Equal(t, "true", stat.Info.Labels["test"])
}

func TestPrepareAlreadyExists(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "dup"})
	require.NoError(t, err)

	_, err = ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "dup"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestCommitAndClone(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	// Prepare a root layer.
	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "active-1"})
	require.NoError(t, err)

	// Commit it.
	_, err = ss.Commit(ctx, &snapshotspb.CommitSnapshotRequest{
		Key:  "active-1",
		Name: "committed-1",
	})
	require.NoError(t, err)

	// Old key should be gone.
	_, err = ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "active-1"})
	require.Error(t, err)

	// New name should exist and be committed.
	stat, err := ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "committed-1"})
	require.NoError(t, err)
	require.Equal(t, snapshotspb.Kind_COMMITTED, stat.Info.Kind)

	// Prepare a child from the committed parent.
	resp, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{
		Key:    "child-1",
		Parent: "committed-1",
	})
	require.NoError(t, err)
	require.Len(t, resp.Mounts, 1)
	require.Contains(t, resp.Mounts[0].Options, "rw")

	stat, err = ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "child-1"})
	require.NoError(t, err)
	require.Equal(t, "committed-1", stat.Info.Parent)
	require.Equal(t, snapshotspb.Kind_ACTIVE, stat.Info.Kind)
}

func TestView(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	// Create and commit a base layer.
	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "base"})
	require.NoError(t, err)
	_, err = ss.Commit(ctx, &snapshotspb.CommitSnapshotRequest{Key: "base", Name: "base-committed"})
	require.NoError(t, err)

	// Create a read-only view.
	resp, err := ss.View(ctx, &snapshotspb.ViewSnapshotRequest{
		Key:    "view-1",
		Parent: "base-committed",
	})
	require.NoError(t, err)
	require.Len(t, resp.Mounts, 1)
	require.Contains(t, resp.Mounts[0].Options, "ro")

	stat, err := ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "view-1"})
	require.NoError(t, err)
	require.Equal(t, snapshotspb.Kind_VIEW, stat.Info.Kind)
}

func TestRemove(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "to-remove"})
	require.NoError(t, err)

	_, err = ss.Remove(ctx, &snapshotspb.RemoveSnapshotRequest{Key: "to-remove"})
	require.NoError(t, err)

	_, err = ss.Stat(ctx, &snapshotspb.StatSnapshotRequest{Key: "to-remove"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestMounts(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "m1"})
	require.NoError(t, err)

	resp, err := ss.Mounts(ctx, &snapshotspb.MountsRequest{Key: "m1"})
	require.NoError(t, err)
	require.Len(t, resp.Mounts, 1)
	require.Equal(t, "bind", resp.Mounts[0].Type)
	require.Contains(t, resp.Mounts[0].Options, "rw")
}

func TestUpdateLabels(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{
		Key:    "u1",
		Labels: map[string]string{"old": "value"},
	})
	require.NoError(t, err)

	resp, err := ss.Update(ctx, &snapshotspb.UpdateSnapshotRequest{
		Info: &snapshotspb.Info{
			Name:   "u1",
			Labels: map[string]string{"new": "label"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "label", resp.Info.Labels["new"])
	require.Empty(t, resp.Info.Labels["old"])
}

func TestList(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "a"})
	require.NoError(t, err)
	_, err = ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "b"})
	require.NoError(t, err)

	// List uses a streaming interface. We need a mock server stream.
	stream := &listStream{}
	err = ss.List(&snapshotspb.ListSnapshotsRequest{}, stream)
	require.NoError(t, err)
	require.Len(t, stream.responses, 1) // single batch
	require.Len(t, stream.responses[0].Info, 2)

	names := map[string]bool{}
	for _, info := range stream.responses[0].Info {
		names[info.Name] = true
	}
	require.True(t, names["a"])
	require.True(t, names["b"])
}

func TestUsage(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "usage-test"})
	require.NoError(t, err)

	resp, err := ss.Usage(ctx, &snapshotspb.UsageRequest{Key: "usage-test"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestCommitNotActive(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	// Create and commit.
	_, err := ss.Prepare(ctx, &snapshotspb.PrepareSnapshotRequest{Key: "x"})
	require.NoError(t, err)
	_, err = ss.Commit(ctx, &snapshotspb.CommitSnapshotRequest{Key: "x", Name: "x-committed"})
	require.NoError(t, err)

	// Create a view from the committed snapshot.
	_, err = ss.View(ctx, &snapshotspb.ViewSnapshotRequest{Key: "view-x", Parent: "x-committed"})
	require.NoError(t, err)

	// Committing a view should fail.
	_, err = ss.Commit(ctx, &snapshotspb.CommitSnapshotRequest{Key: "view-x", Name: "bad"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not active")
}

func TestCleanup(t *testing.T) {
	ss := newTestSnapshotter(t)
	ctx := t.Context()

	resp, err := ss.Cleanup(ctx, &snapshotspb.CleanupRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
}
