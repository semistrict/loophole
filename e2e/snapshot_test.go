package e2e

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

func TestE2E_SnapshotPreservesData(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "snap-pres-parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "snap-pres-parent"}))
	err := b.Mount(ctx, "snap-pres-parent", parentMP)
	require.NoError(t, err)

	tfs := newTestFS(t, b, parentMP)
	randomMD5 := writeTestFiles(t, tfs)

	childMP := mountpoint(t, "snap-pres-child")
	err = b.Clone(t.Context(), parentMP, "snap-pres-child", childMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	verifyTestFiles(t, newTestFS(t, b, childMP), randomMD5)
}

func TestE2E_ChildCanWriteIndependently(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "cwr-parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "cwr-parent"}))
	err := b.Mount(ctx, "cwr-parent", parentMP)
	require.NoError(t, err)

	parentFS := newTestFS(t, b, parentMP)
	parentFS.WriteFile(t, "parent.txt", []byte("from parent\n"))
	if needsKernelExt4() {
		syncFS(t, parentMP)
	}

	childMP := mountpoint(t, "cwr-child")
	err = b.Clone(t.Context(), parentMP, "cwr-child", childMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	childFS := newTestFS(t, b, childMP)
	childFS.WriteFile(t, "child.txt", []byte("from child\n"))

	require.Equal(t, "from parent\n", string(childFS.ReadFile(t, "parent.txt")))
	require.Equal(t, "from child\n", string(childFS.ReadFile(t, "child.txt")))
}

func TestE2E_SnapshotMountsReadOnly(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "sro-parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "sro-parent"}))
	require.NoError(t, b.Mount(ctx, "sro-parent", parentMP))

	tfs := newTestFS(t, b, parentMP)
	tfs.WriteFile(t, "hello.txt", []byte("snapshot test\n"))

	// Snapshot the volume (this freezes, branches, and thaws).
	require.NoError(t, b.Snapshot(ctx, parentMP, "sro-snap"))

	// Unmount parent so the snapshot can acquire the timeline lease.
	require.NoError(t, b.Unmount(ctx, parentMP))

	// Mount the snapshot — should succeed (read-only lwext4 mount).
	snapMP := mountpoint(t, "sro-snap")
	require.NoError(t, b.Mount(ctx, "sro-snap", snapMP))

	// Data from before the snapshot should be readable.
	snapFS := newTestFS(t, b, snapMP)
	data := snapFS.ReadFile(t, "hello.txt")
	require.Equal(t, "snapshot test\n", string(data))

	require.NoError(t, b.Unmount(ctx, snapMP))
}

func TestE2E_DeviceSnapshotParentStaysWritable(t *testing.T) {
	skipKernelOnly(t)

	b := newBackend(t)
	ctx := t.Context()
	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "frozen-parent"}))
	parentDev, err := b.DeviceAttach(ctx, "frozen-parent")
	require.NoError(t, err)

	err = b.DeviceSnapshot(t.Context(), "frozen-parent", "frozen-child")
	require.NoError(t, err)

	// Parent should continue on a new writable layer after snapshot.
	f, err := os.OpenFile(parentDev, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteAt([]byte{0, 0, 0, 0}, 0)
	require.NoError(t, err, "write to parent after snapshot should succeed")

	// Snapshot target should be read-only.
	childDev, err := b.DeviceAttach(t.Context(), "frozen-child")
	require.NoError(t, err)
	child, err := os.OpenFile(childDev, os.O_RDWR, 0)
	if err == nil {
		defer child.Close()
		_, err = child.WriteAt([]byte{1, 2, 3, 4}, 0)
	}
	require.Error(t, err, "write to snapshot volume should fail")
}
