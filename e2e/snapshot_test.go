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
	parentMP := mountpoint(t, "parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "parent"}))
	err := b.Mount(ctx, "parent", parentMP)
	require.NoError(t, err)

	tfs := newTestFS(t, b, parentMP)
	randomMD5 := writeTestFiles(t, tfs)

	childMP := mountpoint(t, "child")
	err = b.Clone(t.Context(), parentMP, "child", childMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	verifyTestFiles(t, newTestFS(t, b, childMP), randomMD5)
}

func TestE2E_ChildCanWriteIndependently(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Type: defaultVolumeType(), Volume: "parent"}))
	err := b.Mount(ctx, "parent", parentMP)
	require.NoError(t, err)

	parentFS := newTestFS(t, b, parentMP)
	parentFS.WriteFile(t, "parent.txt", []byte("from parent\n"))
	if needsKernelExt4() {
		syncFS(t, parentMP)
	}

	childMP := mountpoint(t, "child")
	err = b.Clone(t.Context(), parentMP, "child", childMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	childFS := newTestFS(t, b, childMP)
	childFS.WriteFile(t, "child.txt", []byte("from child\n"))

	require.Equal(t, "from parent\n", string(childFS.ReadFile(t, "parent.txt")))
	require.Equal(t, "from child\n", string(childFS.ReadFile(t, "child.txt")))
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
