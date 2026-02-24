//go:build linux

package e2e

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Fallocate tests are Linux-only: they need FUSE + fallocate syscall.

func punchHole(t *testing.T, fd int, offset, length int64) {
	t.Helper()
	err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, offset, length)
	require.NoError(t, err, "fallocate punch hole failed")
}

func requireReadOnlyErr(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "operation should fail on read-only snapshot")
	var errno unix.Errno
	require.True(t, errors.As(err, &errno), "expected errno, got %T: %v", err, err)
	switch errno {
	case unix.EROFS, unix.EPERM, unix.EACCES, unix.EIO:
		return
	default:
		t.Fatalf("expected read-only-style errno, got %v", errno)
	}
}

func TestE2E_PunchHoleZerosData(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint("punch-zeros")
	os.MkdirAll(mp, 0o755)
	require.NoError(t, b.Create(ctx, "punch-zeros"))
	err := b.Mount(ctx, "punch-zeros", mp)
	require.NoError(t, err)

	// Write 8K of 'A's.
	data := make([]byte, 8192)
	for i := range data {
		data[i] = 'A'
	}
	err = os.WriteFile(mp+"/testfile", data, 0o644)
	require.NoError(t, err)
	syncFS(t)

	// Punch hole in second half.
	fd, err := unix.Open(mp+"/testfile", unix.O_RDWR, 0)
	require.NoError(t, err)
	punchHole(t, fd, 4096, 4096)
	unix.Close(fd)
	syncFS(t)

	content, err := os.ReadFile(mp + "/testfile")
	require.NoError(t, err)
	require.Equal(t, 8192, len(content))

	expected := make([]byte, 4096)
	for i := range expected {
		expected[i] = 'A'
	}
	require.Equal(t, expected, content[:4096])
	require.Equal(t, make([]byte, 4096), content[4096:8192])
}

func TestE2E_PunchHoleTombstone(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()

	// Write to parent, clone, punch in child.
	require.NoError(t, b.Create(ctx, "tomb-parent"))
	parentDev, err := b.DeviceMount(ctx, "tomb-parent")
	require.NoError(t, err)

	xData := make([]byte, 4096)
	for i := range xData {
		xData[i] = 'X'
	}
	f, err := os.OpenFile(parentDev, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(xData, 0)
	require.NoError(t, err)
	f.Close()
	syncFS(t)

	childDev, err := b.DeviceClone(t.Context(), "tomb-parent", "tomb-child")
	require.NoError(t, err)

	fd, err := unix.Open(childDev, unix.O_RDWR, 0)
	require.NoError(t, err)
	punchHole(t, fd, 0, 4096)
	unix.Close(fd)
	syncFS(t)

	// Read back: should be zeros.
	f, err = os.OpenFile(childDev, os.O_RDONLY, 0)
	require.NoError(t, err)
	buf := make([]byte, 4096)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	f.Close()
	require.Equal(t, make([]byte, 4096), buf)
}

func TestE2E_PunchHoleSnapshotIsReadOnly(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, "tomb-ro-parent"))
	parentDev, err := b.DeviceMount(ctx, "tomb-ro-parent")
	require.NoError(t, err)

	data := make([]byte, 4096)
	for i := range data {
		data[i] = 'R'
	}
	f, err := os.OpenFile(parentDev, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	syncFS(t)

	require.NoError(t, b.DeviceSnapshot(ctx, "tomb-ro-parent", "tomb-ro-snap"))

	snapDev, err := b.DeviceMount(ctx, "tomb-ro-snap")
	require.NoError(t, err)

	fd, err := unix.Open(snapDev, unix.O_RDWR, 0)
	if err != nil {
		requireReadOnlyErr(t, err)
		return
	}
	defer unix.Close(fd)

	err = unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, 0, 4096)
	requireReadOnlyErr(t, err)
}

func TestE2E_PunchHoleNoAncestorDeletesBlock(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, "punch-del"))
	dev, err := b.DeviceMount(ctx, "punch-del")
	require.NoError(t, err)

	yData := make([]byte, 4096)
	for i := range yData {
		yData[i] = 'Y'
	}
	f, err := os.OpenFile(dev, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(yData, 0)
	require.NoError(t, err)
	f.Close()
	syncFS(t)

	fd, err := unix.Open(dev, unix.O_RDWR, 0)
	require.NoError(t, err)
	punchHole(t, fd, 0, 4096)
	unix.Close(fd)
	syncFS(t)

	// Read back: should be zeros.
	f, err = os.OpenFile(dev, os.O_RDONLY, 0)
	require.NoError(t, err)
	buf := make([]byte, 4096)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	f.Close()
	require.Equal(t, make([]byte, 4096), buf)
}

func TestE2E_PunchHolePartialBlock(t *testing.T) {
	skipKernelOnly(t)
	if isNBDMode() {
		t.Skip("NBD does not support sub-block-aligned punch holes")
	}
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, "punch-partial"))
	dev, err := b.DeviceMount(ctx, "punch-partial")
	require.NoError(t, err)

	zData := make([]byte, 4096)
	for i := range zData {
		zData[i] = 'Z'
	}
	f, err := os.OpenFile(dev, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(zData, 0)
	require.NoError(t, err)
	f.Close()
	syncFS(t)

	fd, err := unix.Open(dev, unix.O_RDWR, 0)
	require.NoError(t, err)
	punchHole(t, fd, 1024, 2048)
	unix.Close(fd)
	syncFS(t)

	f, err = os.OpenFile(dev, os.O_RDONLY, 0)
	require.NoError(t, err)
	block := make([]byte, 4096)
	_, err = f.ReadAt(block, 0)
	require.NoError(t, err)
	f.Close()

	zExpect := make([]byte, 1024)
	for i := range zExpect {
		zExpect[i] = 'Z'
	}
	require.Equal(t, zExpect, block[:1024])
	require.Equal(t, make([]byte, 2048), block[1024:3072])
	require.Equal(t, zExpect, block[3072:4096])
}
