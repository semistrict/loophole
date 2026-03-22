package fuseblockdev_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/semistrict/loophole/internal/fuseblockdev"
	"github.com/semistrict/loophole/internal/objstore"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func skipWithoutFuse(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("FUSE tests require root or fusermount")
	}
}

type fuseTestEnv struct {
	store *objstore.MemStore
	vm    *storage.Manager
	vol   *storage.Volume
	srv   *fuseblockdev.Server
	f     *os.File
}

func setupFuse(t *testing.T) *fuseTestEnv {
	t.Helper()
	skipWithoutFuse(t)

	store := objstore.NewMemStore()
	vm := &storage.Manager{ObjectStore: store}
	t.Cleanup(func() { vm.Close() })

	vol, err := vm.NewVolume(storage.CreateParams{Volume: "testvol", Size: 4096})
	require.NoError(t, err)

	mountDir := t.TempDir()
	srv, err := fuseblockdev.Start(mountDir, &fuseblockdev.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { srv.Unmount() })

	srv.Add("testvol", vol)

	f, err := os.OpenFile(srv.DevicePath("testvol"), os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	return &fuseTestEnv{store: store, vm: vm, vol: vol, srv: srv, f: f}
}

func TestFuseReadWrite(t *testing.T) {
	env := setupFuse(t)

	data := []byte("hello loophole")
	_, err := env.f.WriteAt(data, 0)
	require.NoError(t, err)

	buf := make([]byte, len(data))
	_, err = env.f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	buf2 := make([]byte, len(data))
	_, err = env.vol.Read(t.Context(), buf2, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf2)
}

func TestFuseUnwrittenReturnsZeros(t *testing.T) {
	env := setupFuse(t)

	buf := make([]byte, 64)
	_, err := env.f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 64), buf)
}

func TestFuseReadAtOffset(t *testing.T) {
	env := setupFuse(t)

	data := []byte("offset-test")
	_, err := env.f.WriteAt(data, 100)
	require.NoError(t, err)

	buf := make([]byte, len(data))
	_, err = env.f.ReadAt(buf, 100)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	before := make([]byte, 10)
	_, err = env.f.ReadAt(before, 90)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 10), before)
}

func TestFuseWriteSpanningBlocks(t *testing.T) {
	env := setupFuse(t)

	data := bytes.Repeat([]byte("X"), 100)
	_, err := env.f.WriteAt(data, 30)
	require.NoError(t, err)

	buf := make([]byte, 100)
	_, err = env.f.ReadAt(buf, 30)
	require.NoError(t, err)
	require.Equal(t, data, buf)
}

func TestFuseDeviceFileSize(t *testing.T) {
	env := setupFuse(t)

	info, err := env.f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(4096), info.Size())
}

func TestFuseFsync(t *testing.T) {
	env := setupFuse(t)

	data := []byte("sync me")
	_, err := env.f.WriteAt(data, 0)
	require.NoError(t, err)

	err = env.f.Sync()
	require.NoError(t, err)

	buf := make([]byte, len(data))
	_, err = env.f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf)
}

func TestFuseWritePastEnd(t *testing.T) {
	env := setupFuse(t)

	data := []byte("x")
	_, err := env.f.WriteAt(data, 4096)
	require.Error(t, err)
}

func TestFuseReaddir(t *testing.T) {
	env := setupFuse(t)

	entries, err := os.ReadDir(env.srv.MountDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "testvol", entries[0].Name())
	require.False(t, entries[0].IsDir())
}

func TestFuseAddVolume(t *testing.T) {
	env := setupFuse(t)

	vm2 := &storage.Manager{ObjectStore: env.store}
	t.Cleanup(func() { vm2.Close() })

	newvol, err := vm2.NewVolume(storage.CreateParams{Volume: "newvol", Size: 4096})
	require.NoError(t, err)
	env.srv.Add("newvol", newvol)

	f, err := os.OpenFile(env.srv.DevicePath("newvol"), os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteAt([]byte("hello new"), 0)
	require.NoError(t, err)

	buf := make([]byte, 9)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello new"), buf)
}

func TestFuseCopyFileRangeFullVolume(t *testing.T) {
	env := setupFuse(t)

	data := bytes.Repeat([]byte("C"), 256)
	_, err := env.f.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, env.f.Sync()) // flush writeback cache to storage

	vm2 := &storage.Manager{ObjectStore: env.store}
	t.Cleanup(func() { vm2.Close() })

	cloneVol, err := vm2.NewVolume(storage.CreateParams{Volume: "clone", Size: 4096})
	require.NoError(t, err)
	env.srv.Add("clone", cloneVol)

	dst, err := os.OpenFile(env.srv.DevicePath("clone"), os.O_RDWR, 0)
	require.NoError(t, err)
	defer dst.Close()

	n, err := cloneVol.CopyFrom(env.vol, 0, 0, 256)
	require.NoError(t, err)
	assert.Equal(t, uint64(256), n)

	buf := make([]byte, 256)
	_, err = dst.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)
}

func TestFuseCopyFileRangeIsCoW(t *testing.T) {
	env := setupFuse(t)

	data := bytes.Repeat([]byte("R"), 192)
	_, err := env.f.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, env.f.Sync()) // flush writeback cache to storage

	require.NoError(t, env.vol.Flush())

	vm2 := &storage.Manager{ObjectStore: env.store}
	t.Cleanup(func() { vm2.Close() })

	dstVol, err := vm2.NewVolume(storage.CreateParams{Volume: "refclone", Size: 4096})
	require.NoError(t, err)
	env.srv.Add("refclone", dstVol)

	n, err := dstVol.CopyFrom(env.vol, 0, 0, 192)
	require.NoError(t, err)
	assert.Equal(t, uint64(192), n)

	dst, err := os.OpenFile(env.srv.DevicePath("refclone"), os.O_RDWR, 0)
	require.NoError(t, err)
	defer dst.Close()

	buf := make([]byte, 192)
	_, err = dst.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)

	_, err = dst.WriteAt([]byte("MODIFIED"), 0)
	require.NoError(t, err)

	srcBuf := make([]byte, 8)
	_, err = env.f.ReadAt(srcBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("R"), 8), srcBuf)
}
