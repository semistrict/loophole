package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lwext4"
)

const daemonVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB — matches fsbackend.defaultVolumeSize

// TestE2E_Lwext4Format100GB verifies that lwext4 can format a 100GB volume
// (the default daemon size). This reproduces a bug where ext4_mkfs returned -1
// at large volume sizes.
func TestE2E_Lwext4Format100GB(t *testing.T) {
	skipE2E(t)
	ctx := t.Context()

	inst := uniqueInstance(t)
	store, err := loophole.NewS3Store(ctx, inst, defaultS3Options())
	require.NoError(t, err)
	_ = loophole.FormatSystem(ctx, store, 4*1024*1024)

	vm, err := loophole.NewVolumeManager(ctx, store, t.TempDir(), 20, 200)
	require.NoError(t, err)
	defer vm.Close(ctx)

	vol, err := vm.NewVolume(ctx, "big")
	require.NoError(t, err)

	vio := vol.IO(ctx)
	fs, err := lwext4.Format(vio, daemonVolumeSize, nil)
	require.NoError(t, err, "lwext4.Format must succeed at 100GB")
	defer fs.Close()

	// Verify we can write and read back a file at this size.
	ino, err := fs.Mknod(lwext4.RootIno, "test.txt", 0o644)
	require.NoError(t, err)
	f, err := fs.OpenFile(ino, 1) // O_WRONLY
	require.NoError(t, err)
	_, err = f.Write([]byte("100GB volume works\n"))
	require.NoError(t, err)
	f.Close()

	f2, err := fs.Open(ino)
	require.NoError(t, err)
	buf := make([]byte, 64)
	n, err := f2.Read(buf)
	f2.Close()
	require.NoError(t, err)
	require.Equal(t, "100GB volume works\n", string(buf[:n]))
}

// TestE2E_Lwext4CanMountCurrentFormat creates a volume using the current
// mode's backend (which may use mkfs.ext4 or lwext4 mkfs), writes a file,
// unmounts, then re-opens the raw volume with lwext4 in-process to verify
// the on-disk format is lwext4-compatible.
func TestE2E_Lwext4CanMountCurrentFormat(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "compat")

	require.NoError(t, b.Create(ctx, "compat"))
	require.NoError(t, b.Mount(ctx, "compat", mp))

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "hello.txt", []byte("lwext4 compat test\n"))
	if needsKernelExt4() {
		syncFS(t)
	}

	require.NoError(t, b.Unmount(ctx, mp))

	// Re-open the raw volume and mount with lwext4 in-process.
	vol, err := b.VM().OpenVolume(ctx, "compat")
	require.NoError(t, err)

	vio := vol.IO(ctx)
	fs, err := lwext4.Mount(vio, defaultVolumeSize)
	require.NoError(t, err, "lwext4 must be able to mount a volume created by %s mode", mode())
	defer fs.Close()

	// Verify the file is readable through lwext4.
	ino, err := fs.Lookup(lwext4.RootIno, "hello.txt")
	require.NoError(t, err)
	f, err := fs.Open(ino)
	require.NoError(t, err)
	buf := make([]byte, 64)
	n, err := f.Read(buf)
	f.Close()
	require.NoError(t, err)
	require.Equal(t, "lwext4 compat test\n", string(buf[:n]))
}
