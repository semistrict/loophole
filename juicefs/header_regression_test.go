package juicefs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lsm"
)

func TestJuiceFSParentAndSnapshotRetainMetaHeaderAfterUnmount(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()
	vm := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{}, nil, nil)
	t.Cleanup(func() { _ = vm.Close(ctx) })

	svc := NewInProcess(vm, Config{ObjStore: store})
	const (
		volumeName   = "juicefs-parent"
		snapshotName = "juicefs-snap"
		mountpoint   = "/mnt/test"
	)

	require.NoError(t, svc.Create(ctx, loophole.CreateParams{
		Volume: volumeName,
		Size:   64 << 20,
		Type:   loophole.VolumeTypeJuiceFS,
	}))
	require.NoError(t, svc.Mount(ctx, volumeName, mountpoint))

	fs, err := svc.FS(mountpoint)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile("/hello.txt", []byte("hello from juicefs\n"), 0o644))
	require.NoError(t, fs.MkdirAll("/subdir/nested", 0o755))
	require.NoError(t, fs.WriteFile("/subdir/nested/deep.txt", []byte("deep file\n"), 0o644))

	require.NoError(t, svc.Snapshot(ctx, mountpoint, snapshotName))
	require.NoError(t, svc.Unmount(ctx, mountpoint))

	// Verify both parent and snapshot volumes have the bbolt header.
	for _, name := range []string{volumeName, snapshotName} {
		vol, err := vm.OpenVolume(ctx, name)
		require.NoError(t, err, name)

		var magic [8]byte
		_, err = vol.Read(ctx, magic[:], 0)
		require.NoError(t, err, name)
		require.Equal(t, "LHBOLT01", string(magic[:]), name)

		require.NoError(t, vol.ReleaseRef(ctx), name)
	}
}
