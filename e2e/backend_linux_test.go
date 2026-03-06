//go:build linux

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/nbdvm"
)

func newPlatformBackend(t testing.TB, vm loophole.VolumeManager, inst loophole.Instance) fsbackend.Service {
	t.Helper()
	switch mode() {
	case loophole.ModeNBD:
		nbd, err := fsbackend.NewNBDDriver(vm, &nbdvm.Options{})
		require.NoError(t, err)
		return fsbackend.New(vm, nbd, loophole.VolumeTypeExt4, loophole.VolumeTypeXFS)
	case loophole.ModeTestNBDTCP:
		nbd, err := fsbackend.NewNBDTCPDriver(vm, &nbdvm.Options{})
		require.NoError(t, err)
		return fsbackend.New(vm, nbd, loophole.VolumeTypeExt4, loophole.VolumeTypeXFS)
	case loophole.ModeFUSE:
		dir := loophole.Dir(t.TempDir())
		fuse, err := fsbackend.NewFUSEDriver(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{})
		require.NoError(t, err)
		return fsbackend.New(vm, fuse, loophole.VolumeTypeExt4, loophole.VolumeTypeXFS)
	case loophole.ModeFuseFS:
		return fsbackend.NewBackend(vm, map[string]fsbackend.AnyDriver{
			loophole.VolumeTypeExt4: fsbackend.NewLwext4FUSEDriver(),
		})
	default:
		return nil
	}
}
