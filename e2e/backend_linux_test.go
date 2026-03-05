//go:build linux

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/juicefs"
	"github.com/semistrict/loophole/nbdvm"
)

func newPlatformBackend(t testing.TB, vm loophole.VolumeManager, inst loophole.Instance, store loophole.ObjectStore) fsbackend.Service {
	t.Helper()
	switch mode() {
	case loophole.ModeNBD:
		b, err := fsbackend.NewNBD(vm, &nbdvm.Options{})
		require.NoError(t, err)
		return b
	case loophole.ModeTestNBDTCP:
		b, err := fsbackend.NewNBDTCP(vm, &nbdvm.Options{})
		require.NoError(t, err)
		return b
	case loophole.ModeFUSE:
		dir := loophole.Dir(t.TempDir())
		b, err := fsbackend.NewFUSE(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{})
		require.NoError(t, err)
		return b
	case loophole.ModeFuseFS:
		if fsType() == loophole.FSJuiceFS {
			return juicefs.NewFUSE(vm, store)
		}
		return fsbackend.NewLwext4FUSE(vm)
	default:
		return nil
	}
}
