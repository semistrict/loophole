package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/nbdvm"
)

func newBackendForMode(t *testing.T, vm *loophole.VolumeManager, inst loophole.Instance) *fsbackend.Backend {
	t.Helper()
	if isKernelMode() {
		mode := loophole.ModeFromEnv()
		switch mode {
		case loophole.ModeNBD:
			b, err := fsbackend.NewNBD(vm, &nbdvm.Options{})
			require.NoError(t, err)
			return b
		default:
			dir := loophole.Dir(t.TempDir())
			b, err := fsbackend.NewFUSE(dir.Fuse(inst), vm, &fuseblockdev.Options{})
			require.NoError(t, err)
			return b
		}
	}
	return fsbackend.NewLwext4(vm, &fsbackend.Lwext4Options{VolumeSize: defaultVolumeSize})
}
