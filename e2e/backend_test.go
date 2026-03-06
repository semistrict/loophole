package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func newBackendForMode(t testing.TB, vm loophole.VolumeManager, inst loophole.Instance) fsbackend.Service {
	t.Helper()
	if b := newPlatformBackend(t, vm, inst); b != nil {
		return b
	}
	// Cross-platform userspace backends.
	switch mode() {
	case loophole.ModeInProcess:
		return fsbackend.NewBackend(vm, map[string]fsbackend.AnyDriver{
			loophole.VolumeTypeExt4: fsbackend.NewLwext4Driver(),
		})
	default:
		return fsbackend.NewBackend(vm, map[string]fsbackend.AnyDriver{
			loophole.VolumeTypeExt4: fsbackend.NewLwext4Driver(),
		})
	}
}
