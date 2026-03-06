package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/juicefs"
)

func newBackendForMode(t testing.TB, vm loophole.VolumeManager, inst loophole.Instance, cfg juicefs.Config) fsbackend.Service {
	t.Helper()
	if b := newPlatformBackend(t, vm, inst, cfg); b != nil {
		return b
	}
	// Cross-platform userspace backends.
	switch mode() {
	case loophole.ModeInProcess:
		drivers := map[string]fsbackend.AnyDriver{
			loophole.VolumeTypeExt4: fsbackend.NewLwext4Driver(),
		}
		if cfg.ObjStore != nil {
			drivers[loophole.VolumeTypeJuiceFS] = juicefs.NewInProcessDriver(cfg)
		}
		return fsbackend.NewBackend(vm, drivers)
	default:
		return fsbackend.NewBackend(vm, map[string]fsbackend.AnyDriver{
			loophole.VolumeTypeExt4: fsbackend.NewLwext4Driver(),
		})
	}
}
