//go:build darwin

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/juicefs"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, _ loophole.Dir, cfg juicefs.Config) (fsbackend.Service, error) {
	drivers := make(map[string]fsbackend.AnyDriver)
	switch inst.Mode {
	case loophole.ModeInProcess:
		drivers[loophole.VolumeTypeExt4] = fsbackend.NewLwext4Driver()
		if cfg.ObjStore != nil {
			drivers[loophole.VolumeTypeJuiceFS] = juicefs.NewInProcessDriver(cfg)
		}
	case loophole.ModeFuseFS:
		drivers[loophole.VolumeTypeExt4] = fsbackend.NewLwext4FUSEDriver()
		if cfg.ObjStore != nil {
			// JuiceFS FUSE driver is Linux-only; use in-process on macOS.
			drivers[loophole.VolumeTypeJuiceFS] = juicefs.NewInProcessDriver(cfg)
		}
	default:
		return nil, fmt.Errorf("unsupported mode %q on macOS", inst.Mode)
	}
	return fsbackend.NewBackend(vm, drivers), nil
}
