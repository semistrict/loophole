//go:build linux

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/juicefs"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, dir loophole.Dir, cfg juicefs.Config) (fsbackend.Service, error) {
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
			drivers[loophole.VolumeTypeJuiceFS] = juicefs.NewFUSEDriver(cfg)
		}
	case loophole.ModeNBD:
		nbd, err := fsbackend.NewNBDDriver(vm, nil)
		if err != nil {
			return nil, fmt.Errorf("start NBD backend: %w", err)
		}
		drivers[loophole.VolumeTypeExt4] = nbd
		drivers[loophole.VolumeTypeXFS] = nbd
	case loophole.ModeTestNBDTCP:
		nbd, err := fsbackend.NewNBDTCPDriver(vm, nil)
		if err != nil {
			return nil, fmt.Errorf("start NBD TCP backend: %w", err)
		}
		drivers[loophole.VolumeTypeExt4] = nbd
		drivers[loophole.VolumeTypeXFS] = nbd
	default: // ModeFUSE (blockdev)
		fuse, err := fsbackend.NewFUSEDriver(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug"})
		if err != nil {
			return nil, fmt.Errorf("start FUSE backend: %w", err)
		}
		drivers[loophole.VolumeTypeExt4] = fuse
		drivers[loophole.VolumeTypeXFS] = fuse
	}
	return fsbackend.NewBackend(vm, drivers), nil
}
