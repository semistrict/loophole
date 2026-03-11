//go:build linux

package daemon

import (
	"fmt"
	"log/slog"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, dir loophole.Dir) (fsbackend.Service, error) {
	drivers := make(map[string]fsbackend.AnyDriver)
	switch inst.Mode {
	case loophole.ModeInProcess:
		d, err := lwext4Driver()
		if err != nil {
			return nil, err
		}
		drivers[loophole.VolumeTypeExt4] = d
	case loophole.ModeFuseFS:
		d, err := lwext4FUSEDriver()
		if err != nil {
			return nil, err
		}
		drivers[loophole.VolumeTypeExt4] = d
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
		fuse, err := fsbackend.NewFUSEDriver(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug", EnableWriteback: true})
		if err != nil {
			slog.Warn("FUSE backend unavailable, filesystem operations will fail", "error", err)
		} else {
			drivers[loophole.VolumeTypeExt4] = fuse
			drivers[loophole.VolumeTypeXFS] = fuse
		}
	}
	return fsbackend.NewBackend(vm, drivers), nil
}
