//go:build linux

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, dir loophole.Dir) (fsbackend.Service, error) {
	switch inst.Mode {
	case loophole.ModeNBD:
		return fsbackend.NewNBD(vm, nil)
	case loophole.ModeTestNBDTCP:
		return fsbackend.NewNBDTCP(vm, nil)
	case loophole.ModeInProcess:
		return fsbackend.NewLwext4(vm), nil
	case loophole.ModeLwext4FUSE:
		return fsbackend.NewLwext4FUSE(vm), nil
	default:
		backend, err := fsbackend.NewFUSE(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug"})
		if err != nil {
			return nil, fmt.Errorf("start FUSE backend: %w", err)
		}
		return backend, nil
	}
}
