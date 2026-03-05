//go:build linux

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/juicefs"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, dir loophole.Dir, store loophole.ObjectStore) (fsbackend.Service, error) {
	switch inst.Mode {
	case loophole.ModeInProcess:
		if inst.DefaultFSType == loophole.FSJuiceFS {
			return juicefs.NewInProcess(vm, store), nil
		}
		return fsbackend.NewLwext4(vm), nil
	case loophole.ModeFuseFS:
		if inst.DefaultFSType == loophole.FSJuiceFS {
			return juicefs.NewFUSE(vm, store), nil
		}
		return fsbackend.NewLwext4FUSE(vm), nil
	case loophole.ModeNBD:
		return fsbackend.NewNBD(vm, nil)
	case loophole.ModeTestNBDTCP:
		return fsbackend.NewNBDTCP(vm, nil)
	default:
		backend, err := fsbackend.NewFUSE(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug"})
		if err != nil {
			return nil, fmt.Errorf("start FUSE backend: %w", err)
		}
		return backend, nil
	}
}
