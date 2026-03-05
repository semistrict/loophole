//go:build darwin

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/juicefs"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, _ loophole.Dir, store loophole.ObjectStore) (fsbackend.Service, error) {
	switch inst.Mode {
	case loophole.ModeInProcess:
		if inst.DefaultFSType == loophole.FSJuiceFS {
			return juicefs.NewInProcess(vm, store), nil
		}
		return fsbackend.NewLwext4(vm), nil
	case loophole.ModeFuseFS:
		if inst.DefaultFSType == loophole.FSJuiceFS {
			return nil, fmt.Errorf("juicefs+fusefs not supported on macOS (use inprocess)")
		}
		return fsbackend.NewLwext4FUSE(vm), nil
	default:
		return nil, fmt.Errorf("unsupported mode %q on macOS", inst.Mode)
	}
}
