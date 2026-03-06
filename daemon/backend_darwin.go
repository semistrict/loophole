//go:build darwin

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, _ loophole.Dir) (fsbackend.Service, error) {
	drivers := make(map[string]fsbackend.AnyDriver)
	switch inst.Mode {
	case loophole.ModeInProcess:
		drivers[loophole.VolumeTypeExt4] = fsbackend.NewLwext4Driver()
	case loophole.ModeFuseFS:
		drivers[loophole.VolumeTypeExt4] = fsbackend.NewLwext4FUSEDriver()
	default:
		return nil, fmt.Errorf("unsupported mode %q on macOS", inst.Mode)
	}
	return fsbackend.NewBackend(vm, drivers), nil
}
