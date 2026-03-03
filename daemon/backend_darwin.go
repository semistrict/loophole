//go:build darwin

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, _ loophole.Dir) (fsbackend.Service, error) {
	switch inst.Mode {
	case loophole.ModeInProcess:
		return fsbackend.NewLwext4(vm), nil
	case loophole.ModeLwext4FUSE:
		return fsbackend.NewLwext4FUSE(vm), nil
	default:
		return nil, fmt.Errorf("unsupported mode %q on macOS", inst.Mode)
	}
}
