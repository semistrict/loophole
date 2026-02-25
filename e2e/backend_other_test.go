//go:build !linux

package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func newBackendForMode(t *testing.T, vm *loophole.VolumeManager, _ loophole.Instance) fsbackend.Service {
	t.Helper()
	switch mode() {
	case loophole.ModeLwext4FUSE:
		return fsbackend.NewLwext4FUSE(vm, &fsbackend.Lwext4Options{VolumeSize: defaultVolumeSize})
	default:
		return fsbackend.NewLwext4(vm, &fsbackend.Lwext4Options{VolumeSize: defaultVolumeSize})
	}
}
