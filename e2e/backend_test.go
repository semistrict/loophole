package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/juicefs"
)

func newBackendForMode(t *testing.T, vm loophole.VolumeManager, inst loophole.Instance, store loophole.ObjectStore) fsbackend.Service {
	t.Helper()
	if b := newPlatformBackend(t, vm, inst, store); b != nil {
		return b
	}
	// Cross-platform userspace backends.
	switch mode() {
	case loophole.ModeInProcess:
		if fsType() == loophole.FSJuiceFS {
			return juicefs.NewInProcess(vm, store)
		}
		return fsbackend.NewLwext4(vm)
	default:
		return fsbackend.NewLwext4(vm)
	}
}
