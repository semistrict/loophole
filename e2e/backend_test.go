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
	switch mode() {
	case loophole.ModeJuiceFS:
		return juicefs.NewInProcess(vm, store)
	default:
		return fsbackend.NewLwext4(vm)
	}
}
