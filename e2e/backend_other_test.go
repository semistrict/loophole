//go:build !linux

package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func newPlatformBackend(_ *testing.T, _ loophole.VolumeManager, _ loophole.Instance, _ loophole.ObjectStore) fsbackend.Service {
	return nil
}
