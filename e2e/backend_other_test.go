//go:build !linux

package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

func newPlatformBackend(_ testing.TB, _ loophole.VolumeManager, _ loophole.Instance) fsbackend.Service {
	return nil
}
