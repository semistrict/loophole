//go:build !linux

package e2e

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/juicefs"
)

func newPlatformBackend(_ testing.TB, _ loophole.VolumeManager, _ loophole.Instance, _ juicefs.Config) fsbackend.Service {
	return nil
}
