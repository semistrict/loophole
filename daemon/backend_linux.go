//go:build linux

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
)

func createBackend(vm loophole.VolumeManager, inst loophole.Instance, dir loophole.Dir) (fsbackend.Service, error) {
	fuse, err := fsbackend.NewFUSEDriver(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug", EnableWriteback: true})
	if err != nil {
		return nil, fmt.Errorf("start FUSE backend: %w", err)
	}
	return fsbackend.NewBackend(vm, fuse), nil
}
