//go:build linux

package apiserver

import (
	"fmt"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/storage"
)

func createBackend(vm *storage.Manager, inst env.ResolvedProfile, dir env.Dir) (*fsbackend.Backend, error) {
	fuse, err := fsbackend.NewFUSEDriver(dir.Fuse(inst.ProfileName), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug", EnableWriteback: true})
	if err != nil {
		return nil, fmt.Errorf("start FUSE backend: %w", err)
	}
	return fsbackend.NewBackend(vm, fuse), nil
}
