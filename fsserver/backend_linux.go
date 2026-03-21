//go:build linux

package fsserver

import (
	"fmt"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/storage"
)

func createBackend(vm *storage.Manager, inst env.ResolvedStore, dir env.Dir) (*Backend, error) {
	fuse, err := NewFUSEDriver(dir.Fuse(inst.VolsetID), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug", EnableWriteback: true})
	if err != nil {
		return nil, fmt.Errorf("start FUSE backend: %w", err)
	}
	return NewBackend(vm, fuse), nil
}
