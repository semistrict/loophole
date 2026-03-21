//go:build linux

package fsserver

import (
	"fmt"
	"log/slog"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/nbdvm"
	"github.com/semistrict/loophole/storage"
)

func createBackend(vm *storage.Manager, inst env.ResolvedStore, dir env.Dir) (*Backend, error) {
	requireNBD := env.HasOption("requirenbd")
	disableNBD := env.HasOption("nonbd")

	if requireNBD && disableNBD {
		return nil, fmt.Errorf("LOOPHOLE_OPTIONS cannot include both requirenbd and nonbd")
	}
	if disableNBD {
		slog.Info("filesystem backend: forcing FUSE because LOOPHOLE_OPTIONS includes nonbd")
		return createFUSEBackend(vm, inst, dir)
	}
	if err := nbdvm.Available(); err == nil {
		nbd, nbdErr := NewNBDDriver(vm, nil)
		if nbdErr == nil {
			slog.Info("filesystem backend: using NBD")
			return NewBackend(vm, nil, nbd), nil
		}
		if requireNBD {
			return nil, fmt.Errorf("NBD is required but initialization failed: %w", nbdErr)
		}
		slog.Warn("filesystem backend: NBD init failed, falling back to FUSE", "error", nbdErr)
	} else {
		if requireNBD {
			return nil, fmt.Errorf("NBD is required but unavailable: %w", err)
		}
		slog.Info("filesystem backend: NBD unavailable, falling back to FUSE", "error", err)
	}
	return createFUSEBackend(vm, inst, dir)
}

func createFUSEBackend(vm *storage.Manager, inst env.ResolvedStore, dir env.Dir) (*Backend, error) {
	fuse, err := NewFUSEDriver(dir.Fuse(inst.VolsetID), vm, &fuseblockdev.Options{Debug: inst.LogLevel == "debug", EnableWriteback: true})
	if err != nil {
		return nil, fmt.Errorf("start FUSE backend: %w", err)
	}
	return NewBackend(vm, fuse, nil), nil
}
