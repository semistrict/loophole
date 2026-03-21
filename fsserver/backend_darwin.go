//go:build darwin

package fsserver

import (
	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/storage"
)

func createBackend(vm *storage.Manager, _ env.ResolvedStore, _ env.Dir) (*Backend, error) {
	return NewBackend(vm, nil), nil
}
