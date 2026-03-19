//go:build darwin

package fsserver

import (
	"fmt"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/storage"
)

func createBackend(_ *storage.Manager, _ env.ResolvedProfile, _ env.Dir) (*Backend, error) {
	return nil, fmt.Errorf("kernel ext4 via FUSE is not supported on macOS")
}
