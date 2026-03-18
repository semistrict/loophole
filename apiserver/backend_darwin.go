//go:build darwin

package apiserver

import (
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/storage"
)

func createBackend(_ *storage.Manager, _ loophole.Instance, _ loophole.Dir) (*fsbackend.Backend, error) {
	return nil, fmt.Errorf("kernel ext4 via FUSE is not supported on macOS")
}
