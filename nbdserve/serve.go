// Package nbdserve implements an NBD TCP server backed by loophole volumes.
// All volumes in the store are accessible as named NBD exports.
package nbdserve

import (
	"context"
	"fmt"

	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole"
)

const defaultVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB

// Server implements nbd.ExportProvider, resolving loophole volumes
// as NBD exports dynamically at connection time.
type Server struct {
	vm         *loophole.VolumeManager
	volumeSize uint64
}

var _ nbd.ExportProvider = (*Server)(nil)

// NewServer creates an NBD TCP server backed by vm.
func NewServer(vm *loophole.VolumeManager, volumeSize uint64) *Server {
	if volumeSize == 0 {
		volumeSize = defaultVolumeSize
	}
	return &Server{vm: vm, volumeSize: volumeSize}
}

// FindExport opens the named volume and returns an NBD Export for it.
// A fresh VolumeIO is created per connection so its context matches
// the connection lifetime.
func (s *Server) FindExport(ctx context.Context, name string) (nbd.Export, error) {
	if name == "" {
		return nbd.Export{}, fmt.Errorf("no default export; specify a volume name")
	}
	vol, err := s.vm.OpenVolume(ctx, name)
	if err != nil {
		return nbd.Export{}, fmt.Errorf("open volume %q: %w", name, err)
	}
	dev := vol.IO(ctx)
	return nbd.Export{
		Name:   name,
		Size:   s.volumeSize,
		Flags:  nbd.ExportFlags(dev, vol.ReadOnly()),
		Device: dev,
	}, nil
}

// ListExports returns lightweight exports (Name only) for all volumes in the store.
func (s *Server) ListExports(ctx context.Context) ([]nbd.Export, error) {
	names, err := s.vm.ListAllVolumes(ctx)
	if err != nil {
		return nil, err
	}
	exports := make([]nbd.Export, len(names))
	for i, name := range names {
		exports[i] = nbd.Export{Name: name}
	}
	return exports, nil
}

// Serve starts the NBD TCP server, blocking until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, network, addr string) error {
	return nbd.ListenAndServeDynamic(ctx, network, addr, s)
}
