// Package nbdserve implements an NBD TCP server backed by loophole volumes.
// All volumes in the store are accessible as named NBD exports.
package nbdserve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

// Server implements nbd.ExportProvider, resolving loophole volumes
// as NBD exports dynamically at connection time.
type Server struct {
	vm loophole.VolumeManager
}

var _ nbd.ExportProvider = (*Server)(nil)

// NewServer creates an NBD TCP server backed by vm.
func NewServer(vm loophole.VolumeManager) *Server {
	return &Server{vm: vm}
}

// FindExport opens the named volume and returns an NBD Export for it.
func (s *Server) FindExport(ctx context.Context, name string) (nbd.Export, error) {
	if name == "" {
		return nbd.Export{}, fmt.Errorf("no default export; specify a volume name")
	}
	vol, err := s.vm.OpenVolume(ctx, name)
	if err != nil {
		return nbd.Export{}, fmt.Errorf("open volume %q: %w", name, err)
	}
	dev := volumeNBD{vol: vol}
	return nbd.Export{
		Name:       name,
		Size:       vol.Size(),
		Flags:      nbd.ExportFlags(dev, vol.ReadOnly()),
		Device:     dev,
		BlockSizes: &nbd.BlockSizeConstraints{Min: 1, Preferred: 4096, Max: 4 << 20},
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

// Serve listens on network/addr and serves NBD, blocking until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, network, addr string) error {
	return nbd.ListenAndServeDynamic(ctx, network, addr, s)
}

// ServeListener accepts connections on ln and serves NBD on each.
// It closes ln when done and blocks until all connections are finished.
func (s *Server) ServeListener(ln net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		wg.Go(func() {
			defer util.SafeClose(c, "close NBD conn")
			if err := nbd.ServeDynamic(context.Background(), c, s); err != nil {
				slog.Warn("NBD serve", "error", err)
			}
		})
	}
}

// volumeNBD adapts a *Volume to the nbd.Device/Trimmer/WriteZeroer interfaces.
type volumeNBD struct {
	vol loophole.Volume
}

func (d volumeNBD) WriteAt(p []byte, off int64) (int, error) {
	if err := d.vol.Write(context.Background(), p, uint64(off)); err != nil {
		slog.Error("volumeNBD.WriteAt failed", "volume", d.vol.Name(), "offset", off, "len", len(p), "error", err)
		return 0, err
	}
	return len(p), nil
}

func (d volumeNBD) ReadAt(p []byte, off int64) (int, error) {
	n, err := d.vol.Read(context.Background(), p, uint64(off))
	if err != nil {
		slog.Error("volumeNBD.ReadAt failed", "volume", d.vol.Name(), "offset", off, "len", len(p), "error", err)
	}
	return n, err
}

func (d volumeNBD) Sync() error {
	err := d.vol.Flush(context.Background())
	if err != nil {
		slog.Error("volumeNBD.Sync failed", "volume", d.vol.Name(), "error", err)
	}
	return err
}

func (d volumeNBD) Trim(offset, length int64) error {
	return d.vol.PunchHole(context.Background(), uint64(offset), uint64(length))
}

func (d volumeNBD) WriteZeroes(offset, length int64, _ bool) error {
	return d.vol.PunchHole(context.Background(), uint64(offset), uint64(length))
}
