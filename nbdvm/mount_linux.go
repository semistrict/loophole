//go:build linux

package nbdvm

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	nbd "github.com/Merovius/nbd"
	"github.com/Merovius/nbd/nbdnl"
	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole"
)

const (
	defaultVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB
	defaultNumConns   = 4                         // parallel socket pairs per device
)

// Server manages NBD exports for loophole volumes using nbd.Loopback.
// Each volume gets a /dev/nbdN device backed by the volume's block store.
type Server struct {
	vm         *loophole.VolumeManager
	volumeSize uint64
	numConns int
	log      *slog.Logger

	mu      sync.Mutex
	exports map[string]*volumeExport // volume name → export state
}

type volumeExport struct {
	vol    *loophole.Volume
	cancel context.CancelFunc
	wait   func() error
	devIdx uint32
}

// Options configures the NBD server.
type Options struct {
	VolumeSize     uint64
	NumConnections int // parallel socket pairs per device (default 4)
}

// NewServer creates a new NBD volume server.
func NewServer(vm *loophole.VolumeManager, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}
	volumeSize := opts.VolumeSize
	if volumeSize == 0 {
		volumeSize = defaultVolumeSize
	}
	numConns := opts.NumConnections
	if numConns == 0 {
		numConns = defaultNumConns
	}
	return &Server{
		vm:         vm,
		volumeSize: volumeSize,
		numConns:   numConns,
		log:        slog.Default(),
		exports:    make(map[string]*volumeExport),
	}, nil
}

// Connect creates an NBD device for a volume and returns /dev/nbdN.
func (s *Server) Connect(ctx context.Context, vol *loophole.Volume) (string, error) {
	name := vol.Name()

	s.mu.Lock()
	if exp, ok := s.exports[name]; ok {
		s.mu.Unlock()
		return nbd.DevicePath(exp.devIdx), nil
	}
	s.mu.Unlock()

	srvCtx, cancel := context.WithCancel(context.Background())
	dev := vol.IO(srvCtx)

	loopOpts := []nbd.LoopbackOption{
		nbd.WithNumConnections(s.numConns),
	}
	if vol.ReadOnly() {
		sf := nbdnl.FlagHasFlags | nbdnl.FlagReadOnly | nbdnl.FlagSendFlush |
			nbdnl.FlagSendTrim | nbdnl.FlagSendWriteZeroes | nbdnl.FlagSendFastZero
		loopOpts = append(loopOpts, nbd.WithServerFlags(sf))
	}

	idx, wait, err := nbd.Loopback(srvCtx, dev, s.volumeSize, loopOpts...)
	if err != nil {
		cancel()
		return "", fmt.Errorf("nbd loopback: %w", err)
	}

	if err := ensureDeviceNode(idx); err != nil {
		cancel()
		wait()
		return "", fmt.Errorf("mknod %s: %w", nbd.DevicePath(idx), err)
	}

	exp := &volumeExport{
		vol:    vol,
		cancel: cancel,
		wait:   wait,
		devIdx: idx,
	}

	s.mu.Lock()
	s.exports[name] = exp
	s.mu.Unlock()

	devPath := nbd.DevicePath(idx)
	s.log.Info("nbd: connected", "volume", name, "device", devPath)
	return devPath, nil
}

// DevicePath returns the /dev/nbdN path for a connected volume, or empty string.
func (s *Server) DevicePath(volumeName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.exports[volumeName]; ok {
		return nbd.DevicePath(exp.devIdx)
	}
	return ""
}

// Disconnect tears down the NBD device for a volume.
func (s *Server) Disconnect(ctx context.Context, volumeName string) error {
	s.mu.Lock()
	exp, ok := s.exports[volumeName]
	if ok {
		delete(s.exports, volumeName)
	}
	s.mu.Unlock()

	if !ok {
		return nil
	}

	s.log.Info("nbd: disconnecting", "volume", volumeName, "device", nbd.DevicePath(exp.devIdx))

	// Tell the kernel to disconnect this NBD device via netlink.
	nbdnl.Disconnect(exp.devIdx)
	// Cancel the server context so the Go-side goroutine exits.
	exp.cancel()
	return exp.wait()
}

// Close shuts down all exports.
func (s *Server) Close(ctx context.Context) {
	s.mu.Lock()
	names := make([]string, 0, len(s.exports))
	for name := range s.exports {
		names = append(names, name)
	}
	s.mu.Unlock()

	for _, name := range names {
		s.Disconnect(ctx, name)
	}
}

// ensureDeviceNode creates /dev/nbdN if it doesn't already exist.
// The kernel auto-creates the block device when using IndexAny, but the
// device node in /dev may not exist in containers.
func ensureDeviceNode(idx uint32) error {
	path := nbd.DevicePath(idx)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	// NBD major is 43, minor is idx * partitions_per_device (default 16).
	const nbdMajor = 43
	const partsPerDev = 16
	dev := unix.Mkdev(nbdMajor, uint32(idx)*partsPerDev)
	return unix.Mknod(path, unix.S_IFBLK|0600, int(dev))
}
