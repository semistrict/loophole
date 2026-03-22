//go:build linux

package nbdvm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	nbd "github.com/Merovius/nbd"
	"github.com/Merovius/nbd/nbdnl"
	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole/internal/storage"
)

const defaultNumConns = 1
const loopbackBlockSize = 512

const (
	nbdClearSock = 0xab04
	nbdClearQue  = 0xab05
)

// Server manages NBD exports for loophole volumes using nbd.Loopback.
// Each volume gets a /dev/nbdN device backed by the volume's block store.
type Server struct {
	numConns int

	mu      sync.Mutex
	exports map[string]*volumeExport // volume name → export state
}

type volumeExport struct {
	cancel context.CancelFunc
	wait   func() error
	devIdx uint32
}

// Options configures the NBD server.
type Options struct {
	NumConnections int // parallel socket pairs per device (default 4)
}

// NewServer creates a new NBD volume server.
func NewServer(_ *storage.Manager, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}
	numConns := opts.NumConnections
	if numConns == 0 {
		numConns = defaultNumConns
	}
	return &Server{
		numConns: numConns,
		exports:  make(map[string]*volumeExport),
	}, nil
}

func Available() error {
	if _, err := os.Stat("/sys/module/nbd"); err != nil {
		return fmt.Errorf("nbd module not loaded: %w", err)
	}
	if _, err := nbdnl.StatusAll(); err != nil {
		return fmt.Errorf("nbd netlink unavailable: %w", err)
	}
	return nil
}

// Connect creates an NBD device for a volume and returns /dev/nbdN.
func (s *Server) Connect(ctx context.Context, vol *storage.Volume) (string, error) {
	name := vol.Name()

	s.mu.Lock()
	if exp, ok := s.exports[name]; ok {
		s.mu.Unlock()
		return nbd.DevicePath(exp.devIdx), nil
	}
	s.mu.Unlock()

	srvCtx, cancel := context.WithCancel(context.Background())
	dev := NewDevice(vol)

	loopOpts := []nbd.LoopbackOption{
		nbd.WithClientFlags(nbdnl.FlagDestroyOnDisconnect),
		nbd.WithNumConnections(s.numConns),
		nbd.WithLoopbackBlockSize(loopbackBlockSize),
	}
	if vol.ReadOnly() {
		sf := nbdnl.FlagHasFlags | nbdnl.FlagReadOnly | nbdnl.FlagSendFlush |
			nbdnl.FlagSendTrim | nbdnl.FlagSendWriteZeroes | nbdnl.FlagSendFastZero
		loopOpts = append(loopOpts, nbd.WithServerFlags(sf))
	}

	idx, wait, err := nbd.Loopback(srvCtx, dev, vol.Size(), loopOpts...)
	if err != nil {
		cancel()
		return "", fmt.Errorf("nbd loopback: %w", err)
	}

	if err := ensureDeviceNode(idx); err != nil {
		cancel()
		if werr := wait(); werr != nil {
			slog.Warn("nbd wait failed", "error", werr)
		}
		return "", fmt.Errorf("mknod %s: %w", nbd.DevicePath(idx), err)
	}
	if err := waitForConnected(ctx, idx, vol.Size()); err != nil {
		cancel()
		if werr := wait(); werr != nil {
			slog.Warn("nbd wait failed", "error", werr)
		}
		return "", err
	}

	exp := &volumeExport{cancel: cancel, wait: wait, devIdx: idx}

	s.mu.Lock()
	s.exports[name] = exp
	s.mu.Unlock()

	devPath := nbd.DevicePath(idx)
	slog.Info("nbd: connected", "volume", name, "device", devPath)
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

	slog.Info("nbd: disconnecting", "volume", volumeName, "device", nbd.DevicePath(exp.devIdx))

	// Tell the kernel to disconnect this NBD device via netlink.
	if err := nbdnl.Disconnect(exp.devIdx); err != nil {
		slog.Warn("nbd disconnect failed", "device", nbd.DevicePath(exp.devIdx), "error", err)
	}
	// Cancel the server context so the Go-side goroutine exits.
	exp.cancel()
	if err := exp.wait(); err != nil {
		return err
	}
	if err := clearDisconnectedDevice(nbd.DevicePath(exp.devIdx)); err != nil {
		return err
	}
	return nil
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
		if err := s.Disconnect(ctx, name); err != nil {
			slog.Warn("disconnect failed", "volume", name, "error", err)
		}
	}
}

// ensureDeviceNode creates /dev/nbdN if it doesn't already exist.
// The kernel auto-creates the block device when using IndexAny, but the
// device node in /dev may not exist in containers.
func ensureDeviceNode(idx uint32) error {
	path := nbd.DevicePath(idx)
	dev, err := deviceNumberFromSysfs(idx)
	if err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err == nil {
		if st.Mode&unix.S_IFMT == unix.S_IFBLK && uint64(st.Rdev) == uint64(dev) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return unix.Mknod(path, unix.S_IFBLK|0600, int(dev))
}

func waitForConnected(ctx context.Context, idx uint32, sizeBytes uint64) error {
	devPath := nbd.DevicePath(idx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := nbdnl.Status(idx)
		if err == nil && st.Connected {
			ready, rerr := deviceReady(devPath, idx, sizeBytes)
			if rerr == nil && ready {
				return nil
			}
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return fmt.Errorf("wait for nbd%d connected: %w", idx, ctx.Err())
			default:
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait for nbd%d connected: timeout", idx)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func deviceSizeReady(idx uint32, sizeBytes uint64) (bool, error) {
	data, err := os.ReadFile(filepath.Join("/sys/block", fmt.Sprintf("nbd%d", idx), "size"))
	if err != nil {
		return false, err
	}
	sectors, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return false, err
	}
	return kernelSizeMatches(sectors*512, sizeBytes), nil
}

func deviceReady(devPath string, idx uint32, sizeBytes uint64) (bool, error) {
	if ok, err := deviceSizeReady(idx, sizeBytes); err != nil || !ok {
		return ok, err
	}
	f, err := os.OpenFile(devPath, os.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	var bytes uint64
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), uintptr(unix.BLKGETSIZE64), uintptr(unsafe.Pointer(&bytes)))
	if errno != 0 {
		return false, errno
	}
	return kernelSizeMatches(bytes, sizeBytes), nil
}

func kernelSizeMatches(kernelBytes, volumeBytes uint64) bool {
	if kernelBytes == volumeBytes {
		return true
	}
	if kernelBytes < volumeBytes && volumeBytes-kernelBytes < 512 {
		return true
	}
	return false
}

func deviceNumberFromSysfs(idx uint32) (uint64, error) {
	data, err := os.ReadFile(filepath.Join("/sys/block", fmt.Sprintf("nbd%d", idx), "dev"))
	if err != nil {
		return 0, fmt.Errorf("read sysfs dev for nbd%d: %w", idx, err)
	}
	parts := strings.Split(strings.TrimSpace(string(data)), ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("parse sysfs dev for nbd%d: %q", idx, strings.TrimSpace(string(data)))
	}
	major, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse major for nbd%d: %w", idx, err)
	}
	minor, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse minor for nbd%d: %w", idx, err)
	}
	return unix.Mkdev(uint32(major), uint32(minor)), nil
}

func clearDisconnectedDevice(devPath string) error {
	f, err := os.OpenFile(devPath, os.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, unix.ENXIO) || errors.Is(err, unix.ENODEV) {
			return nil
		}
		return fmt.Errorf("open %s for NBD clear: %w", devPath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	for _, req := range []uintptr{nbdClearQue, nbdClearSock} {
		if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), req, 0); errno != 0 && errno != unix.ENOTTY && errno != unix.EINVAL {
			return fmt.Errorf("ioctl %#x on %s: %w", req, devPath, errno)
		}
	}
	return nil
}
