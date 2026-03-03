//go:build linux

package nbdvm

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	nbd "github.com/Merovius/nbd"
	"github.com/Merovius/nbd/nbdnl"
	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/nbdserve"
)

// TCPServer manages NBD exports using TCP NBD servers instead of kernel
// loopback socketpairs. Each volume gets its own TCP listener on localhost,
// and the kernel NBD device is connected through a proxy that relays between
// kernel socketpairs and TCP connections to the NBD server.
//
// This exercises the full NBD network code path (handshake + dynamic export
// resolution + transmission) while still producing /dev/nbdN block devices.
type TCPServer struct {
	vm       loophole.VolumeManager
	numConns int
	log      *slog.Logger

	mu         sync.Mutex
	exports    map[string]*tcpExport
	connecting map[string]struct{} // volumes with Connect in progress
}

type tcpExport struct {
	vol    loophole.Volume
	cancel context.CancelFunc
	ln     net.Listener
	devIdx uint32
}

// NewTCPServer creates a new TCP-based NBD server.
func NewTCPServer(vm loophole.VolumeManager, opts *Options) (*TCPServer, error) {
	if opts == nil {
		opts = &Options{}
	}
	numConns := opts.NumConnections
	if numConns == 0 {
		numConns = 1 // TCP proxy mode: use 1 connection for now
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &TCPServer{
		vm:         vm,
		numConns:   numConns,
		log:        logger,
		exports:    make(map[string]*tcpExport),
		connecting: make(map[string]struct{}),
	}, nil
}

// Connect starts a TCP NBD server for the volume on localhost and connects
// the kernel NBD device to it via proxied socketpairs.
func (s *TCPServer) Connect(ctx context.Context, vol loophole.Volume) (string, error) {
	name := vol.Name()

	s.mu.Lock()
	if exp, ok := s.exports[name]; ok {
		s.mu.Unlock()
		return nbd.DevicePath(exp.devIdx), nil
	}
	if _, ok := s.connecting[name]; ok {
		s.mu.Unlock()
		return "", fmt.Errorf("volume %q: connect already in progress", name)
	}
	s.connecting[name] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.connecting, name)
		s.mu.Unlock()
	}()

	// Start a TCP NBD server on a random localhost port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	tcpAddr := ln.Addr().String()

	srvCtx, cancel := context.WithCancel(context.Background())

	// Create a single-volume provider for this export.
	provider := &singleVolumeProvider{
		name:   name,
		vol:    vol,
		srvCtx: srvCtx,
	}

	// Start serving NBD connections in the background.
	go func() {
		defer func() {
			if err := ln.Close(); err != nil {
				slog.Warn("close failed", "error", err)
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				s.log.Debug("nbd-tcp: accept loop ended", "err", err)
				return
			}
			s.log.Debug("nbd-tcp: accepted connection", "remote", c.RemoteAddr())
			go func() {
				err := nbd.ServeDynamic(srvCtx, c, provider)
				s.log.Debug("nbd-tcp: ServeDynamic returned", "remote", c.RemoteAddr(), "err", err)
				if cerr := c.Close(); cerr != nil {
					slog.Warn("close failed", "error", cerr)
				}
			}()
		}
	}()

	// Close listener when context is cancelled.
	go func() {
		<-srvCtx.Done()
		if err := ln.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagSendFlush |
		nbdnl.FlagSendTrim | nbdnl.FlagSendWriteZeroes
	if vol.ReadOnly() {
		serverFlags |= nbdnl.FlagReadOnly
	}
	if s.numConns > 1 {
		serverFlags |= nbdnl.FlagCanMulticonn
	}

	// Create socketpairs: kernel gets one end, we proxy the other end
	// through a TCP connection to the NBD server (with full handshake).
	kernelFDs := make([]*os.File, s.numConns)
	cleanup := func(upTo int) {
		for j := range upTo {
			if kernelFDs[j] != nil {
				if err := kernelFDs[j].Close(); err != nil {
					slog.Warn("close failed", "error", err)
				}
			}
		}
	}

	for i := range s.numConns {
		sp, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			cleanup(i)
			cancel()
			return "", fmt.Errorf("socketpair: %w", err)
		}
		kernelFile := os.NewFile(uintptr(sp[0]), fmt.Sprintf("kernel-%d", i))
		proxyFile := os.NewFile(uintptr(sp[1]), fmt.Sprintf("proxy-%d", i))

		proxyConn, err := net.FileConn(proxyFile)
		if cerr := proxyFile.Close(); cerr != nil {
			slog.Warn("close failed", "error", cerr)
		}
		if err != nil {
			if cerr := kernelFile.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			cleanup(i)
			cancel()
			return "", fmt.Errorf("fileconn: %w", err)
		}

		kernelFDs[i] = kernelFile

		// Dial the TCP NBD server and do the full handshake.
		tcpConn, err := net.Dial("tcp", tcpAddr)
		if err != nil {
			if cerr := proxyConn.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			if cerr := kernelFile.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			cleanup(i)
			cancel()
			return "", fmt.Errorf("dial NBD server: %w", err)
		}

		client, err := nbd.ClientHandshake(ctx, tcpConn)
		if err != nil {
			if cerr := tcpConn.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			if cerr := proxyConn.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			if cerr := kernelFile.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			cleanup(i)
			cancel()
			return "", fmt.Errorf("client handshake: %w", err)
		}

		_, err = client.Go(name)
		if err != nil {
			if cerr := tcpConn.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			if cerr := proxyConn.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			if cerr := kernelFile.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
			cleanup(i)
			cancel()
			return "", fmt.Errorf("client go: %w", err)
		}

		// client.Go() internally closes the ctxRW wrapper, which sets
		// the TCP conn deadline to the past. Reset it so the proxy works.
		if err := tcpConn.SetDeadline(time.Time{}); err != nil {
			cancel()
			return "", fmt.Errorf("reset deadline: %w", err)
		}

		// Both sides are now in transmission mode. Proxy between the
		// kernel's socketpair end and the TCP connection.
		go proxy(srvCtx, proxyConn, tcpConn, s.log, i)
	}

	// Pass the kernel-side fds to the kernel via netlink.
	exp := nbd.Export{
		Size:       vol.Size(),
		Flags:      uint16(serverFlags),
		BlockSizes: &nbd.BlockSizeConstraints{Min: 1, Preferred: 64 * 1024, Max: 0xffffffff},
	}
	idx, err := nbd.Configure(exp, kernelFDs...)
	if err != nil {
		cleanup(s.numConns)
		cancel()
		return "", fmt.Errorf("nbd configure: %w", err)
	}

	// Close our copies of the kernel-side fds. The kernel has its own
	// copies via netlink. Keeping ours open can interfere with Go's poller.
	for _, f := range kernelFDs {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}

	if err := ensureDeviceNode(idx); err != nil {
		if derr := nbdnl.Disconnect(idx); derr != nil {
			slog.Warn("nbd disconnect failed", "device", nbd.DevicePath(idx), "error", derr)
		}
		cancel()
		return "", fmt.Errorf("mknod %s: %w", nbd.DevicePath(idx), err)
	}

	s.mu.Lock()
	s.exports[name] = &tcpExport{
		vol:    vol,
		cancel: cancel,
		ln:     ln,
		devIdx: idx,
	}
	s.mu.Unlock()

	devPath := nbd.DevicePath(idx)
	s.log.Info("nbd-tcp: connected", "volume", name, "device", devPath, "server", tcpAddr)
	return devPath, nil
}

// onlyRW wraps an io.ReadWriter to hide any ReadFrom/WriteTo methods,
// preventing Go's io.Copy from using splice() which deadlocks between
// unix socketpairs and TCP connections.
type onlyRW struct {
	io.Reader
	io.Writer
}

// proxy bidirectionally copies data between a (kernel socketpair) and b (TCP)
// until ctx is cancelled or either side closes.
func proxy(ctx context.Context, a, b net.Conn, log *slog.Logger, connIdx int) {
	var once sync.Once
	done := make(chan struct{})
	closeDone := func() { once.Do(func() { close(done) }) }

	go func() {
		_, err := io.Copy(onlyRW{b, b}, onlyRW{a, a})
		if err != nil {
			log.Debug("nbd-tcp: proxy kernel→tcp error", "conn", connIdx, "err", err)
		}
		closeDone()
	}()
	go func() {
		_, err := io.Copy(onlyRW{a, a}, onlyRW{b, b})
		if err != nil {
			log.Debug("nbd-tcp: proxy tcp→kernel error", "conn", connIdx, "err", err)
		}
		closeDone()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}
	if err := a.Close(); err != nil {
		slog.Warn("close failed", "error", err)
	}
	if err := b.Close(); err != nil {
		slog.Warn("close failed", "error", err)
	}
}

// DevicePath returns the /dev/nbdN path for a connected volume, or empty string.
func (s *TCPServer) DevicePath(volumeName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.exports[volumeName]; ok {
		return nbd.DevicePath(exp.devIdx)
	}
	return ""
}

// Disconnect tears down the NBD device and TCP server for a volume.
func (s *TCPServer) Disconnect(ctx context.Context, volumeName string) error {
	s.mu.Lock()
	exp, ok := s.exports[volumeName]
	if ok {
		delete(s.exports, volumeName)
	}
	s.mu.Unlock()

	if !ok {
		return nil
	}

	s.log.Info("nbd-tcp: disconnecting", "volume", volumeName, "device", nbd.DevicePath(exp.devIdx))

	if err := nbdnl.Disconnect(exp.devIdx); err != nil {
		slog.Warn("nbd disconnect failed", "device", nbd.DevicePath(exp.devIdx), "error", err)
	}
	exp.cancel()
	return nil
}

// Close shuts down all exports.
func (s *TCPServer) Close(ctx context.Context) {
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

// singleVolumeProvider is an ExportProvider that serves a single volume.
type singleVolumeProvider struct {
	name   string
	vol    loophole.Volume
	srvCtx context.Context
}

// FindExport returns the export for this volume. It uses srvCtx (not the
// per-request ctx) because the Device's lifetime is tied to the TCP server,
// not to an individual NBD connection — the kernel reuses it across reconnects.
func (p *singleVolumeProvider) FindExport(_ context.Context, name string) (nbd.Export, error) {
	if name != "" && name != p.name {
		return nbd.Export{}, fmt.Errorf("unknown export %q", name)
	}
	dev := NewDevice(p.vol)
	return nbd.Export{
		Name:   p.name,
		Size:   p.vol.Size(),
		Flags:  nbd.ExportFlags(dev, p.vol.ReadOnly()),
		Device: dev,
	}, nil
}

func (p *singleVolumeProvider) ListExports(_ context.Context) ([]nbd.Export, error) {
	return []nbd.Export{{Name: p.name}}, nil
}

// NewNBDTCPBackend creates an nbdserve.Server for use with --nbd flag.
func NewNBDTCPBackend(vm loophole.VolumeManager) *nbdserve.Server {
	return nbdserve.NewServer(vm)
}
