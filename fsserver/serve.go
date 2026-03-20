package fsserver

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	axiomslog "github.com/axiomhq/axiom-go/adapters/slog"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/objstore"
	"github.com/semistrict/loophole/storage"
)

// ServerOptions configures the HTTP server created by the *AndServe functions.
type ServerOptions struct {
	Foreground bool        // add console log handler
	SocketMode os.FileMode // chmod the Unix socket
	ListenAddr string      // "tcp://..." or ""
	SocketPath string      // Unix socket path
}

// CreateFSAndServe creates a new volume, formats ext4, mounts it, and serves.
func CreateFSAndServe(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, p storage.CreateParams, mountpoint string, opts ServerOptions) error {
	s, err := setup(ctx, inst, dir, p.Volume, opts)
	if err != nil {
		return err
	}

	if mountpoint == "" {
		mountpoint = p.Volume
	}
	if err := s.backend.Create(ctx, p); err != nil {
		s.cleanup(context.Background())
		return err
	}
	if err := s.backend.Mount(ctx, p.Volume, mountpoint); err != nil {
		s.cleanup(context.Background())
		return err
	}
	s.setVolume(s.volume())
	s.mountpoint = mountpoint
	s.writeSymlink(mountpoint)
	return s.serve(ctx)
}

// MountFSAndServe mounts an existing volume and serves.
func MountFSAndServe(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, volume, mountpoint string, opts ServerOptions) error {
	s, err := setup(ctx, inst, dir, volume, opts)
	if err != nil {
		return err
	}

	if mountpoint == "" {
		mountpoint = volume
	}
	if err := s.backend.Mount(ctx, volume, mountpoint); err != nil {
		s.cleanup(context.Background())
		return err
	}
	s.setVolume(s.volume())
	s.mountpoint = mountpoint
	s.writeSymlink(mountpoint)
	return s.serve(ctx)
}

// CreateBlockDeviceAndServe creates a new raw volume, attaches it as a block device, and serves.
func CreateBlockDeviceAndServe(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, p storage.CreateParams, opts ServerOptions) error {
	s, err := setup(ctx, inst, dir, p.Volume, opts)
	if err != nil {
		return err
	}

	p.NoFormat = true
	if err := s.backend.Create(ctx, p); err != nil {
		s.cleanup(context.Background())
		return err
	}
	devicePath, err := s.backend.DeviceAttach(ctx, p.Volume)
	if err != nil {
		s.cleanup(context.Background())
		return err
	}
	s.setVolume(s.volume())
	s.devicePath = devicePath
	s.writeDeviceSymlink(devicePath)
	return s.serve(ctx)
}

// AttachBlockDeviceAndServe attaches an existing volume as a raw block device and serves.
func AttachBlockDeviceAndServe(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, volume string, opts ServerOptions) error {
	s, err := setup(ctx, inst, dir, volume, opts)
	if err != nil {
		return err
	}

	devicePath, err := s.backend.DeviceAttach(ctx, volume)
	if err != nil {
		s.cleanup(context.Background())
		return err
	}
	s.setVolume(s.volume())
	s.devicePath = devicePath
	s.writeDeviceSymlink(devicePath)
	return s.serve(ctx)
}

// setup creates all the infrastructure for a server: logging, process tuning,
// object store, page cache, volume manager, backend, and network listener.
func setup(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, volume string, opts ServerOptions) (*server, error) {
	logPath, axiomClose, err := setupLogging(inst, dir, volume, opts.Foreground)
	if err != nil {
		return nil, err
	}

	tuneProcess()

	store, err := objstore.OpenForProfile(ctx, inst)
	if err != nil {
		if inst.LocalDir != "" {
			return nil, fmt.Errorf("create file store: %w", err)
		}
		return nil, fmt.Errorf("create S3 store: %w", err)
	}
	if inst.LocalDir != "" {
		slog.Warn("using local file store -- data is NOT replicated to S3")
	}

	vm := storage.NewManagerForProfile(inst, dir, store)

	backend, err := createBackend(vm, inst, dir)
	if err != nil {
		util.SafeClose(vm, "close manager after backend init failure")
		return nil, err
	}

	ln, err := listen(opts)
	if err != nil {
		util.SafeClose(vm, "close manager after listen failure")
		if closeErr := backend.Close(context.Background()); closeErr != nil {
			slog.Warn("close backend after listen failure", "error", closeErr)
		}
		return nil, err
	}

	s := &server{
		inst:       inst,
		dir:        dir,
		socket:     opts.SocketPath,
		backend:    backend,
		ln:         ln,
		logPath:    logPath,
		axiomClose: axiomClose,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	return s, nil
}

// setupLogging configures structured logging to a file (and optionally console/axiom).
func setupLogging(inst env.ResolvedProfile, dir env.Dir, volume string, foreground bool) (logPath string, axiomClose func(), err error) {
	logPath = dir.VolumeLog(volume)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}

	// Redirect Go's standard log package (used by net/http for panic recovery).
	log.SetOutput(logFile)

	var level slog.Level
	switch strings.ToLower(inst.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	fileHandler := slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: level})
	handlers := multiHandler{fileHandler}
	if foreground {
		handlers = append(handlers, &consoleHandler{level: level})
	}
	if os.Getenv("AXIOM_TOKEN") != "" {
		ah, err := axiomslog.New(axiomslog.SetLevel(level))
		if err != nil {
			return "", nil, fmt.Errorf("create axiom handler: %w", err)
		}
		var h slog.Handler = ah
		if doID := os.Getenv("CONTAINER_DO_ID"); doID != "" {
			h = h.WithAttrs([]slog.Attr{slog.String("do_id", doID)})
		}
		handlers = append(handlers, h)
		axiomClose = func() { ah.Close() }
	}

	logger := slog.New(handlers)
	slog.SetDefault(logger)
	slog.Info("starting server", "s3", inst.URL(), "log", logPath)
	return logPath, axiomClose, nil
}

// listen creates the network listener based on options.
func listen(opts ServerOptions) (net.Listener, error) {
	if strings.HasPrefix(opts.ListenAddr, "tcp://") {
		addr := strings.TrimPrefix(opts.ListenAddr, "tcp://")
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen tcp %s: %w", addr, err)
		}
		slog.Info("listening on TCP", "addr", addr)
		return ln, nil
	}

	sockPath := opts.SocketPath
	if sockPath == "" {
		return nil, fmt.Errorf("socket path is required (use SocketPath option)")
	}
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove stale socket failed", "path", sockPath, "error", err)
	}
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", sockPath, err)
	}
	if opts.SocketMode != 0 {
		if err := os.Chmod(sockPath, opts.SocketMode); err != nil {
			util.SafeClose(ln, "close listener after chmod failure")
			return nil, fmt.Errorf("chmod socket: %w", err)
		}
	}
	return ln, nil
}
