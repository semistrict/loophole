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

	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/env"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/util"
)

// ServerOptions configures the HTTP server created by the *AndServe functions.
type ServerOptions struct {
	Foreground      bool        // add console log handler
	ConsoleLogLevel string      // foreground console level; empty => inherit file level
	SocketMode      os.FileMode // chmod the Unix socket
	ListenAddr      string      // "tcp://..." or ""
	SocketPath      string      // Unix socket path
}

// CreateFSAndServe creates a new volume, formats ext4, mounts it, and serves.
func CreateFSAndServe(ctx context.Context, inst env.ResolvedStore, dir env.Dir, p storage.CreateParams, mountpoint string, opts ServerOptions) error {
	s, err := setup(ctx, inst, dir, p.Volume, opts)
	if err != nil {
		return err
	}

	if mountpoint == "" {
		mountpoint = p.Volume
	}

	// Start the HTTP server early so /metrics and /status are available during create.
	serveCtx, stop := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.serve(serveCtx) }()

	shutdownAndReturn := func(err error) error {
		stop()
		<-serveErr
		s.cleanup(context.Background())
		return err
	}

	if err := s.backend.Create(ctx, p); err != nil {
		return shutdownAndReturn(err)
	}

	if !s.backend.SupportsFilesystem() {
		return shutdownAndReturn(fmt.Errorf("filesystem backend is not available on this platform"))
	}

	if err := s.backend.Mount(ctx, p.Volume, mountpoint); err != nil {
		return shutdownAndReturn(err)
	}

	s.setVolume(s.volume())
	s.mountpoint = mountpoint
	s.writeSymlink(mountpoint)
	return <-serveErr
}

// MountFSAndServe mounts an existing volume and serves.
func MountFSAndServe(ctx context.Context, inst env.ResolvedStore, dir env.Dir, volume, mountpoint string, opts ServerOptions) error {
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
func CreateBlockDeviceAndServe(ctx context.Context, inst env.ResolvedStore, dir env.Dir, p storage.CreateParams, opts ServerOptions) error {
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
func AttachBlockDeviceAndServe(ctx context.Context, inst env.ResolvedStore, dir env.Dir, volume string, opts ServerOptions) error {
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
func setup(ctx context.Context, inst env.ResolvedStore, dir env.Dir, volume string, opts ServerOptions) (*server, error) {
	var store *blob.Store
	var err error
	if inst.VolsetID == "" {
		inst, store, err = storage.ResolveFormattedStore(ctx, inst)
		if err != nil {
			return nil, err
		}
	} else {
		store, err = blob.Open(ctx, inst)
		if err != nil {
			if inst.IsLocal() {
				return nil, fmt.Errorf("create file store: %w", err)
			}
			return nil, fmt.Errorf("create object store: %w", err)
		}
	}

	logPath, axiomClose, err := setupLogging(inst, dir, volume, opts)
	if err != nil {
		return nil, err
	}

	tuneProcess()
	if inst.IsLocal() {
		slog.Warn("using local file store -- data is NOT replicated to S3")
	}

	vm := storage.NewManagerForStore(inst, dir, store)

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
func setupLogging(inst env.ResolvedStore, dir env.Dir, volume string, opts ServerOptions) (logPath string, axiomClose func(), err error) {
	logPath = dir.VolumeLog(inst.VolsetID, volume)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}

	// Redirect Go's standard log package (used by net/http for panic recovery).
	log.SetOutput(logFile)

	fileLevel := parseSlogLevel(inst.LogLevel, slog.LevelInfo)
	consoleLevel := fileLevel
	if opts.ConsoleLogLevel != "" {
		consoleLevel = parseSlogLevel(opts.ConsoleLogLevel, slog.LevelWarn)
	}

	fileHandler := slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: fileLevel})
	handlers := multiHandler{fileHandler}
	if opts.Foreground {
		handlers = append(handlers, &consoleHandler{level: consoleLevel})
	}
	if os.Getenv("AXIOM_TOKEN") != "" {
		ah, err := axiomslog.New(axiomslog.SetLevel(fileLevel))
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

func parseSlogLevel(raw string, fallback slog.Level) slog.Level {
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return fallback
	}
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
