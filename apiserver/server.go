// Package apiserver implements the loophole HTTP/UDS API.
// It is a thin remoting layer over fsbackend.Backend.
package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	axiomslog "github.com/axiomhq/axiom-go/adapters/slog"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/storage"
)

func storage2ConfigFromEnv() storage.Config {
	cfg := storage.Config{}

	if v := os.Getenv("LOOPHOLE_TEST_STORAGE2_FLUSH_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.FlushThreshold = n
		}
	}
	return cfg
}

// Server serves the loophole HTTP API over a Unix socket.
type Server struct {
	inst      loophole.Instance
	dir       loophole.Dir
	socket    string
	backend   *fsbackend.Backend
	diskCache *storage.PageCache
	ln        net.Listener

	managedVolume string
	mountpoint    string
	devicePath    string
	logPath       string

	startupErr string // non-fatal startup error (e.g. bad S3 creds)

	axiomClose func() // flushes and closes the axiom handler; nil if axiom is not configured

	shutdownCh chan struct{} // closed when shutdown begins
	doneCh     chan struct{} // closed when cleanup is complete
}

// Backend returns the underlying backend.
func (d *Server) Backend() *fsbackend.Backend { return d.backend }

// Start initializes everything and returns a Server ready to Serve.
// Options configures server startup.
type Options struct {
	Foreground bool
	SocketMode os.FileMode
	ListenAddr string // If set (e.g. "tcp://0.0.0.0:8080"), listen on this address instead of Unix socket.
	SocketPath string
	Volume     string // Volume name — used for per-volume log file path.
}

func Start(ctx context.Context, inst loophole.Instance, dir loophole.Dir, opts Options) (*Server, error) {
	foreground := opts.Foreground
	socketMode := opts.SocketMode
	logPath := dir.VolumeLog(opts.Volume)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}

	// Redirect Go's standard log package (used by net/http for panic recovery)
	// to the log file.
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
	var axiomClose func()
	handlers := multiHandler{fileHandler}
	if foreground {
		handlers = append(handlers, &consoleHandler{level: level})
	}
	if os.Getenv("AXIOM_TOKEN") != "" {
		ah, err := axiomslog.New(axiomslog.SetLevel(level))
		if err != nil {
			return nil, fmt.Errorf("create axiom handler: %w", err)
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

	tuneProcess()

	// Create object store
	var startupErr string
	var store loophole.ObjectStore
	if inst.LocalDir != "" {
		var err error
		store, err = loophole.NewFileStore(inst.LocalDir)
		if err != nil {
			return nil, fmt.Errorf("create file store: %w", err)
		}
		slog.Warn("using local file store — data is NOT replicated to S3")
	} else {
		var err error
		store, err = loophole.NewS3Store(ctx, inst)
		if err != nil {
			storeErr := fmt.Sprintf("create S3 store: %v", err)
			slog.Error("S3 store init failed, server will start degraded", "error", err)
			// Fall back to a nil store — volume operations will fail but server stays up.
			store = nil
			startupErr = storeErr
		}
	}

	var diskCache *storage.PageCache
	var backend *fsbackend.Backend
	if store != nil {
		// Create volume manager
		// The page cache remains profile-scoped even though each process now
		// manages a single volume. That lets all owner processes for a profile
		// share the same on-disk cache.
		cacheDir := dir.Cache(inst.ProfileName)
		var err error
		diskCache, err = storage.NewPageCache(filepath.Join(cacheDir, "diskcache"))
		if err != nil {
			return nil, fmt.Errorf("create page cache: %w", err)
		}
		vm := storage.NewManager(store, cacheDir, storage2ConfigFromEnv(), nil, diskCache)

		backend, err = createBackend(vm, inst, dir)
		if err != nil {
			util.SafeClose(diskCache, "close disk cache after backend init failure")
			return nil, err
		}

		// When a remote break-lease arrives, properly unmount/detach via the backend.
		vm.SetOnRelease(func(ctx context.Context, volumeName string) {
			// Unmount if mounted.
			for mp, vol := range backend.Mounts() {
				if vol == volumeName {
					slog.Info("release: unmounting", "volume", volumeName, "mountpoint", mp)
					if err := backend.Unmount(ctx, mp); err != nil {
						slog.Warn("release: unmount failed", "volume", volumeName, "error", err)
					}
				}
			}
			// Detach device if attached (and not already closed by Unmount).
			if v := vm.GetVolume(volumeName); v != nil {
				slog.Info("release: detaching device", "volume", volumeName)
				if err := backend.DeviceDetach(ctx, volumeName); err != nil {
					slog.Warn("release: device detach failed", "volume", volumeName, "error", err)
				}
			}
		})
	}

	var ln net.Listener
	if strings.HasPrefix(opts.ListenAddr, "tcp://") {
		addr := strings.TrimPrefix(opts.ListenAddr, "tcp://")
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen tcp %s: %w", addr, err)
		}
		slog.Info("listening on TCP", "addr", addr)
	} else {
		sockPath := opts.SocketPath
		if sockPath == "" {
			return nil, fmt.Errorf("socket path is required (use --socket-path or SocketPath option)")
		}
		if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
			return nil, err
		}
		if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove stale socket failed", "path", sockPath, "error", err)
		}
		ln, err = net.Listen("unix", sockPath)
		if err != nil {
			return nil, fmt.Errorf("listen %s: %w", sockPath, err)
		}
		if socketMode != 0 {
			if err := os.Chmod(sockPath, socketMode); err != nil {
				if closeErr := ln.Close(); closeErr != nil {
					slog.Warn("listener close error", "error", closeErr)
				}
				return nil, fmt.Errorf("chmod socket: %w", err)
			}
		}
	}

	d := &Server{
		inst:       inst,
		dir:        dir,
		socket:     socketPathFromOptions(opts),
		backend:    backend,
		diskCache:  diskCache,
		ln:         ln,
		logPath:    logPath,
		axiomClose: axiomClose,
		startupErr: startupErr,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	return d, nil
}

// Serve blocks, handling HTTP requests until the context is cancelled.
func (d *Server) Serve(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			n := runtime.Stack(buf, true)
			slog.Error("PANIC", "panic", fmt.Sprintf("%v", r), "stack", string(buf[:n]))
			if d.axiomClose != nil {
				d.axiomClose()
			}
			panic(r) // re-panic after logging
		}
	}()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Handler: d.instrument(d.mux(stop))}

	// Heartbeat: log runtime stats every 5s at debug level.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				slog.Debug("heartbeat",
					"heap_mb", m.HeapAlloc>>20,
					"sys_mb", m.Sys>>20,
					"goroutines", runtime.NumGoroutine(),
					"gc_cycles", m.NumGC,
				)
			case <-ctx.Done():
				return
			}
		}
	}()

	// When context cancels, begin graceful shutdown in background.
	// The HTTP server stays running so clients can poll /status and /shutdown/wait.
	go func() {
		<-ctx.Done()
		close(d.shutdownCh)

		slog.Info("shutdown: start")

		if d.backend != nil {
			slog.Info("shutdown: closing backend (flush volumes, release leases)")
			if err := d.backend.Close(context.Background()); err != nil {
				slog.Warn("shutdown: backend close error", "error", err)
			}
			slog.Info("shutdown: backend closed")
		}
		if d.diskCache != nil {
			slog.Info("shutdown: closing disk cache")
			if err := d.diskCache.Close(); err != nil {
				slog.Warn("shutdown: disk cache close error", "error", err)
			}
			slog.Info("shutdown: disk cache closed")
		}
		close(d.doneCh)

		// Now stop accepting requests and remove sockets.
		d.removeOwnerLinks()
		slog.Info("shutdown: closing HTTP server")
		if err := srv.Close(); err != nil {
			slog.Warn("shutdown: http server close error", "error", err)
		}
		if err := os.Remove(d.socket); err != nil {
			slog.Warn("shutdown: remove socket", "path", d.socket, "error", err)
		}
		slog.Info("shutdown: complete")
		if d.axiomClose != nil {
			d.axiomClose()
		}
	}()

	slog.Info("server ready", "mode", "fuse", "socket", d.socket)
	err := srv.Serve(d.ln)
	if err == http.ErrServerClosed {
		err = nil
	}

	slog.Info("server stopped")
	return err
}

func (d *Server) shuttingDown() bool {
	select {
	case <-d.shutdownCh:
		return true
	default:
		return false
	}
}

// rejectIfShuttingDown returns true (and writes a 503) if the server is shutting down.
func (d *Server) rejectIfShuttingDown(w http.ResponseWriter) bool {
	if d.shuttingDown() {
		writeError(w, 503, fmt.Errorf("server is shutting down"))
		return true
	}
	return false
}

func (d *Server) mux(stop context.CancelFunc) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /device/checkpoint", d.handleDeviceCheckpoint)
	mux.HandleFunc("POST /device/clone", d.handleDeviceClone)
	mux.HandleFunc("POST /device/dd/write", d.handleDeviceDDWrite)
	mux.HandleFunc("GET /device/dd/read", d.handleDeviceDDRead)
	mux.HandleFunc("POST /device/dd/finalize", d.handleDeviceDDFinalize)

	mux.HandleFunc("POST /create", d.handleCreate)
	mux.HandleFunc("POST /mount", d.handleMount)
	mux.HandleFunc("POST /delete", d.handleDelete)
	mux.HandleFunc("POST /checkpoint", d.handleCheckpoint)
	mux.HandleFunc("POST /clone", d.handleClone)
	mux.HandleFunc("GET /checkpoints", d.handleListCheckpoints)
	registerVolumeCmds(mux, d)
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("shutdown requested")
		writeJSON(w, map[string]string{"status": "shutting_down"})
		stop()
	})
	mux.HandleFunc("GET /shutdown/wait", func(w http.ResponseWriter, r *http.Request) {
		<-d.doneCh
		writeJSON(w, map[string]string{"status": "done"})
	})

	mux.HandleFunc("GET /status", d.handleStatus)
	mux.HandleFunc("GET /volumes", d.handleListVolumes)
	mux.HandleFunc("GET /volume-info", d.handleVolumeInfo)
	mux.HandleFunc("GET /debug/volume", d.handleDebugVolume)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	// Also register at /pprof/ so goroutine dumps work through the CF
	// scheduler which strips the /debug/ prefix.
	mux.HandleFunc("GET /pprof/", pprof.Index)
	mux.HandleFunc("GET /pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /pprof/trace", pprof.Trace)
	return mux
}

// instrument wraps a handler with logging and metrics.
func (d *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip endpoints that hijack the connection or don't need metrics.
		if r.URL.Path == "/metrics" || len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/debug" || len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/pprof" || len(r.URL.Path) >= 9 && r.URL.Path[:9] == "/sandbox/" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(rw, r)
		dur := time.Since(start)

		code := strconv.Itoa(rw.code)
		metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, code).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur.Seconds())

		level := slog.LevelInfo
		if rw.code >= 500 {
			level = slog.LevelWarn
		}

		slog.Log(r.Context(), level, "http", "method", r.Method, "path", r.URL.Path, "status", rw.code, "dur", dur)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap returns the underlying ResponseWriter so that http.ResponseController
// can discover optional interfaces (Flusher, Hijacker, etc.) automatically.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// requireBackend returns true (and writes 503) if storage is not available.
func (d *Server) requireBackend(w http.ResponseWriter) bool {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return true
	}
	return false
}

var errNoVolume = fmt.Errorf("no volume managed")

// --- helpers ---

func readJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		if err == io.EOF {
			return nil // empty body — callers fall back to managed volume
		}
		return fmt.Errorf("decode request: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("writeJSON encode error", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, err error) {
	slog.Warn("returning http error", "code", code, "err", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if encErr := json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); encErr != nil {
		slog.Warn("writeError encode error", "error", encErr)
	}
}

func (d *Server) writeSymlink(mountpoint string) {
	symPath := d.dir.MountSymlink(mountpoint)
	if err := os.MkdirAll(filepath.Dir(symPath), 0o755); err != nil {
		slog.Warn("create symlink dir failed", "path", symPath, "error", err)
		return
	}
	if err := os.Remove(symPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove old symlink failed", "path", symPath, "error", err)
	}
	if err := os.Symlink(d.socket, symPath); err != nil {
		slog.Warn("create mount symlink failed", "path", symPath, "error", err)
	}
}

func (d *Server) writeDeviceSymlink(devicePath string) {
	symPath := d.dir.DeviceSymlink(devicePath)
	if err := os.MkdirAll(filepath.Dir(symPath), 0o755); err != nil {
		slog.Warn("create device symlink dir failed", "path", symPath, "error", err)
		return
	}
	if err := os.Remove(symPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove old device symlink failed", "path", symPath, "error", err)
	}
	if err := os.Symlink(d.socket, symPath); err != nil {
		slog.Warn("create device symlink failed", "path", symPath, "error", err)
	}
}

func (d *Server) removeOwnerLinks() {
	if d.mountpoint != "" {
		if err := os.Remove(d.dir.MountSymlink(d.mountpoint)); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove mount symlink failed", "mountpoint", d.mountpoint, "error", err)
		}
	}
	if d.devicePath != "" {
		if err := os.Remove(d.dir.DeviceSymlink(d.devicePath)); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove device symlink failed", "device", d.devicePath, "error", err)
		}
	}
}

func socketPathFromOptions(opts Options) string {
	return opts.SocketPath
}
