// Package fsserver implements the loophole HTTP/UDS API with filesystem
// (FUSE/ext4) support. It embeds volserver for pure volume endpoints and
// adds freeze/thaw-wrapped checkpoint.
package fsserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/storage"
	"github.com/semistrict/loophole/volserver"
)

// server serves the loophole HTTP API over a Unix socket.
// It embeds a volserver.Server for volume-level endpoints and adds
// filesystem-specific routes (freeze/thaw checkpoint).
type server struct {
	inst      env.ResolvedStore
	dir       env.Dir
	socket    string
	backend   *Backend
	diskCache storage.PageCache
	ln        net.Listener
	vs        *volserver.Server // embedded volume server (nil until volume is set)

	// mountpoint and devicePath are retained for symlink cleanup after
	// the backend is closed during shutdown.
	mountpoint string
	devicePath string
	logPath    string

	axiomClose func() // flushes and closes the axiom handler; nil if axiom is not configured

	shutdownCh chan struct{} // closed when shutdown begins
	doneCh     chan struct{} // closed when cleanup is complete
}

// setVolume creates the embedded volserver for the given volume.
func (d *server) setVolume(vol *storage.Volume) {
	d.vs = volserver.New(vol)
}

// volumeName returns the managed volume name, or "".
func (d *server) volumeName() string {
	if d.backend == nil {
		return ""
	}
	if v := d.backend.VM().Volume(); v != nil {
		return v.Name()
	}
	return ""
}

// volume returns the managed volume, or nil.
func (d *server) volume() *storage.Volume {
	if d.backend == nil {
		return nil
	}
	return d.backend.VM().Volume()
}

// serve blocks, handling HTTP requests until the context is cancelled.
func (d *server) serve(ctx context.Context) error {
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

	slog.Info("server ready", "mode", d.backend.Mode(), "socket", d.socket)
	err := srv.Serve(d.ln)
	if err == http.ErrServerClosed {
		err = nil
	}

	slog.Info("server stopped")
	return err
}

func (d *server) shuttingDown() bool {
	select {
	case <-d.shutdownCh:
		return true
	default:
		return false
	}
}

func (d *server) mux(stop context.CancelFunc) *http.ServeMux {
	mux := http.NewServeMux()

	vs := d.vs
	if vs == nil {
		vs = volserver.New(nil)
	}

	// Debug endpoint is safe in both modes.
	mux.HandleFunc("GET /debug/volume", vs.HandleDebugVolume)

	if d.devicePath != "" {
		// Device mode — register raw volume endpoints under /device/.
		// No FS-level freeze/thaw routes.
		mux.HandleFunc("POST /device/flush", vs.HandleFlush)
		mux.HandleFunc("POST /device/checkpoint", vs.HandleCheckpoint)
		mux.HandleFunc("POST /device/dd/write", vs.HandleDeviceDDWrite)
		mux.HandleFunc("GET /device/dd/read", vs.HandleDeviceDDRead)
		mux.HandleFunc("GET /device/dd/size", vs.HandleDeviceDDSize)
		mux.HandleFunc("POST /device/dd/finalize", vs.HandleDeviceDDFinalize)
	} else {
		// FS mode — register freeze/thaw-wrapped endpoints.
		// No raw device routes; callers must use the FS-aware API.
		mux.HandleFunc("POST /flush", vs.HandleFlush)
		mux.HandleFunc("POST /checkpoint", d.handleCheckpoint)
	}

	mux.HandleFunc("GET /status", d.handleStatus)

	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("shutdown requested")
		volserver.WriteJSON(w, map[string]string{"status": "shutting_down"})
		stop()
	})
	mux.HandleFunc("GET /shutdown/wait", func(w http.ResponseWriter, r *http.Request) {
		<-d.doneCh
		volserver.WriteJSON(w, map[string]string{"status": "done"})
	})

	volserver.RegisterObservabilityRoutes(mux)
	return mux
}

// instrument wraps a handler with logging and metrics.
func (d *server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip endpoints that hijack the connection or don't need metrics.
		if r.URL.Path == "/metrics" || len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/debug" || len(r.URL.Path) >= 9 && r.URL.Path[:9] == "/sandbox/" {
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

var errNoVolume = fmt.Errorf("no volume managed")

func (d *server) writeSymlink(mountpoint string) {
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

func (d *server) writeDeviceSymlink(devicePath string) {
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

func (d *server) removeOwnerLinks() {
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

// cleanup tears down resources after a setup failure before serve runs.
func (d *server) cleanup(ctx context.Context) {
	d.removeOwnerLinks()
	if d.backend != nil {
		_ = d.backend.Close(ctx)
	}
	if d.diskCache != nil {
		_ = d.diskCache.Close()
	}
	if d.ln != nil {
		_ = d.ln.Close()
	}
	if d.socket != "" {
		_ = os.Remove(d.socket)
	}
	if d.axiomClose != nil {
		d.axiomClose()
	}
}
