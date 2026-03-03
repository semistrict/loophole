// Package daemon implements the loophole HTTP/UDS API.
// It is a thin remoting layer over fsbackend.Backend.
package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/nbdserve"
)

// Daemon serves the loophole HTTP API over a Unix socket.
type Daemon struct {
	inst    loophole.Instance
	dir     loophole.Dir
	backend fsbackend.Service
	ln      net.Listener
	log     *slog.Logger

	nbdSock string       // set once NBD server is started
	nbdLn   net.Listener // NBD listener, closed on shutdown
}

// Start initializes everything and returns a Daemon ready to Serve.
func Start(ctx context.Context, inst loophole.Instance, dir loophole.Dir, foreground bool, socketMode os.FileMode) (*Daemon, error) {
	if inst.Mode == "" {
		inst.Mode = loophole.DefaultMode()
	}
	logPath := dir.Log(inst.ProfileName)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}

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
	var handler slog.Handler = fileHandler
	if foreground {
		handler = multiHandler{fileHandler, &consoleHandler{level: level}}
	}
	logger := slog.New(handler)
	logger.Info("starting daemon", "s3", inst.URL(), "log", logPath)

	tuneProcess(logger)

	vm, err := loophole.SetupVolumeManager(ctx, inst, dir, logger)
	if err != nil {
		return nil, err
	}

	backend, err := createBackend(vm, inst, dir)
	if err != nil {
		return nil, err
	}

	sockPath := dir.Socket(inst.ProfileName)
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("remove stale socket failed", "path", sockPath, "error", err)
	}

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", sockPath, err)
	}
	if socketMode != 0 {
		if err := os.Chmod(sockPath, socketMode); err != nil {
			if closeErr := ln.Close(); closeErr != nil {
				logger.Warn("listener close error", "error", closeErr)
			}
			return nil, fmt.Errorf("chmod socket: %w", err)
		}
	}

	d := &Daemon{
		inst:    inst,
		dir:     dir,
		backend: backend,
		ln:      ln,
		log:     logger,
	}

	// Auto-start NBD server if configured.
	if inst.NBDSocket != "" {
		if err := d.startNBD(inst.NBDSocket); err != nil {
			return nil, fmt.Errorf("start NBD server: %w", err)
		}
	}

	return d, nil
}

// Serve blocks, handling HTTP requests until the context is cancelled.
func (d *Daemon) Serve(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Handler: d.instrument(d.mux(stop))}
	go func() {
		<-ctx.Done()
		// Close NBD and HTTP sockets immediately so a new daemon
		// can bind them without racing against our shutdown cleanup.
		if d.nbdLn != nil {
			if err := d.nbdLn.Close(); err != nil {
				d.log.Warn("NBD listener close error", "error", err)
			}
			if err := os.Remove(d.nbdSock); err != nil {
				d.log.Warn("remove NBD socket", "path", d.nbdSock, "error", err)
			}
		}
		sockPath := d.dir.Socket(d.inst.ProfileName)
		if err := os.Remove(sockPath); err != nil {
			d.log.Warn("remove socket", "path", sockPath, "error", err)
		}
		if err := srv.Close(); err != nil {
			d.log.Warn("http server close error", "error", err)
		}
	}()

	d.log.Info("daemon ready", "mode", d.inst.Mode, "socket", d.dir.Socket(d.inst.ProfileName))
	err := srv.Serve(d.ln)
	if err == http.ErrServerClosed {
		err = nil
	}

	d.log.Info("shutting down")
	if err := d.backend.Close(context.Background()); err != nil {
		d.log.Warn("backend close error", "error", err)
	}
	d.log.Info("daemon stopped")
	return err
}

func (d *Daemon) mux(stop context.CancelFunc) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /device/mount", d.handleDeviceMount)
	mux.HandleFunc("POST /device/unmount", d.handleDeviceUnmount)
	mux.HandleFunc("POST /device/snapshot", d.handleDeviceSnapshot)
	mux.HandleFunc("POST /device/clone", d.handleDeviceClone)

	mux.HandleFunc("POST /create", d.handleCreate)
	mux.HandleFunc("POST /delete", d.handleDelete)
	mux.HandleFunc("POST /mount", d.handleMount)
	mux.HandleFunc("POST /unmount", d.handleUnmount)
	mux.HandleFunc("POST /snapshot", d.handleSnapshot)
	mux.HandleFunc("POST /clone", d.handleClone)
	mux.HandleFunc("GET /file", d.handleFile)
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		d.log.Info("shutdown requested")
		writeJSON(w, map[string]string{"status": "ok"})
		stop()
	})

	mux.HandleFunc("GET /status", d.handleStatus)
	mux.HandleFunc("GET /volumes", d.handleListVolumes)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	return mux
}

// instrument wraps a handler with logging and metrics.
func (d *Daemon) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip endpoints that hijack the connection or don't need metrics.
		if r.URL.Path == "/file" || r.URL.Path == "/metrics" || len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/debug" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, code: 200} // XXX - there is a proper way to wrap this so that e.g. Hijacker etc still work
		next.ServeHTTP(rw, r)
		dur := time.Since(start)

		code := strconv.Itoa(rw.code)
		metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, code).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur.Seconds())

		level := slog.LevelInfo
		if rw.code >= 500 {
			level = slog.LevelWarn
		}

		d.log.Log(r.Context(), level, "http", "method", r.Method, "path", r.URL.Path, "status", rw.code, "dur", dur)
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

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support Hijack")
}

// --- High-level handlers ---

func (d *Daemon) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req client.CreateParams
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("create", "volume", req.Volume, "size", req.Size, "no_format", req.NoFormat)
	if err := d.backend.Create(r.Context(), req); err != nil {
		d.log.Error("create failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("delete", "volume", req.Volume)
	if err := d.backend.VM().DeleteVolume(r.Context(), req.Volume); err != nil {
		d.log.Error("delete failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleMount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume     string `json:"volume"`
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	mountpoint := req.Mountpoint
	if mountpoint == "" {
		mountpoint = req.Volume
	}
	d.log.Info("mount", "volume", req.Volume, "mountpoint", mountpoint)
	if err := d.backend.Mount(r.Context(), req.Volume, mountpoint); err != nil {
		d.log.Error("mount failed", "err", err)
		writeError(w, 500, err)
		return
	}

	if req.Mountpoint != "" {
		d.writeSymlink(mountpoint)
	}
	writeJSON(w, map[string]string{"status": "ok", "mountpoint": mountpoint})
}

func (d *Daemon) handleUnmount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("unmount", "mountpoint", req.Mountpoint)
	if err := d.backend.Unmount(r.Context(), req.Mountpoint); err != nil {
		d.log.Error("unmount failed", "err", err)
		writeError(w, 500, err)
		return
	}
	if err := os.Remove(d.dir.MountSymlink(req.Mountpoint)); err != nil && !os.IsNotExist(err) {
		d.log.Warn("remove mount symlink failed", "error", err)
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mountpoint string `json:"mountpoint"`
		Name       string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("snapshot", "mountpoint", req.Mountpoint, "name", req.Name)
	if err := d.backend.Snapshot(r.Context(), req.Mountpoint, req.Name); err != nil {
		d.log.Error("snapshot failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleClone(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mountpoint      string `json:"mountpoint"`
		Clone           string `json:"clone"`
		CloneMountpoint string `json:"clone_mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("clone", "mountpoint", req.Mountpoint, "clone", req.Clone)
	if err := d.backend.Clone(r.Context(), req.Mountpoint, req.Clone, req.CloneMountpoint); err != nil {
		d.log.Error("clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	d.writeSymlink(req.CloneMountpoint)
	writeJSON(w, map[string]string{"status": "ok", "mountpoint": req.CloneMountpoint})
}

// --- Device handlers ---

func (d *Daemon) handleDeviceMount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("device/mount", "volume", req.Volume)
	device, err := d.backend.DeviceMount(r.Context(), req.Volume)
	if err != nil {
		d.log.Error("device/mount failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"device": device})
}

func (d *Daemon) handleDeviceUnmount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("device/unmount", "volume", req.Volume)
	if err := d.backend.DeviceUnmount(r.Context(), req.Volume); err != nil {
		d.log.Error("device/unmount failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDeviceSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume   string `json:"volume"`
		Snapshot string `json:"snapshot"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("device/snapshot", "volume", req.Volume, "snapshot", req.Snapshot)
	if err := d.backend.DeviceSnapshot(r.Context(), req.Volume, req.Snapshot); err != nil {
		d.log.Error("device/snapshot failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDeviceClone(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume string `json:"volume"`
		Clone  string `json:"clone"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("device/clone", "volume", req.Volume, "clone", req.Clone)
	device, err := d.backend.DeviceClone(r.Context(), req.Volume, req.Clone)
	if err != nil {
		d.log.Error("device/clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"device": device})
}

// --- NBD ---

func (d *Daemon) startNBD(sockPath string) error {
	if err := os.MkdirAll(filepath.Dir(sockPath), 0700); err != nil {
		return fmt.Errorf("create NBD dir: %w", err)
	}

	// Remove any leftover socket from a previous daemon.
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		d.log.Warn("remove stale NBD socket", "path", sockPath, "error", err)
	}

	srv := nbdserve.NewServer(d.backend.VM())
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listen NBD socket %s: %w", sockPath, err)
	}

	d.nbdSock = sockPath
	d.nbdLn = ln
	d.log.Info("NBD server started", "socket", sockPath)

	go func() {
		if err := srv.ServeListener(ln); err != nil {
			d.log.Error("NBD server error", "error", err)
		}
	}()

	return nil
}

// --- Status ---

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"s3":      d.inst.URL(),
		"mode":    string(d.inst.Mode),
		"socket":  d.dir.Socket(d.inst.ProfileName),
		"log":     d.dir.Log(d.inst.ProfileName),
		"volumes": d.backend.VM().Volumes(),
		"mounts":  d.backend.Mounts(),
	})
}

func (d *Daemon) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	names, err := d.backend.VM().ListAllVolumes(r.Context())
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"volumes": names})
}

// --- helpers ---

func readJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
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

func (d *Daemon) writeSymlink(mountpoint string) {
	symPath := d.dir.MountSymlink(mountpoint)
	if err := os.MkdirAll(filepath.Dir(symPath), 0o755); err != nil {
		d.log.Warn("create symlink dir failed", "path", symPath, "error", err)
		return
	}
	if err := os.Remove(symPath); err != nil && !os.IsNotExist(err) {
		d.log.Warn("remove old symlink failed", "path", symPath, "error", err)
	}
	if err := os.Symlink(d.dir.Socket(d.inst.ProfileName), symPath); err != nil {
		d.log.Warn("create mount symlink failed", "path", symPath, "error", err)
	}
}

// consoleHandler writes human-friendly log lines to stderr.
type consoleHandler struct {
	level slog.Level
	attrs []slog.Attr
}

func (h *consoleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

// XXX - move this logger stuff to a different file
var (
	colorTime  = color.New(color.FgHiBlack)
	colorDebug = color.New(color.FgHiBlack)
	colorInfo  = color.New(color.FgCyan)
	colorWarn  = color.New(color.FgYellow)
	colorError = color.New(color.FgRed, color.Bold)
	colorKey   = color.New(color.FgHiBlack)
	colorMsg   = color.New(color.FgWhite, color.Bold)
)

func levelColor(l slog.Level) *color.Color {
	switch {
	case l >= slog.LevelError:
		return colorError
	case l >= slog.LevelWarn:
		return colorWarn
	case l >= slog.LevelInfo:
		return colorInfo
	default:
		return colorDebug
	}
}

func (h *consoleHandler) Handle(_ context.Context, r slog.Record) error {
	var buf []byte
	buf = append(buf, colorTime.Sprint(r.Time.Format("15:04:05"))...)
	buf = append(buf, ' ')
	lc := levelColor(r.Level)
	buf = append(buf, lc.Sprintf("%-5s", r.Level.String())...)
	buf = append(buf, ' ')
	buf = append(buf, colorMsg.Sprint(r.Message)...)
	appendAttr := func(a slog.Attr) {
		buf = append(buf, ' ')
		buf = append(buf, colorKey.Sprint(a.Key)...)
		buf = append(buf, '=')
		buf = append(buf, a.Value.String()...)
	}
	for _, a := range h.attrs {
		appendAttr(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(a)
		return true
	})
	buf = append(buf, '\n')
	_, err := os.Stderr.Write(buf)
	return err
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &consoleHandler{level: h.level, attrs: append(h.attrs[:len(h.attrs):len(h.attrs)], attrs...)}
}

func (h *consoleHandler) WithGroup(_ string) slog.Handler {
	return h
}

// multiHandler fans out log records to multiple handlers.
type multiHandler []slog.Handler

func (m multiHandler) Enabled(_ context.Context, l slog.Level) bool {
	for _, h := range m {
		if h.Enabled(context.Background(), l) {
			return true
		}
	}
	return false
}

func (m multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make(multiHandler, len(m))
	for i, h := range m {
		handlers[i] = h.WithAttrs(attrs)
	}
	return handlers
}

func (m multiHandler) WithGroup(name string) slog.Handler {
	handlers := make(multiHandler, len(m))
	for i, h := range m {
		handlers[i] = h.WithGroup(name)
	}
	return handlers
}
