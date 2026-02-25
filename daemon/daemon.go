//go:build linux

// Package daemon implements the loophole HTTP/UDS API.
// It is a thin remoting layer over fsbackend.Backend.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/fatih/color"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/metrics"
)

// Daemon serves the loophole HTTP API over a Unix socket.
type Daemon struct {
	inst    loophole.Instance
	dir     loophole.Dir
	mode    loophole.Mode
	backend fsbackend.Service
	ln      net.Listener
	log     *slog.Logger
}

// Start initializes everything and returns a Daemon ready to Serve.
func Start(ctx context.Context, inst loophole.Instance, dir loophole.Dir, debug, foreground bool, socketMode os.FileMode, mode loophole.Mode, s3opts *loophole.S3Options) (*Daemon, error) {
	logPath := dir.Log(inst)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	fileHandler := slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: level})
	var handler slog.Handler = fileHandler
	if foreground {
		handler = multiHandler{fileHandler, &consoleHandler{level: level}}
	}
	logger := slog.New(handler)
	logger.Info("starting daemon", "s3", inst.S3URL(), "log", logPath)

	vm, err := loophole.SetupVolumeManager(ctx, inst, dir, s3opts, logger)
	if err != nil {
		return nil, err
	}

	if mode == "" {
		mode = loophole.DefaultMode()
	}

	var backend fsbackend.Service
	switch mode {
	case loophole.ModeNBD:
		backend, err = fsbackend.NewNBD(vm, nil)
		if err != nil {
			return nil, fmt.Errorf("start NBD backend: %w", err)
		}
	case loophole.ModeTestNBDTCP:
		backend, err = fsbackend.NewNBDTCP(vm, nil)
		if err != nil {
			return nil, fmt.Errorf("start NBD TCP backend: %w", err)
		}
	case loophole.ModeLwext4FUSE:
		backend = fsbackend.NewLwext4FUSE(vm, nil)
	default:
		backend, err = fsbackend.NewFUSE(dir.Fuse(inst), vm, &fuseblockdev.Options{Debug: debug})
		if err != nil {
			return nil, fmt.Errorf("start FUSE backend: %w", err)
		}
	}

	sockPath := dir.Socket(inst)
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return nil, err
	}
	os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", sockPath, err)
	}
	if socketMode != 0 {
		if err := os.Chmod(sockPath, socketMode); err != nil {
			ln.Close()
			return nil, fmt.Errorf("chmod socket: %w", err)
		}
	}

	return &Daemon{
		inst:    inst,
		dir:     dir,
		mode:    mode,
		backend: backend,
		ln:      ln,
		log:     logger,
	}, nil
}

// Serve blocks, handling HTTP requests until the context is cancelled.
func (d *Daemon) Serve(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Handler: d.mux(stop)}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	d.log.Info("daemon ready", "mode", d.mode, "socket", d.dir.Socket(d.inst))
	err := srv.Serve(d.ln)
	if err == http.ErrServerClosed {
		err = nil
	}

	d.log.Info("shutting down")
	if err := d.backend.Close(context.Background()); err != nil {
		d.log.Warn("backend close error", "error", err)
	}
	sockPath := d.dir.Socket(d.inst)
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		d.log.Warn("remove socket failed", "path", sockPath, "error", err)
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
	mux.HandleFunc("POST /mount", d.handleMount)
	mux.HandleFunc("POST /unmount", d.handleUnmount)
	mux.HandleFunc("POST /snapshot", d.handleSnapshot)
	mux.HandleFunc("POST /clone", d.handleClone)
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		d.log.Info("shutdown requested")
		writeJSON(w, map[string]string{"status": "ok"})
		stop()
	})

	mux.HandleFunc("GET /status", d.handleStatus)
	mux.HandleFunc("GET /volumes", d.handleListVolumes)
	mux.Handle("GET /metrics", metrics.Handler())

	return mux
}

// --- High-level handlers ---

func (d *Daemon) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	d.log.Info("create", "volume", req.Volume)
	if err := d.backend.Create(r.Context(), req.Volume); err != nil {
		d.log.Error("create failed", "err", err)
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

	d.log.Info("mount", "volume", req.Volume, "mountpoint", req.Mountpoint)
	if err := d.backend.Mount(r.Context(), req.Volume, req.Mountpoint); err != nil {
		d.log.Error("mount failed", "err", err)
		writeError(w, 500, err)
		return
	}

	d.writeSymlink(req.Mountpoint)
	writeJSON(w, map[string]string{"status": "ok", "mountpoint": req.Mountpoint})
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
	os.Remove(d.dir.MountSymlink(req.Mountpoint))
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

// --- Status ---

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"s3":      d.inst.S3URL(),
		"mode":    string(d.mode),
		"socket":  d.dir.Socket(d.inst),
		"log":     d.dir.Log(d.inst),
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
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (d *Daemon) writeSymlink(mountpoint string) {
	symPath := d.dir.MountSymlink(mountpoint)
	if err := os.MkdirAll(filepath.Dir(symPath), 0o755); err != nil {
		d.log.Warn("create symlink dir failed", "path", symPath, "error", err)
		return
	}
	os.Remove(symPath) // best-effort, may not exist
	if err := os.Symlink(d.dir.Socket(d.inst), symPath); err != nil {
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
