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

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/metrics"
)

const (
	defaultBlockSize    = 4 * 1024 * 1024
	defaultMaxUploads   = 20
	defaultMaxDownloads = 200
)

// Daemon serves the loophole HTTP API over a Unix socket.
type Daemon struct {
	inst    loophole.Instance
	dir     loophole.Dir
	mode    loophole.Mode
	backend *fsbackend.Backend
	ln      net.Listener
	log     *slog.Logger
}

// Start initializes everything and returns a Daemon ready to Serve.
func Start(ctx context.Context, inst loophole.Instance, dir loophole.Dir, debug bool, s3opts *loophole.S3Options) (*Daemon, error) {
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
	logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{Level: level}))
	logger.Info("starting daemon", "s3", inst.S3URL(), "log", logPath)

	store, err := loophole.NewS3Store(ctx, inst, s3opts)
	if err != nil {
		return nil, fmt.Errorf("connect to S3: %w", err)
	}

	if err := loophole.FormatSystem(ctx, store, defaultBlockSize); err != nil {
		logger.Debug("format system (may already exist)", "error", err)
	}

	vm, err := loophole.NewVolumeManager(ctx, store, dir.Cache(inst), defaultMaxUploads, defaultMaxDownloads)
	if err != nil {
		return nil, fmt.Errorf("init volume manager: %w", err)
	}

	mode := loophole.ModeFromEnv()

	var backend *fsbackend.Backend
	switch mode {
	case loophole.ModeNBD:
		backend, err = fsbackend.NewNBD(vm, nil)
		if err != nil {
			return nil, fmt.Errorf("start NBD backend: %w", err)
		}
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

	// Start Prometheus metrics server in background.
	go metrics.ListenAndServe(":9090", logger)

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

	srv := &http.Server{Handler: d.mux()}
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

func (d *Daemon) mux() *http.ServeMux {
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

	mux.HandleFunc("GET /status", d.handleStatus)
	mux.HandleFunc("GET /volumes", d.handleListVolumes)

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
