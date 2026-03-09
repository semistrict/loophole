package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/semistrict/loophole/fsbackend"
)

// startChrootSocket creates a restricted Unix socket that is bind-mounted into
// the chroot at /.loophole. It serves only flush, snapshot, and clone
// operations, scoped to the volume mounted at the given mountpoint.
func startChrootSocket(mountpoint string, backend fsbackend.Service) (cleanup func(), err error) {
	// Host-side socket in a temp directory.
	hostSock := filepath.Join(os.TempDir(), "loophole-chroot-"+filepath.Base(mountpoint)+".sock")
	_ = os.Remove(hostSock)

	ln, err := net.Listen("unix", hostSock)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", hostSock, err)
	}
	if err := os.Chmod(hostSock, 0o666); err != nil {
		_ = ln.Close()
		_ = os.Remove(hostSock)
		return nil, fmt.Errorf("chmod %s: %w", hostSock, err)
	}

	// Create an empty file as the bind-mount target, then bind-mount the
	// host socket over it so processes inside the chroot see /.loophole.
	chrootSock := filepath.Join(mountpoint, ".loophole")
	_ = os.Remove(chrootSock)
	if f, err := os.Create(chrootSock); err == nil {
		_ = f.Close()
	}
	if out, err := exec.Command("mount", "--bind", hostSock, chrootSock).CombinedOutput(); err != nil {
		slog.Warn("chroot socket bind mount failed", "error", err, "output", string(out))
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /flush", func(w http.ResponseWriter, r *http.Request) {
		volName := backend.VolumeAt(mountpoint)
		if volName == "" {
			writeError(w, 500, fmt.Errorf("no volume at %s", mountpoint))
			return
		}
		vol := backend.VM().GetVolume(volName)
		if vol == nil {
			writeError(w, 500, fmt.Errorf("volume %s not open", volName))
			return
		}
		slog.Info("chroot flush", "volume", volName)
		if err := vol.Flush(); err != nil {
			writeError(w, 500, fmt.Errorf("flush: %w", err))
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, 400, err)
			return
		}
		if req.Name == "" {
			writeError(w, 400, fmt.Errorf("name is required"))
			return
		}
		slog.Info("chroot snapshot", "mountpoint", mountpoint, "name", req.Name)
		if err := backend.Snapshot(r.Context(), mountpoint, req.Name); err != nil {
			writeError(w, 500, fmt.Errorf("snapshot: %w", err))
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /clone", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Clone string `json:"clone"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, 400, err)
			return
		}
		if req.Clone == "" {
			writeError(w, 400, fmt.Errorf("clone name is required"))
			return
		}
		// Create a temporary mount directory for kernel-mode backends (FUSE/NBD)
		// where the mountpoint must be a real directory.
		cloneMP, err := os.MkdirTemp("", "loophole-clone-")
		if err != nil {
			writeError(w, 500, fmt.Errorf("create clone mountpoint: %w", err))
			return
		}
		slog.Info("chroot clone", "mountpoint", mountpoint, "clone", req.Clone, "clone_mountpoint", cloneMP)
		if err := backend.Clone(r.Context(), mountpoint, req.Clone, cloneMP); err != nil {
			_ = os.Remove(cloneMP)
			writeError(w, 500, fmt.Errorf("clone: %w", err))
			return
		}
		writeJSON(w, map[string]string{"status": "ok", "volume": req.Clone, "mountpoint": cloneMP})
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"status": "ok", "volume": backend.VolumeAt(mountpoint)})
	})

	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("chroot socket server error", "mountpoint", mountpoint, "error", err)
		}
	}()

	slog.Info("chroot socket started", "host", hostSock, "chroot", chrootSock, "volume", backend.VolumeAt(mountpoint))

	cleanup = func() {
		_ = srv.Close()
		if out, err := exec.Command("umount", "-n", "-l", chrootSock).CombinedOutput(); err != nil {
			slog.Warn("chroot socket umount failed", "error", err, "output", string(out))
		}
		_ = os.Remove(chrootSock)
		_ = os.Remove(hostSock)
	}
	return cleanup, nil
}
