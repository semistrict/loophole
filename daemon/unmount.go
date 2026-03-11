package daemon

import (
	"log/slog"
	"net/http"
	"os"
)

func (d *Daemon) handleUnmount(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("unmount", "mountpoint", req.Mountpoint)
	if err := d.backend.Unmount(r.Context(), req.Mountpoint); err != nil {
		slog.Error("unmount failed", "err", err)
		writeError(w, 500, err)
		return
	}
	if err := os.Remove(d.dir.MountSymlink(req.Mountpoint)); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove mount symlink failed", "error", err)
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
