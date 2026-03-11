package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Mountpoint string `json:"mountpoint"`
		Name       string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("snapshot", "mountpoint", req.Mountpoint, "name", req.Name)
	if err := d.backend.Snapshot(r.Context(), req.Mountpoint, req.Name); err != nil {
		slog.Error("snapshot failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
