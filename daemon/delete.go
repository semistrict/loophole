package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleDelete(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("delete", "volume", req.Volume)
	if err := d.backend.VM().CloseVolume(req.Volume); err != nil {
		slog.Warn("delete: close volume", "volume", req.Volume, "err", err)
	}
	if err := d.backend.VM().WaitClosed(r.Context(), req.Volume); err != nil {
		slog.Error("delete: wait closed failed", "volume", req.Volume, "err", err)
		writeError(w, 500, err)
		return
	}
	if err := d.backend.VM().DeleteVolume(r.Context(), req.Volume); err != nil {
		slog.Error("delete failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
