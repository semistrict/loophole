package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleBreakLease(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
		Force  bool   `json:"force"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("break-lease", "volume", req.Volume, "force", req.Force)
	graceful, err := d.backend.VM().BreakLease(r.Context(), req.Volume, req.Force)
	if err != nil {
		slog.Error("break-lease failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"status": "ok", "graceful": graceful})
}
