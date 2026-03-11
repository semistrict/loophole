package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleFreeze(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}
	var req struct {
		Volume  string `json:"volume"`
		Compact bool   `json:"compact"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	slog.Info("freeze", "volume", req.Volume, "compact", req.Compact)
	if err := d.backend.FreezeVolume(r.Context(), req.Volume, req.Compact); err != nil {
		slog.Error("freeze failed", "volume", req.Volume, "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
