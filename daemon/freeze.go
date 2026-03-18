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
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	volume := req.Volume
	if volume == "" {
		volume = d.managedVolume
	}
	if volume == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	slog.Info("freeze", "volume", volume)
	if err := d.backend.FreezeVolume(r.Context(), volume); err != nil {
		slog.Error("freeze failed", "volume", volume, "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
