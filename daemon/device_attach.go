package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleDeviceAttach(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("device/attach", "volume", req.Volume)
	device, err := d.backend.DeviceAttach(r.Context(), req.Volume)
	if err != nil {
		slog.Error("device/attach failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"device": device})
}
