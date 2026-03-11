package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleDeviceDetach(w http.ResponseWriter, r *http.Request) {
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

	slog.Info("device/detach", "volume", req.Volume)
	if err := d.backend.DeviceDetach(r.Context(), req.Volume); err != nil {
		slog.Error("device/detach failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
