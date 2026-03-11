package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleDeviceClone(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
		Clone  string `json:"clone"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("device/clone", "volume", req.Volume, "clone", req.Clone)
	device, err := d.backend.DeviceClone(r.Context(), req.Volume, req.Clone)
	if err != nil {
		slog.Error("device/clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"device": device})
}
