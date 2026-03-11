package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleDeviceSnapshot(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume   string `json:"volume"`
		Snapshot string `json:"snapshot"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("device/snapshot", "volume", req.Volume, "snapshot", req.Snapshot)
	if err := d.backend.DeviceSnapshot(r.Context(), req.Volume, req.Snapshot); err != nil {
		slog.Error("device/snapshot failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
