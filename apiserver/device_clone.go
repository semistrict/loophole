package apiserver

import (
	"fmt"
	"log/slog"
	"net/http"
)

func (d *Server) handleDeviceClone(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Checkpoint string `json:"checkpoint"`
		Clone      string `json:"clone"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	if req.Clone == "" {
		writeError(w, 400, fmt.Errorf("missing clone parameter"))
		return
	}
	name := d.volumeName()
	if name == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	slog.Info("device/clone", "volume", name, "checkpoint", req.Checkpoint, "clone", req.Clone)
	err := d.backend.DeviceClone(r.Context(), name, req.Checkpoint, req.Clone)
	if err != nil {
		slog.Error("device/clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Clone})
}
