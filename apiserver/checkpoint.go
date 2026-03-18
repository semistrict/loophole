package apiserver

import (
	"log/slog"
	"net/http"
)

func (d *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	mountpoint := d.currentMountpoint()
	if mountpoint == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	slog.Info("checkpoint", "mountpoint", mountpoint)
	cpID, err := d.backend.Checkpoint(r.Context(), mountpoint)
	if err != nil {
		slog.Error("checkpoint failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (d *Server) handleDeviceCheckpoint(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	vol := d.volume()
	if vol == nil {
		writeError(w, 400, errNoVolume)
		return
	}
	slog.Info("device checkpoint", "volume", vol.Name())
	cpID, err := d.backend.DeviceCheckpoint(r.Context(), vol.Name())
	if err != nil {
		slog.Error("device checkpoint failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (d *Server) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	name := d.volumeName()
	if name == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	checkpoints, err := d.backend.VM().ListCheckpoints(r.Context(), name)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"checkpoints": checkpoints})
}
