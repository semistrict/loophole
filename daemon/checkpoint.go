package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	mountpoint := req.Mountpoint
	if mountpoint == "" {
		mountpoint = d.currentMountpoint()
	}
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

func (d *Daemon) handleDeviceCheckpoint(w http.ResponseWriter, r *http.Request) {
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
	volume := req.Volume
	if volume == "" {
		volume = d.managedVolume
	}
	if volume == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	slog.Info("device checkpoint", "volume", volume)
	cpID, err := d.backend.DeviceCheckpoint(r.Context(), volume)
	if err != nil {
		slog.Error("device checkpoint failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (d *Daemon) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	volume := r.URL.Query().Get("volume")
	if volume == "" {
		volume = d.managedVolume
	}
	if volume == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	checkpoints, err := d.backend.VM().ListCheckpoints(r.Context(), volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"checkpoints": checkpoints})
}
