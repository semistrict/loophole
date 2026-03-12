package daemon

import (
	"fmt"
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

	slog.Info("checkpoint", "mountpoint", req.Mountpoint)
	cpID, err := d.backend.Checkpoint(r.Context(), req.Mountpoint)
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

	slog.Info("device checkpoint", "volume", req.Volume)
	cpID, err := d.backend.DeviceCheckpoint(r.Context(), req.Volume)
	if err != nil {
		slog.Error("device checkpoint failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (d *Daemon) handleCloneFromCheckpoint(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume          string `json:"volume"`
		Checkpoint      string `json:"checkpoint"`
		Clone           string `json:"clone"`
		CloneMountpoint string `json:"clone_mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("clone from checkpoint", "volume", req.Volume, "checkpoint", req.Checkpoint, "clone", req.Clone)
	if err := d.backend.CloneFromCheckpoint(r.Context(), req.Volume, req.Checkpoint, req.Clone, req.CloneMountpoint); err != nil {
		slog.Error("clone from checkpoint failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	volume := r.URL.Query().Get("volume")
	if volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}

	checkpoints, err := d.backend.VM().ListCheckpoints(r.Context(), volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"checkpoints": checkpoints})
}
