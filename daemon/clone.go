package daemon

import (
	"fmt"
	"log/slog"
	"net/http"
)

func (d *Daemon) handleClone(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Mountpoint string `json:"mountpoint"`
		Volume     string `json:"volume"`
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

	var err error
	switch {
	case req.Checkpoint != "":
		volume := req.Volume
		if volume == "" {
			volume = d.managedVolume
		}
		if volume == "" {
			writeError(w, 400, errNoVolume)
			return
		}
		slog.Info("clone", "volume", volume, "checkpoint", req.Checkpoint, "clone", req.Clone)
		err = d.backend.CloneFromCheckpoint(r.Context(), volume, req.Checkpoint, req.Clone)
	default:
		mountpoint := req.Mountpoint
		if mountpoint == "" {
			mountpoint = d.currentMountpoint()
		}
		if mountpoint == "" {
			writeError(w, 400, fmt.Errorf("no volume mounted"))
			return
		}
		slog.Info("clone", "mountpoint", mountpoint, "clone", req.Clone)
		err = d.backend.Clone(r.Context(), mountpoint, req.Clone)
	}
	if err != nil {
		slog.Error("clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Clone})
}
