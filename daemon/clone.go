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
		if req.Volume == "" {
			writeError(w, 400, fmt.Errorf("volume is required when checkpoint is set"))
			return
		}
		slog.Info("clone", "volume", req.Volume, "checkpoint", req.Checkpoint, "clone", req.Clone)
		err = d.backend.CloneFromCheckpoint(r.Context(), req.Volume, req.Checkpoint, req.Clone)
	case req.Mountpoint != "":
		slog.Info("clone", "mountpoint", req.Mountpoint, "clone", req.Clone)
		err = d.backend.Clone(r.Context(), req.Mountpoint, req.Clone)
	default:
		writeError(w, 400, fmt.Errorf("mountpoint or volume+checkpoint is required"))
		return
	}
	if err != nil {
		slog.Error("clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Clone})
}
