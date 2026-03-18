package apiserver

import (
	"fmt"
	"log/slog"
	"net/http"
)

func (d *Server) handleClone(w http.ResponseWriter, r *http.Request) {
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

	var err error
	if req.Checkpoint != "" {
		slog.Info("clone from checkpoint", "volume", name, "checkpoint", req.Checkpoint, "clone", req.Clone)
		err = d.backend.CloneFromCheckpoint(r.Context(), name, req.Checkpoint, req.Clone)
	} else {
		mountpoint := d.currentMountpoint()
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
