package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleClone(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Mountpoint      string `json:"mountpoint"`
		Clone           string `json:"clone"`
		CloneMountpoint string `json:"clone_mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("clone", "mountpoint", req.Mountpoint, "clone", req.Clone)
	if err := d.backend.Clone(r.Context(), req.Mountpoint, req.Clone, req.CloneMountpoint); err != nil {
		slog.Error("clone failed", "err", err)
		writeError(w, 500, err)
		return
	}
	d.writeSymlink(req.CloneMountpoint)
	writeJSON(w, map[string]string{"status": "ok", "mountpoint": req.CloneMountpoint})
}
