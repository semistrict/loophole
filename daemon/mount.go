package daemon

import (
	"log/slog"
	"net/http"
)

func (d *Daemon) handleMount(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}
	var req struct {
		Volume     string `json:"volume"`
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	mountpoint := req.Mountpoint
	if mountpoint == "" {
		mountpoint = req.Volume
	}
	slog.Info("mount", "volume", req.Volume, "mountpoint", mountpoint)
	if err := d.backend.Mount(r.Context(), req.Volume, mountpoint); err != nil {
		slog.Error("mount failed", "err", err)
		writeError(w, 500, err)
		return
	}

	if req.Mountpoint != "" {
		d.writeSymlink(mountpoint)
	}
	writeJSON(w, map[string]string{"status": "ok", "mountpoint": mountpoint})
}
