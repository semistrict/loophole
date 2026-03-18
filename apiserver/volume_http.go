package apiserver

import (
	"fmt"
	"net/http"

	"github.com/semistrict/loophole/storage"
)

func (d *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	var req storage.CreateParams
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	if req.Volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}
	if err := d.backend.Create(r.Context(), req); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Volume})
}

func (d *Server) handleMount(w http.ResponseWriter, r *http.Request) {
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
	if req.Volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}
	mountpoint, err := d.MountVolume(r.Context(), req.Volume, req.Mountpoint)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Volume, "mountpoint": mountpoint})
}

func (d *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	if req.Volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}

	if mountpoint := d.backend.MountpointForVolume(req.Volume); mountpoint != "" {
		if err := d.backend.Unmount(r.Context(), mountpoint); err != nil {
			writeError(w, 500, fmt.Errorf("unmount %q before delete: %w", mountpoint, err))
			return
		}
	}

	if err := d.backend.VM().DeleteVolume(r.Context(), req.Volume); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": req.Volume})
}
