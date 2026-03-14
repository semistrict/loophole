package daemon

import (
	"fmt"
	"net/http"

	"github.com/semistrict/loophole"
)

func (d *Daemon) handleCreate(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	var req loophole.CreateParams
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

func (d *Daemon) handleUnmount(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	var req struct {
		Mountpoint string `json:"mountpoint"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}
	if req.Mountpoint == "" {
		writeError(w, 400, fmt.Errorf("missing mountpoint parameter"))
		return
	}
	volume := d.backend.VolumeAt(req.Mountpoint)
	if volume == "" {
		writeError(w, 400, fmt.Errorf("no volume mounted at %q", req.Mountpoint))
		return
	}
	if err := d.backend.Unmount(r.Context(), req.Mountpoint); err != nil {
		writeError(w, 500, err)
		return
	}
	if d.mountpoint == req.Mountpoint {
		d.removeOwnerLinks()
		d.mountpoint = ""
		d.managedVolume = ""
	}
	writeJSON(w, map[string]string{"status": "ok", "volume": volume, "mountpoint": req.Mountpoint})
}

func (d *Daemon) handleDelete(w http.ResponseWriter, r *http.Request) {
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
