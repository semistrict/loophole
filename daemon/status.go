package daemon

import (
	"fmt"
	"net/http"

	"github.com/semistrict/loophole/storage2"
)

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	state := "running"
	if d.shuttingDown() {
		select {
		case <-d.doneCh:
			state = "stopped"
		default:
			state = "shutting_down"
		}
	}
	status := map[string]any{
		"state":         state,
		"s3":            d.inst.URL(),
		"mode":          "fuse",
		"socket":        d.socket,
		"cache":         d.dir.Cache(d.inst.ProfileName),
		"log":           d.dir.Log(d.inst.ProfileName),
		"sandbox_debug": d.sandboxDebugInfo(),
	}
	if d.backend != nil {
		status["volumes"] = d.backend.VM().Volumes()
		status["mounts"] = d.backend.Mounts()
	}
	if d.managedVolume != "" {
		status["volume"] = d.managedVolume
	}
	if d.mountpoint != "" {
		status["mountpoint"] = d.mountpoint
	}
	if d.devicePath != "" {
		status["device"] = d.devicePath
	}
	if d.startupErr != "" {
		status["error"] = d.startupErr
	}
	writeJSON(w, status)
}

func (d *Daemon) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	names, err := d.backend.VM().ListAllVolumes(r.Context())
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"volumes": names})
}

func (d *Daemon) handleVolumeInfo(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	name := r.URL.Query().Get("volume")
	if name == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}
	info, err := d.backend.VM().VolumeInfo(r.Context(), name)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, info)
}

func (d *Daemon) handleDebugVolume(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	name := r.URL.Query().Get("volume")
	if name == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}
	vol := d.backend.VM().GetVolume(name)
	if vol == nil {
		writeError(w, 404, fmt.Errorf("volume %q not open", name))
		return
	}
	info, ok := storage2.DebugInfo(vol)
	if !ok {
		writeError(w, 500, fmt.Errorf("volume %q does not support debug info", name))
		return
	}
	writeJSON(w, info)
}
