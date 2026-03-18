package apiserver

import (
	"fmt"
	"net/http"

	"github.com/semistrict/loophole/storage"
)

func (d *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
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
		"state":  state,
		"s3":     d.inst.URL(),
		"mode":   "fuse",
		"socket": d.socket,
		"cache":  d.dir.Cache(d.inst.ProfileName),
		"log":    d.logPath,
	}
	if d.backend != nil {
		status["volumes"] = d.backend.VM().Volumes()
		status["mounts"] = d.backend.Mounts()
	}
	if name := d.volumeName(); name != "" {
		status["volume"] = name
	}
	if mountpoint := d.currentMountpoint(); mountpoint != "" {
		status["mountpoint"] = mountpoint
	}
	if d.devicePath != "" {
		status["device"] = d.devicePath
	}
	if d.startupErr != "" {
		status["error"] = d.startupErr
	}
	writeJSON(w, status)
}

func (d *Server) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	names, err := storage.ListAllVolumes(r.Context(), d.backend.VM().Store())
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"volumes": names})
}

func (d *Server) handleVolumeInfo(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	name := d.volumeName()
	if name == "" {
		writeError(w, 400, errNoVolume)
		return
	}
	info, err := d.backend.VM().VolumeInfo(r.Context(), name)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, info)
}

func (d *Server) currentMountpoint() string {
	if d.backend != nil {
		if name := d.volumeName(); name != "" {
			return d.backend.MountpointForVolume(name)
		}
	}
	return ""
}

func (d *Server) handleDebugVolume(w http.ResponseWriter, r *http.Request) {
	if d.backend == nil {
		writeError(w, 503, fmt.Errorf("storage not available: %s", d.startupErr))
		return
	}
	vol := d.volume()
	if vol == nil {
		writeError(w, 400, errNoVolume)
		return
	}
	info, ok := storage.DebugInfo(vol)
	if !ok {
		writeError(w, 500, fmt.Errorf("volume %q does not support debug info", vol.Name()))
		return
	}
	writeJSON(w, info)
}
