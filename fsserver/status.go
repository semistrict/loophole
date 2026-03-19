package fsserver

import (
	"github.com/semistrict/loophole/volserver"
	"net/http"
)

func (d *server) handleStatus(w http.ResponseWriter, r *http.Request) {
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
	volserver.WriteJSON(w, status)
}

func (d *server) currentMountpoint() string {
	if d.backend != nil {
		if name := d.volumeName(); name != "" {
			return d.backend.MountpointForVolume(name)
		}
	}
	return ""
}
