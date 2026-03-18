package daemon

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

// volCmdArgs is passed to every volume command handler.
type volCmdArgs struct {
	vol        loophole.Volume
	volName    string
	mountpoint string
	backend    *fsbackend.Backend
}

// volCmd defines a per-volume command.
type volCmd struct {
	method  string // "POST" or "GET"
	path    string // e.g. "/flush"
	handler func(a volCmdArgs) (any, error)
}

// volumeCommands is the registry of per-volume commands.
var volumeCommands = []volCmd{
	{method: "POST", path: "/flush", handler: cmdFlush},
}

func cmdFlush(a volCmdArgs) (any, error) {
	slog.Info("flush", "volume", a.volName)
	if err := a.vol.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return map[string]string{"status": "ok"}, nil
}

// registerVolumeCmds registers volume commands on the daemon mux.
// Accepts explicit volume/mountpoint in the request body; falls back to
// the daemon's managed volume when omitted.
func registerVolumeCmds(mux *http.ServeMux, d *Daemon) {
	for _, cmd := range volumeCommands {
		cmd := cmd
		mux.HandleFunc(cmd.method+" "+cmd.path, func(w http.ResponseWriter, r *http.Request) {
			if d.requireBackend(w) {
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

			volName := req.Volume
			mp := req.Mountpoint
			if volName == "" {
				volName = d.managedVolume
			}
			if mp == "" {
				mp = d.currentMountpoint()
			}
			if volName == "" {
				writeError(w, 400, errNoVolume)
				return
			}
			vol := d.backend.VM().GetVolume(volName)
			if vol == nil {
				writeError(w, 500, fmt.Errorf("volume %s not open", volName))
				return
			}
			result, err := cmd.handler(volCmdArgs{
				vol: vol, volName: volName, mountpoint: mp,
				backend: d.backend,
			})
			if err != nil {
				writeError(w, 500, err)
				return
			}
			writeJSON(w, result)
		})
	}
}
