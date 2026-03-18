package apiserver

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/semistrict/loophole/storage"
)

// volCmdArgs is passed to every volume command handler.
type volCmdArgs struct {
	vol        *storage.Volume
	mountpoint string
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
	slog.Info("flush", "volume", a.vol.Name())
	if err := a.vol.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return map[string]string{"status": "ok"}, nil
}

// registerVolumeCmds registers volume commands on the server mux.
func registerVolumeCmds(mux *http.ServeMux, d *Server) {
	for _, cmd := range volumeCommands {
		cmd := cmd
		mux.HandleFunc(cmd.method+" "+cmd.path, func(w http.ResponseWriter, r *http.Request) {
			if d.requireBackend(w) {
				return
			}
			vol := d.volume()
			if vol == nil {
				writeError(w, 400, errNoVolume)
				return
			}
			result, err := cmd.handler(volCmdArgs{
				vol:        vol,
				mountpoint: d.currentMountpoint(),
			})
			if err != nil {
				writeError(w, 500, err)
				return
			}
			writeJSON(w, result)
		})
	}
}
