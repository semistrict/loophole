package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/storage2"
)

// volCmdArgs is passed to every volume command handler.
type volCmdArgs struct {
	vol        loophole.Volume
	volName    string
	mountpoint string
	backend    *fsbackend.Backend
	body       json.RawMessage
}

// volCmd defines a per-volume command. It is registered once and automatically
// available on both the main daemon mux and the chroot socket.
type volCmd struct {
	method     string // "POST" or "GET"
	path       string // e.g. "/flush", "/compact"
	handler    func(a volCmdArgs) (any, error)
	chrootOnly bool // if true, only register on chroot socket (path conflicts with daemon-level handler)
}

// volumeCommands is the registry of per-volume commands.
var volumeCommands = []volCmd{
	{method: "GET", path: "/status", handler: cmdStatus, chrootOnly: true},
	{method: "POST", path: "/flush", handler: cmdFlush},
	{method: "POST", path: "/compact", handler: cmdCompact},
	{method: "POST", path: "/checkpoint", handler: cmdCheckpoint, chrootOnly: true},
	{method: "POST", path: "/clone", handler: cmdClone, chrootOnly: true},
}

func cmdStatus(a volCmdArgs) (any, error) {
	if info, ok := storage2.DebugInfo(a.vol); ok {
		return info, nil
	}
	return map[string]string{"status": "ok", "volume": a.volName}, nil
}

func cmdFlush(a volCmdArgs) (any, error) {
	slog.Info("flush", "volume", a.volName)
	if err := a.vol.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return map[string]string{"status": "ok"}, nil
}

func cmdCompact(a volCmdArgs) (any, error) {
	type compactor interface {
		CompactL0() error
	}
	c, ok := a.vol.(compactor)
	if !ok {
		return nil, fmt.Errorf("volume %q does not support compaction", a.volName)
	}
	slog.Info("compact: starting", "volume", a.volName)
	if err := c.CompactL0(); err != nil {
		return nil, fmt.Errorf("compact: %w", err)
	}
	slog.Info("compact: done", "volume", a.volName)
	return map[string]string{"status": "ok"}, nil
}

func cmdCheckpoint(a volCmdArgs) (any, error) {
	if a.mountpoint == "" {
		return nil, fmt.Errorf("mountpoint is required for checkpoint")
	}
	slog.Info("checkpoint", "mountpoint", a.mountpoint)
	cpID, err := a.backend.Checkpoint(context.Background(), a.mountpoint)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	return map[string]string{"status": "ok", "checkpoint": cpID}, nil
}

func cmdClone(a volCmdArgs) (any, error) {
	var req struct {
		Clone           string `json:"clone"`
		CloneMountpoint string `json:"clone_mountpoint"`
	}
	if err := json.Unmarshal(a.body, &req); err != nil {
		return nil, err
	}
	if req.Clone == "" {
		return nil, fmt.Errorf("clone name is required")
	}
	if a.mountpoint == "" {
		return nil, fmt.Errorf("mountpoint is required for clone")
	}
	cloneMP := req.CloneMountpoint
	if cloneMP == "" {
		// Chroot case: auto-create a temp mountpoint.
		var err error
		cloneMP, err = os.MkdirTemp("", "loophole-clone-")
		if err != nil {
			return nil, fmt.Errorf("create clone mountpoint: %w", err)
		}
	}
	slog.Info("clone", "mountpoint", a.mountpoint, "clone", req.Clone, "clone_mountpoint", cloneMP)
	if err := a.backend.Clone(context.Background(), a.mountpoint, req.Clone, cloneMP); err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}
	return map[string]string{"status": "ok", "volume": req.Clone, "mountpoint": cloneMP}, nil
}

// resolveVolume finds the volume from either a volume name or mountpoint.
func resolveVolume(backend *fsbackend.Backend, volume, mountpoint string) (volName, mp string, vol loophole.Volume, err error) {
	if mountpoint != "" {
		volName = backend.VolumeAt(mountpoint)
		if volName == "" {
			return "", "", nil, fmt.Errorf("no volume at mountpoint %s", mountpoint)
		}
		mp = mountpoint
	} else if volume != "" {
		volName = volume
		mp = backend.MountpointForVolume(volume)
	} else {
		return "", "", nil, fmt.Errorf("volume or mountpoint is required")
	}
	vol = backend.VM().GetVolume(volName)
	if vol == nil {
		return "", "", nil, fmt.Errorf("volume %s not open", volName)
	}
	return volName, mp, vol, nil
}

// registerVolumeCmds registers non-chrootOnly volume commands on the main daemon mux.
func registerVolumeCmds(mux *http.ServeMux, d *Daemon) {
	for _, cmd := range volumeCommands {
		if cmd.chrootOnly {
			continue
		}
		cmd := cmd
		mux.HandleFunc(cmd.method+" "+cmd.path, func(w http.ResponseWriter, r *http.Request) {
			if d.requireBackend(w) {
				return
			}
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Volume     string `json:"volume"`
				Mountpoint string `json:"mountpoint"`
			}
			_ = json.Unmarshal(body, &req)

			volName, mp, vol, err := resolveVolume(d.backend, req.Volume, req.Mountpoint)
			if err != nil {
				writeError(w, 400, err)
				return
			}
			result, err := cmd.handler(volCmdArgs{
				vol: vol, volName: volName, mountpoint: mp,
				backend: d.backend, body: json.RawMessage(body),
			})
			if err != nil {
				writeError(w, 500, err)
				return
			}
			writeJSON(w, result)
		})
	}
}
