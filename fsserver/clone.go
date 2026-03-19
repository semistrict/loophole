package fsserver

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/volserver"
)

func (d *server) handleClone(w http.ResponseWriter, r *http.Request) {
	var req client.CloneParams
	if err := volserver.ReadJSON(r, &req); err != nil {
		volserver.WriteError(w, 400, err)
		return
	}
	if req.Clone == "" {
		volserver.WriteError(w, 400, fmt.Errorf("missing clone parameter"))
		return
	}

	vol := d.volume()
	if vol == nil {
		volserver.WriteError(w, 400, errNoVolume)
		return
	}

	var err error
	if req.Checkpoint != "" {
		slog.Info("clone from checkpoint", "volume", vol.Name(), "checkpoint", req.Checkpoint, "clone", req.Clone)
		err = vol.CloneFromCheckpoint(r.Context(), req.Checkpoint, req.Clone)
	} else {
		mountpoint := d.currentMountpoint()
		if mountpoint == "" {
			volserver.WriteError(w, 400, fmt.Errorf("no volume mounted"))
			return
		}
		slog.Info("clone", "mountpoint", mountpoint, "clone", req.Clone)
		err = d.backend.Clone(r.Context(), mountpoint, req.Clone)
	}
	if err != nil {
		slog.Error("clone failed", "err", err)
		volserver.WriteError(w, 500, err)
		return
	}
	volserver.WriteJSON(w, map[string]string{"status": "ok", "volume": req.Clone})
}
