package daemon

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/semistrict/loophole"
)

const zygoteVolume = "zygote-ubuntu-2404"

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
		writeError(w, 400, fmt.Errorf("volume name is required"))
		return
	}

	ctx := r.Context()

	// If size is specified, create a fresh volume (used for zygote creation).
	if req.Size > 0 {
		slog.Info("create (fresh volume)", "volume", req.Volume, "size", req.Size)
		if req.Type == "" {
			req.Type = string(loophole.DefaultFSType())
		}
		if err := d.backend.Create(ctx, req); err != nil {
			slog.Error("create volume failed", "err", err)
			writeError(w, 500, fmt.Errorf("create: %w", err))
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
		return
	}

	// Otherwise clone from zygote.
	slog.Info("create (clone from zygote)", "volume", req.Volume, "zygote", zygoteVolume)

	// Open the zygote volume (frozen layers need no lease).
	zygote, err := d.backend.VM().OpenVolume(zygoteVolume)
	if err != nil {
		slog.Error("open zygote failed", "err", err)
		writeError(w, 500, fmt.Errorf("open zygote: %w", err))
		return
	}

	// Clone from zygote to the requested name.
	_, err = zygote.Clone(req.Volume)
	if err != nil {
		slog.Error("clone from zygote failed", "err", err)
		writeError(w, 500, fmt.Errorf("clone: %w", err))
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}
