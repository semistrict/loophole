package fsserver

import (
	"log/slog"
	"net/http"

	"github.com/semistrict/loophole/internal/volserver"
)

// handleCheckpoint freezes the filesystem, creates a checkpoint, and thaws.
func (d *server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	mountpoint := d.currentMountpoint()
	if mountpoint == "" {
		volserver.WriteError(w, 400, errNoVolume)
		return
	}
	slog.Info("checkpoint", "mountpoint", mountpoint)
	cpID, err := d.backend.Checkpoint(r.Context(), mountpoint)
	if err != nil {
		slog.Error("checkpoint failed", "err", err)
		volserver.WriteError(w, 500, err)
		return
	}
	volserver.WriteJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}
