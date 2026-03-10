//go:build !nosqlite

package daemon

import (
	"log/slog"
	"net/http"

	loophole "github.com/semistrict/loophole"
	"github.com/semistrict/loophole/sqlitevfs"
)

func (d *Daemon) registerDBRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /db/create", d.handleDBCreate)
	mux.HandleFunc("POST /db/snapshot", d.handleDBSnapshot)
	mux.HandleFunc("POST /db/branch", d.handleDBBranch)
	mux.HandleFunc("POST /db/flush", d.handleDBFlush)
	mux.HandleFunc("GET /db/ls", d.handleDBList)
}

func (d *Daemon) handleDBCreate(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
		Size   uint64 `json:"size,omitempty"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("db/create", "volume", req.Volume, "size", req.Size)

	var opts []sqlitevfs.Option
	if req.Size > 0 {
		opts = append(opts, sqlitevfs.WithSize(req.Size))
	}

	db, err := sqlitevfs.Create(r.Context(), d.backend.VM(), req.Volume, opts...)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	if err := db.Close(r.Context()); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDBSnapshot(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume   string `json:"volume"`
		Snapshot string `json:"snapshot"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("db/snapshot", "volume", req.Volume, "snapshot", req.Snapshot)

	db, err := sqlitevfs.Open(r.Context(), d.backend.VM(), req.Volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	defer func() { _ = db.Close(r.Context()) }()

	if err := db.Snapshot(r.Context(), req.Snapshot); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDBBranch(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
		Branch string `json:"branch"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("db/branch", "volume", req.Volume, "branch", req.Branch)

	db, err := sqlitevfs.Open(r.Context(), d.backend.VM(), req.Volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	defer func() { _ = db.Close(r.Context()) }()

	branch, err := db.Branch(r.Context(), req.Branch)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	if err := branch.Close(r.Context()); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDBFlush(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	var req struct {
		Volume string `json:"volume"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, 400, err)
		return
	}

	slog.Info("db/flush", "volume", req.Volume)

	db, err := sqlitevfs.Open(r.Context(), d.backend.VM(), req.Volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	defer func() { _ = db.Close(r.Context()) }()

	if err := db.Flush(r.Context()); err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (d *Daemon) handleDBList(w http.ResponseWriter, r *http.Request) {
	if d.requireBackend(w) {
		return
	}
	volumes, err := d.backend.VM().ListVolumesByType(r.Context(), loophole.VolumeTypeSQLite)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, map[string]any{"volumes": volumes})
}
