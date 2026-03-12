//go:build linux

package daemon

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"time"
)

// dirEntry is the JSON representation of a filesystem directory entry.
type dirEntry struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	IsDir   bool        `json:"isDir"`
	Mode    fs.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
}

func statToEntry(name string, fi fs.FileInfo) dirEntry {
	return dirEntry{
		Name:    name,
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
	}
}

func (d *Daemon) handleExec(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) {
		return
	}
	volume := r.URL.Query().Get("volume")
	cmdStr := r.URL.Query().Get("cmd")
	if cmdStr == "" {
		writeError(w, 400, fmt.Errorf("missing cmd parameter"))
		return
	}

	result, err := d.sandboxRuntime.Exec(r.Context(), volume, cmdStr)
	if err != nil {
		writeError(w, 500, err)
		return
	}
	writeJSON(w, result)
}

func (d *Daemon) handleReadDir(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}
	volume := r.URL.Query().Get("volume")
	dir := r.URL.Query().Get("path")
	if volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}
	if dir == "" {
		dir = "/"
	}

	fsys, err := d.backend.FSForVolume(r.Context(), volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}

	names, err := fsys.ReadDir(dir)
	if err != nil {
		writeError(w, 500, err)
		return
	}

	entries := make([]dirEntry, 0, len(names))
	for _, name := range names {
		fi, err := fsys.Stat(path.Join(dir, name))
		if err != nil {
			// Skip entries we can't stat (e.g. broken symlinks).
			continue
		}
		entries = append(entries, statToEntry(name, fi))
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(entries); err != nil {
		slog.Warn("readdir encode error", "error", err)
	}
}

func (d *Daemon) handleStat(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}
	volume := r.URL.Query().Get("volume")
	p := r.URL.Query().Get("path")
	if volume == "" || p == "" {
		writeError(w, 400, fmt.Errorf("missing volume or path parameter"))
		return
	}

	fsys, err := d.backend.FSForVolume(r.Context(), volume)
	if err != nil {
		writeError(w, 500, err)
		return
	}

	fi, err := fsys.Stat(p)
	if err != nil {
		writeError(w, 404, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(statToEntry(path.Base(p), fi)); err != nil {
		slog.Warn("stat encode error", "error", err)
	}
}
