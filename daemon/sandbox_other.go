//go:build !linux

package daemon

import (
	"net/http"
)

func (d *Daemon) handleExec(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "exec not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleReadDir(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "readdir not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleStat(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "stat not supported on this platform", http.StatusNotImplemented)
}
