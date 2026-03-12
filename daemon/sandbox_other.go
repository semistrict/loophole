//go:build !linux

package daemon

import (
	"net/http"
	"os/exec"

	"github.com/semistrict/loophole/fsbackend"
)

func chrootCmd(mountpoint string, backend fsbackend.Service, argv ...string) *exec.Cmd {
	return exec.Command(argv[0], argv[1:]...)
}

func (d *Daemon) handleExec(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "exec not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleReadDir(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "readdir not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleStat(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "stat not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleVMSnapshot(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "VM snapshot not supported on this platform", http.StatusNotImplemented)
}

func (d *Daemon) handleVMRestore(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "VM restore not supported on this platform", http.StatusNotImplemented)
}
