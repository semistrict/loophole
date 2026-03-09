package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/semistrict/loophole/fsbackend"
)

// preparedChroots tracks which mountpoints have had their chroot
// environment set up (bind mounts for /proc, /sys, /dev, etc.).
var preparedChroots sync.Map // mountpoint → true

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

// chrootEnv is the environment used for chrooted commands.
var chrootEnv = []string{
	"TERM=xterm-256color",
	"HOME=/root",
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

// prepareChrootEnv sets up bind mounts and config files inside a chroot
// mountpoint. This is done once per mountpoint and cached. A before_unmount
// callback is registered on the backend to tear down bind mounts automatically.
func prepareChrootEnv(mountpoint string, backend fsbackend.Service) {
	if _, loaded := preparedChroots.LoadOrStore(mountpoint, true); loaded {
		return
	}

	// Bind mounts: /proc, /sys, /dev, /dev/pts
	binds := []struct{ src, dst, fstype, opts string }{
		{src: "proc", dst: "proc", fstype: "proc"},
		{src: "/sys", dst: "sys"},
		{src: "/dev", dst: "dev", opts: "rbind"},
		{src: "devpts", dst: "dev/pts", fstype: "devpts", opts: "newinstance,ptmxmode=0666"},
	}
	for _, b := range binds {
		dst := filepath.Join(mountpoint, b.dst)
		_ = os.MkdirAll(dst, 0o755)
		args := []string{"-n"}
		if b.fstype != "" {
			args = append(args, "-t", b.fstype)
		} else {
			args = append(args, "--bind")
		}
		if b.opts != "" {
			args = append(args, "-o", b.opts)
		}
		args = append(args, b.src, dst)
		if out, err := exec.Command("mount", args...).CombinedOutput(); err != nil {
			slog.Warn("chroot mount failed", "dst", dst, "error", err, "output", string(out))
			// Don't block on failure — best effort.
		}
	}

	// Copy /etc/resolv.conf from host so DNS works.
	if data, err := os.ReadFile("/etc/resolv.conf"); err == nil {
		dst := filepath.Join(mountpoint, "etc", "resolv.conf")
		_ = os.WriteFile(dst, data, 0o644)
	}

	// Bind-mount the loophole binary into the chroot at /usr/bin/loophole.
	if self, err := os.Executable(); err == nil {
		dst := filepath.Join(mountpoint, "usr", "bin", "loophole")
		_ = os.MkdirAll(filepath.Dir(dst), 0o755)
		if f, err := os.Create(dst); err == nil {
			_ = f.Close()
		}
		if out, err := exec.Command("mount", "--bind", self, dst).CombinedOutput(); err != nil {
			slog.Warn("chroot loophole binary bind mount failed", "error", err, "output", string(out))
		}
	}

	// Start restricted API socket inside the chroot at /.loophole.
	sockCleanup, err := startChrootSocket(mountpoint, backend)
	if err != nil {
		slog.Warn("chroot socket start failed", "mountpoint", mountpoint, "error", err)
	}

	// Register teardown as a mount-level callback — fires before FS unmount.
	backend.OnBeforeUnmount(mountpoint, func() {
		preparedChroots.Delete(mountpoint)
		if sockCleanup != nil {
			sockCleanup()
		}
		for _, sub := range []string{"usr/bin/loophole", "dev/pts", "dev", "sys", "proc"} {
			dst := filepath.Join(mountpoint, sub)
			if out, err := exec.Command("umount", "-n", "-l", dst).CombinedOutput(); err != nil {
				slog.Warn("chroot umount failed", "dst", dst, "error", err, "output", string(out))
			}
		}
	})
}

// chrootCmd builds an exec.Cmd that runs argv inside a chroot at mountpoint.
// It ensures the chroot environment (proc, sys, dev, resolv.conf) is set up.
func chrootCmd(mountpoint string, backend fsbackend.Service, argv ...string) *exec.Cmd {
	prepareChrootEnv(mountpoint, backend)
	args := append([]string{mountpoint}, argv...)
	cmd := exec.Command("/usr/sbin/chroot", args...)
	cmd.Dir = "/"
	cmd.Env = chrootEnv
	return cmd
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

	// If a volume is specified, chroot into its mountpoint (matching the
	// xterm.js shell behaviour). Otherwise run on the host.
	var cmd *exec.Cmd
	if volume != "" && d.backend != nil {
		if _, err := d.backend.FSForVolume(r.Context(), volume); err == nil {
			for mp, volName := range d.backend.Mounts() {
				if volName == volume {
					cmd = chrootCmd(mp, d.backend, "sh", "-c", cmdStr)
					break
				}
			}
		}
	}
	if cmd == nil {
		cmd = exec.CommandContext(r.Context(), "sh", "-c", cmdStr)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			writeError(w, 500, err)
			return
		}
	}

	writeJSON(w, map[string]any{
		"exitCode": exitCode,
		"stdout":   stdout.String(),
		"stderr":   stderr.String(),
	})
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
		d.log.Warn("readdir encode error", "error", err)
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
		d.log.Warn("stat encode error", "error", err)
	}
}
