//go:build linux

package daemon

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole/fsbackend"
)

// startChrootSocket creates a restricted Unix socket that is bind-mounted into
// the chroot at /.loophole. It serves only flush, snapshot, and clone
// operations, scoped to the volume mounted at the given mountpoint.
//
// The socket and bind-mount target both live on the host filesystem (tmpdir),
// never on the ext4 volume. This ensures cleanup doesn't touch the volume,
// which may be frozen (FIFREEZE) at teardown time.
func startChrootSocket(mountpoint string, backend fsbackend.Service) (cleanup func(), err error) {
	// Host-side socket in a temp directory.
	hostSock := filepath.Join(os.TempDir(), "loophole-chroot-"+filepath.Base(mountpoint)+".sock")
	_ = os.Remove(hostSock)

	ln, err := net.Listen("unix", hostSock)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", hostSock, err)
	}
	if err := os.Chmod(hostSock, 0o666); err != nil {
		_ = ln.Close()
		_ = os.Remove(hostSock)
		return nil, fmt.Errorf("chmod %s: %w", hostSock, err)
	}

	// Bind-mount the host socket into the chroot at /.loophole.
	// We create an empty file as the mount target — this is a directory entry
	// on the ext4 volume but we never remove it (cleanup only detaches the
	// bind mount via MNT_DETACH, which doesn't touch the filesystem).
	chrootTarget := filepath.Join(mountpoint, ".loophole")
	if f, err := os.Create(chrootTarget); err == nil {
		_ = f.Close()
	}
	if err := unix.Mount(hostSock, chrootTarget, "", unix.MS_BIND, ""); err != nil {
		slog.Warn("chroot socket bind mount failed", "error", err)
	}

	mux := http.NewServeMux()
	registerChrootVolumeCmds(mux, mountpoint, backend)

	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("chroot socket server error", "mountpoint", mountpoint, "error", err)
		}
	}()

	slog.Info("chroot socket started", "host", hostSock, "chroot", chrootTarget, "volume", backend.VolumeAt(mountpoint))

	cleanup = func() {
		_ = srv.Close()
		_ = unix.Unmount(chrootTarget, unix.MNT_DETACH)
		_ = os.Remove(hostSock)
	}
	return cleanup, nil
}

// registerChrootVolumeCmds registers all volume commands on a chroot socket mux.
func registerChrootVolumeCmds(mux *http.ServeMux, mountpoint string, backend fsbackend.Service) {
	for _, cmd := range volumeCommands {
		cmd := cmd
		mux.HandleFunc(cmd.method+" "+cmd.path, func(w http.ResponseWriter, r *http.Request) {
			volName := backend.VolumeAt(mountpoint)
			if volName == "" {
				writeError(w, 500, fmt.Errorf("no volume at %s", mountpoint))
				return
			}
			vol := backend.VM().GetVolume(volName)
			if vol == nil {
				writeError(w, 500, fmt.Errorf("volume %s not open", volName))
				return
			}
			body, _ := io.ReadAll(r.Body)
			result, err := cmd.handler(volCmdArgs{
				vol: vol, volName: volName, mountpoint: mountpoint,
				backend: backend, body: json.RawMessage(body),
			})
			if err != nil {
				writeError(w, 500, err)
				return
			}
			writeJSON(w, result)
		})
	}
}
