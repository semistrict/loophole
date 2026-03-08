package daemon

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sync"

	"github.com/creack/pty/v2"
	"github.com/gorilla/websocket"
)

// shellResize is sent by the client as a JSON text message to resize the PTY.
type shellResize struct {
	Type string `json:"type"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

func (d *Daemon) handleShell(w http.ResponseWriter, r *http.Request) {
	volume := r.URL.Query().Get("volume")
	if volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}

	// Auto-mount the volume and get its FS (we need the mountpoint for CWD).
	fsys, err := d.backend.FSForVolume(r.Context(), volume)
	if err != nil {
		writeError(w, 500, fmt.Errorf("mount volume %s: %w", volume, err))
		return
	}
	_ = fsys // We use the mountpoint path below, not the FS interface directly.

	// Determine the mountpoint directory for this volume.
	mountpoint := ""
	for mp, vol := range d.backend.Mounts() {
		if vol == volume {
			mountpoint = mp
			break
		}
	}
	if mountpoint == "" {
		writeError(w, 500, fmt.Errorf("volume %s not mounted", volume))
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		d.log.Error("shell websocket upgrade failed", "err", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Spawn a shell chrooted into the volume's mountpoint.
	cmd := exec.Command("/usr/sbin/chroot", mountpoint, "/bin/sh", "-l")
	cmd.Dir = "/"
	cmd.Env = []string{
		"TERM=xterm-256color",
		"HOME=/root",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		d.log.Error("pty start failed", "err", err)
		return
	}
	defer func() {
		_ = ptmx.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	d.log.Info("shell started", "volume", volume, "mountpoint", mountpoint, "pid", cmd.Process.Pid)

	var wg sync.WaitGroup

	// PTY → WebSocket (binary messages)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				if writeErr := conn.WriteMessage(websocket.BinaryMessage, buf[:n]); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// WebSocket → PTY
	// Text messages: JSON control (resize)
	// Binary messages: terminal input
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = ptmx.Close() }() // unblock pty read on close

		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			switch msgType {
			case websocket.BinaryMessage:
				if _, err := ptmx.Write(msg); err != nil {
					return
				}
			case websocket.TextMessage:
				var resize shellResize
				if err := json.Unmarshal(msg, &resize); err != nil {
					continue
				}
				if resize.Type == "resize" {
					_ = pty.Setsize(ptmx, &pty.Winsize{
						Cols: resize.Cols,
						Rows: resize.Rows,
					})
				}
			}
		}
	}()

	// Wait for the shell process to exit.
	_ = cmd.Wait()

	// Send remaining output.
	wg.Wait()

	d.log.Info("shell exited", "volume", volume, "pid", cmd.Process.Pid)

}
