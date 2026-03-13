//go:build linux

package sandboxd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty/v2"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/internal/util"
)

const sandboxdGuestBin = "/.loophole/bin/loophole-sandboxd"

var defaultSandboxCapabilities = []string{
	"CAP_AUDIT_WRITE",
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_FOWNER",
	"CAP_FSETID",
	"CAP_KILL",
	"CAP_MKNOD",
	"CAP_NET_BIND_SERVICE",
	"CAP_NET_RAW",
	"CAP_SETFCAP",
	"CAP_SETGID",
	"CAP_SETPCAP",
	"CAP_SETUID",
	"CAP_SYS_CHROOT",
}

func (d *Daemon) sandboxDir(id string) string {
	return filepath.Join(d.dir.SandboxdState(), "sandboxes", id)
}

func (d *Daemon) bundleDir(id string) string {
	return filepath.Join(d.sandboxDir(id), "bundle")
}

func (d *Daemon) mountDir(id string) string {
	return filepath.Join(d.sandboxDir(id), "rootfs")
}

func (d *Daemon) runscDebugDir(id string) string {
	return filepath.Join(d.sandboxDir(id), "runsc-debug")
}

func (d *Daemon) runscPanicLogPath(id string) string {
	return filepath.Join(d.sandboxDir(id), "runsc-panic.log")
}

func (d *Daemon) buildBundle(sb SandboxRecord) error {
	bundleDir := d.bundleDir(sb.ID)
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return err
	}
	if err := d.installGuestBinary(sb.Mountpoint); err != nil {
		return err
	}

	mounts := []map[string]any{
		{"destination": "/proc", "type": "proc", "source": "proc"},
		{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
		{"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": []string{"nosuid", "noexec", "nodev"}},
		{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev", "mode=1777", "size=65536k"}},
	}
	for _, path := range []string{"/etc/resolv.conf", "/etc/hosts"} {
		if _, err := os.Stat(path); err == nil {
			mounts = append(mounts, map[string]any{
				"destination": path,
				"type":        "bind",
				"source":      path,
				"options":     []string{"rbind", "ro"},
			})
		}
	}

	spec := map[string]any{
		"ociVersion": "1.0.2",
		"process": map[string]any{
			"terminal": false,
			"user": map[string]any{
				"uid": 0,
				"gid": 0,
			},
			"args": []string{sandboxdGuestBin, "internal.init"},
			"env": []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"HOME=/root",
			},
			"cwd":             "/",
			"noNewPrivileges": true,
			"capabilities": map[string]any{
				"bounding":    defaultSandboxCapabilities,
				"effective":   defaultSandboxCapabilities,
				"inheritable": []string{},
				"permitted":   defaultSandboxCapabilities,
			},
			"rlimits": []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1048576, "soft": 1048576}},
		},
		"root": map[string]any{
			"path":     sb.Mountpoint,
			"readonly": false,
		},
		"hostname": sb.Name,
		"mounts":   mounts,
		"linux": map[string]any{
			"namespaces": []map[string]any{
				{"type": "pid"},
				{"type": "ipc"},
				{"type": "uts"},
				{"type": "mount"},
			},
			"maskedPaths": []string{
				"/proc/kcore", "/proc/latency_stats", "/proc/timer_list", "/proc/sched_debug",
				"/sys/firmware", "/proc/scsi",
			},
			"readonlyPaths": []string{
				"/proc/asound", "/proc/bus", "/proc/fs", "/proc/irq",
				"/proc/sys", "/proc/sysrq-trigger",
			},
		},
	}
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0o644)
}

func (d *Daemon) installGuestBinary(rootfs string) error {
	dst := filepath.Join(rootfs, strings.TrimPrefix(filepath.Clean(sandboxdGuestBin), "/"))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	srcFile, err := os.Open(d.selfBin)
	if err != nil {
		return err
	}
	defer util.SafeClose(srcFile, "close sandboxd self binary")

	tmp := dst + ".tmp"
	dstFile, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		util.SafeClose(dstFile, "close staged guest binary after copy failure")
		return err
	}
	if err := dstFile.Sync(); err != nil {
		util.SafeClose(dstFile, "close staged guest binary after sync failure")
		return err
	}
	util.SafeClose(dstFile, "close staged guest binary")
	return os.Rename(tmp, dst)
}

func (d *Daemon) runscArgs(sandboxID string, args ...string) []string {
	base := []string{
		"--root=" + d.runscRoot,
		"--network=" + networkModeHost,
		"--ignore-cgroups=true",
		"--file-access=shared",
		"--overlay2=none",
	}
	if d.runscDebug && sandboxID != "" {
		base = append(base,
			"--debug",
			"--debug-log="+d.runscDebugDir(sandboxID)+"/",
			"--panic-log="+d.runscPanicLogPath(sandboxID),
		)
	}
	base = append(base, args...)
	return base
}

func (d *Daemon) runscCmd(ctx context.Context, sandboxID string, args ...string) *exec.Cmd {
	base := d.runscArgs(sandboxID, args...)
	return exec.CommandContext(ctx, d.runscBin, base...)
}

const networkModeHost = "host"

func (d *Daemon) startRunsc(ctx context.Context, sb SandboxRecord) error {
	if err := d.buildBundle(sb); err != nil {
		return err
	}
	if d.runscDebug {
		if err := os.MkdirAll(d.runscDebugDir(sb.ID), 0o755); err != nil {
			return fmt.Errorf("create runsc debug dir: %w", err)
		}
	}
	logFile, err := os.CreateTemp(d.sandboxDir(sb.ID), "runsc-run-*.log")
	if err != nil {
		return err
	}
	logPath := logFile.Name()
	defer func() {
		util.SafeClose(logFile, "close runsc start log")
		_ = os.Remove(logPath)
	}()
	cmd := d.runscCmd(ctx, sb.ID, "run", "--bundle="+d.bundleDir(sb.ID), "--detach=true", sb.RunscID)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		out, readErr := os.ReadFile(logPath)
		if readErr != nil {
			return fmt.Errorf("runsc run: %w (read log: %v)", err, readErr)
		}
		return fmt.Errorf("runsc run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := d.waitRunscReady(ctx, sb); err != nil {
		return err
	}
	return nil
}

func (d *Daemon) waitRunscReady(ctx context.Context, sb SandboxRecord) error {
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var lastErr error
	for readyCtx.Err() == nil {
		cmd := d.runscCmd(readyCtx, sb.ID, "exec", sb.RunscID, sandboxdGuestBin, "internal.ready")
		out, err := cmd.CombinedOutput()
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("runsc ready probe: %w: %s", err, strings.TrimSpace(string(out)))
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return lastErr
	}
	return readyCtx.Err()
}

func (d *Daemon) runscState(ctx context.Context, sb SandboxRecord) error {
	cmd := d.runscCmd(ctx, sb.ID, "state", sb.RunscID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("runsc state: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (d *Daemon) stopRunsc(ctx context.Context, sb SandboxRecord) error {
	killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer killCancel()
	kill := d.runscCmd(killCtx, sb.ID, "kill", sb.RunscID, "TERM")
	if out, err := kill.CombinedOutput(); err != nil {
		output := strings.TrimSpace(string(out))
		if !isMissingControlSocket(err, output) {
			slog.Warn("runsc kill failed", "runsc_id", sb.RunscID, "error", err, "output", output)
		}
	}
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer deleteCancel()
	deleteCmd := d.runscCmd(deleteCtx, sb.ID, "delete", "--force", sb.RunscID)
	if out, err := deleteCmd.CombinedOutput(); err != nil {
		output := strings.TrimSpace(string(out))
		return fmt.Errorf("runsc delete: %w: %s", err, output)
	}
	return nil
}

func isMissingControlSocket(err error, output string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(output)
	return strings.Contains(msg, "no control socket found")
}

func (d *Daemon) execOneShot(ctx context.Context, sb SandboxRecord, req ProcessCreateRequest) (ExecResult, error) {
	args, err := internalExecArgs(req)
	if err != nil {
		return ExecResult{}, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		cmd := d.runscCmd(ctx, sb.ID, append([]string{"exec", sb.RunscID}, args...)...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		exitCode := 0
		if err := cmd.Run(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				return ExecResult{}, err
			}
		}
		result := ExecResult{
			ExitCode: exitCode,
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
		}
		if !shouldRetryExecResult(result) || attempt == 2 {
			return result, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ExecResult{}, fmt.Errorf("unreachable")
}

func shouldRetryExecResult(result ExecResult) bool {
	if result.ExitCode == 0 {
		return false
	}
	msg := strings.ToLower(result.Stderr)
	return strings.Contains(msg, "input/output error") || strings.Contains(msg, "timed out waiting for executable")
}

func (d *Daemon) startSession(ctx context.Context, processID string, sb SandboxRecord, req ProcessCreateRequest) (*execSession, error) {
	args, err := internalExecArgs(req)
	if err != nil {
		return nil, err
	}
	_ = ctx
	cmd := d.runscCmd(context.Background(), sb.ID, append([]string{"exec", sb.RunscID}, args...)...)
	events := make(chan processEvent, 64)
	session := &execSession{
		id:     processID,
		tty:    req.TTY,
		events: events,
	}
	var readers sync.WaitGroup
	if req.TTY {
		var ptmx *os.File
		if req.Rows > 0 && req.Cols > 0 {
			ptmx, err = pty.StartWithSize(cmd, &pty.Winsize{
				Rows: uint16(req.Rows),
				Cols: uint16(req.Cols),
			})
		} else {
			ptmx, err = pty.Start(cmd)
		}
		if err != nil {
			return nil, err
		}
		session.proc = cmd.Process
		session.stdin = ptmx
		session.pty = ptmx
		session.close = func() error {
			return ptmx.Close()
		}
		readers.Add(1)
		go streamPTY(events, &readers, ptmx)
		go waitSession(recordUpdater(d, session.id), cmd, &readers, events)
		return session, nil
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	session.proc = cmd.Process
	session.stdin = stdin
	session.close = func() error {
		return stdin.Close()
	}
	readers.Add(2)
	go streamReader(events, &readers, "stdout", stdout)
	go streamReader(events, &readers, "stderr", stderr)
	go waitSession(recordUpdater(d, session.id), cmd, &readers, events)
	return session, nil
}

func recordUpdater(d *Daemon, sessionID string) func(processEvent) {
	return func(event processEvent) {
		now := time.Now().UTC()
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.sessions[sessionID]
		if s == nil {
			return
		}
		if event.Stream == "exit" {
			code, _ := strconv.Atoi(string(event.Data))
			s.record.State = StateStopped
			s.record.ExitCode = &code
		} else {
			s.record.State = StateBroken
		}
		s.record.StoppedAt = &now
		d.sessions[sessionID] = s
	}
}

func waitSession(updateRecord func(processEvent), cmd *exec.Cmd, readers *sync.WaitGroup, events chan<- processEvent) {
	err := cmd.Wait()
	readers.Wait()
	var event processEvent
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			event = processEvent{Stream: "exit", Data: []byte(fmt.Sprintf("%d", exitErr.ExitCode()))}
		} else {
			event = processEvent{Stream: "error", Error: err.Error()}
		}
	} else {
		event = processEvent{Stream: "exit", Data: []byte("0")}
	}
	updateRecord(event)
	events <- event
	close(events)
}

func streamReader(events chan<- processEvent, readers *sync.WaitGroup, stream string, r io.Reader) {
	defer readers.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			events <- processEvent{Stream: stream, Data: chunk}
		}
		if err != nil {
			if err != io.EOF {
				events <- processEvent{Stream: "error", Error: err.Error()}
			}
			return
		}
	}
}

func streamPTY(events chan<- processEvent, readers *sync.WaitGroup, f *os.File) {
	defer readers.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			events <- processEvent{Stream: "stdout", Data: chunk}
		}
		if err != nil {
			if err != io.EOF && !errors.Is(err, syscall.EIO) {
				events <- processEvent{Stream: "error", Error: err.Error()}
			}
			return
		}
	}
}

func (d *Daemon) resizeSession(id string, rows, cols int) error {
	if rows <= 0 || cols <= 0 {
		return fmt.Errorf("rows and cols must be positive")
	}
	d.mu.Lock()
	s := d.sessions[id]
	d.mu.Unlock()
	if s == nil || s.cmd == nil || s.cmd.pty == nil {
		return fmt.Errorf("session %q does not have a tty", id)
	}
	return pty.Setsize(s.cmd.pty, &pty.Winsize{
		Rows: uint16(rows),
		Cols: uint16(cols),
	})
}

func internalExecArgs(req ProcessCreateRequest) ([]string, error) {
	command := normalizedCommand(req)
	if len(command) == 0 {
		return nil, fmt.Errorf("command is required")
	}
	args := []string{"internal.exec"}
	args = append([]string{sandboxdGuestBin}, args...)
	if req.Cwd != "" {
		args = append(args, "--cwd", req.Cwd)
	}
	if len(req.Env) > 0 {
		data, err := json.Marshal(req.Env)
		if err != nil {
			return nil, err
		}
		args = append(args, "--env-json", base64.StdEncoding.EncodeToString(data))
	}
	args = append(args, "--")
	args = append(args, command...)
	return args, nil
}

func normalizedCommand(req ProcessCreateRequest) []string {
	if len(req.Command) > 0 {
		return req.Command
	}
	return req.Argv
}

func (d *Daemon) reconcileAll(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, sb := range d.sandboxes {
		ownerAlive := false
		if sb.OwnerSocket != "" {
			c := client.NewFromSocket(sb.OwnerSocket)
			if _, err := c.Status(ctx); err == nil {
				ownerAlive = true
			}
		}
		runscAlive := false
		if sb.RunscID != "" {
			runscAlive = d.runscState(ctx, sb) == nil
		}
		switch {
		case ownerAlive && runscAlive:
			sb.State = StateRunning
		case ownerAlive && !runscAlive:
			sb.State = StateStopped
		case !ownerAlive && runscAlive:
			sb.State = StateBroken
		default:
			if d.volumeExists(ctx, sb.RootfsVolume) {
				sb.State = StateStopped
			} else {
				sb.State = StateMissing
			}
		}
		d.sandboxes[id] = sb
	}
	return d.saveLocked()
}
