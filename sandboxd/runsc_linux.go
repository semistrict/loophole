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
const defaultRunscPlatform = "systrap"


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

func (d *Daemon) sandboxFailureDir(id string) string {
	return filepath.Join(d.dir.SandboxdState(), "failures", id)
}

func (d *Daemon) buildBundle(sb SandboxRecord) error {
	bundleDir := d.bundleDir(sb.ID)
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return err
	}
	if err := d.prepareGuestBinDir(); err != nil {
		return err
	}
	if err := prepareRootfs(sb.Mountpoint); err != nil {
		return err
	}

	spec, err := d.bundleSpec(sb)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0o644)
}

func (d *Daemon) bundleSpec(sb SandboxRecord) (map[string]any, error) {
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
	// Bind-mount the host-side guest bin directory into the sandbox.
	// Contains loophole CLI and loophole-sandboxd (init).
	mounts = append(mounts, map[string]any{
		"destination": "/.loophole/bin",
		"type":        "bind",
		"source":      d.guestBinDir(),
		"options":     []string{"bind", "ro"},
	})
	// Bind-mount the loophole owner socket into the sandbox so guest
	// processes can call flush, checkpoint, clone, etc.
	if sb.OwnerSocket != "" {
		if _, err := os.Stat(sb.OwnerSocket); err == nil {
			mounts = append(mounts, map[string]any{
				"destination": "/.loophole/api.sock",
				"type":        "bind",
				"source":      sb.OwnerSocket,
				"options":     []string{"bind"},
			})
		}
	}

	namespaces := []map[string]any{
		{"type": "pid"},
		{"type": "ipc"},
		{"type": "uts"},
		{"type": "mount"},
	}
	linuxSpec := map[string]any{
		"namespaces": namespaces,
		"maskedPaths": []string{
			"/proc/kcore", "/proc/latency_stats", "/proc/timer_list", "/proc/sched_debug",
			"/sys/firmware", "/proc/scsi",
		},
		"readonlyPaths": []string{
			"/proc/asound", "/proc/bus", "/proc/fs", "/proc/irq",
			"/proc/sys", "/proc/sysrq-trigger",
		},
	}
	if d.runscRootless {
		namespaces = append(namespaces, map[string]any{"type": "user"})
		linuxSpec["uidMappings"] = []map[string]any{
			{
				"containerID": 0,
				"hostID":      os.Getuid(),
				"size":        1,
			},
		}
		linuxSpec["gidMappings"] = []map[string]any{
			{
				"containerID": 0,
				"hostID":      os.Getgid(),
				"size":        1,
			},
		}
		linuxSpec["gidMappingsEnableSetgroups"] = false
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
		"linux":    linuxSpec,
	}
	return spec, nil
}

// guestBinDir returns the host-side directory containing binaries that get
// bind-mounted into every sandbox at /.loophole/bin.
func (d *Daemon) guestBinDir() string {
	return filepath.Join(d.dir.SandboxdState(), "guest-bin")
}

// prepareGuestBinDir creates the host-side /.loophole/bin staging directory
// with hard links to sandboxd and loophole. This directory is bind-mounted
// into every sandbox, keeping the ext4 volume clean.
func (d *Daemon) prepareGuestBinDir() error {
	dir := d.guestBinDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	bins := map[string]string{
		"loophole-sandboxd": d.selfBin,
		"loophole":          d.loopholeBin,
	}
	for name, src := range bins {
		dst := filepath.Join(dir, name)
		srcInfo, err := os.Stat(src)
		if err != nil {
			return fmt.Errorf("stat %s: %w", src, err)
		}
		if dstInfo, err := os.Stat(dst); err == nil {
			if os.SameFile(srcInfo, dstInfo) {
				continue
			}
			_ = os.Remove(dst)
		}
		if err := copyExecutable(src, dst); err != nil {
			return fmt.Errorf("stage %s: %w", name, err)
		}
	}
	return nil
}

// prepareRootfs ensures the rootfs has the minimum directory structure
// needed for the OCI spec mounts (proc, sys, dev, etc.).
func prepareRootfs(rootfs string) error {
	for _, dir := range []string{
		filepath.Join(rootfs, ".loophole", "bin"),
		filepath.Join(rootfs, "bin"),
		filepath.Join(rootfs, "dev"),
		filepath.Join(rootfs, "etc"),
		filepath.Join(rootfs, "proc"),
		filepath.Join(rootfs, "sys"),
		filepath.Join(rootfs, "tmp"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	for _, file := range []string{
		filepath.Join(rootfs, "etc", "hosts"),
		filepath.Join(rootfs, "etc", "resolv.conf"),
	} {
		f, err := os.OpenFile(file, os.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		util.SafeClose(f, "close prepared guest file")
	}
	return nil
}

func copyExecutable(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer util.SafeClose(srcFile, "close source executable")

	tmp := dst + ".tmp"
	dstFile, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		util.SafeClose(dstFile, "close staged executable after copy failure")
		return err
	}
	if err := dstFile.Sync(); err != nil {
		util.SafeClose(dstFile, "close staged executable after sync failure")
		return err
	}
	util.SafeClose(dstFile, "close staged executable")
	return os.Rename(tmp, dst)
}

func (d *Daemon) runscArgs(sandboxID string, args ...string) []string {
	base := []string{
		"--root=" + d.runscRoot,
	}
	if d.runscRootless {
		base = append(base, "--rootless")
	}
	base = append(base,
		"--platform="+d.runscPlatform,
		"--network="+networkModeHost,
		"--ignore-cgroups=true",
		"--file-access=shared",
		"--overlay2=none",
		"--host-uds=open",
	)
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

type sandboxDebugError struct {
	err   error
	debug SandboxDebugInfo
}

func (e *sandboxDebugError) Error() string {
	return e.err.Error()
}

func (e *sandboxDebugError) Unwrap() error {
	return e.err
}

func (d *Daemon) sandboxDebugInfo(id string) SandboxDebugInfo {
	return SandboxDebugInfo{
		SandboxID:     id,
		SandboxDir:    d.sandboxDir(id),
		FailureDir:    d.sandboxFailureDir(id),
		RunscRunLog:   filepath.Join(d.sandboxDir(id), "runsc-run.log"),
		RunscPanicLog: d.runscPanicLogPath(id),
		RunscDebugDir: d.runscDebugDir(id),
	}
}

func (d *Daemon) annotateSandboxError(id string, err error) error {
	if err == nil {
		return nil
	}
	var existing *sandboxDebugError
	if errors.As(err, &existing) {
		return err
	}
	debug := d.snapshotSandboxDebugInfo(id)
	return &sandboxDebugError{
		err:   err,
		debug: debug,
	}
}

func (d *Daemon) snapshotSandboxDebugInfo(id string) SandboxDebugInfo {
	debug := d.sandboxDebugInfo(id)
	failureDir := d.sandboxFailureDir(id)
	if err := os.MkdirAll(failureDir, 0o755); err != nil {
		return debug
	}

	if copied, err := copyIfExists(debug.RunscRunLog, filepath.Join(failureDir, "runsc-run.log")); err == nil && copied {
		debug.RunscRunLog = filepath.Join(failureDir, "runsc-run.log")
	}
	if copied, err := copyIfExists(debug.RunscPanicLog, filepath.Join(failureDir, "runsc-panic.log")); err == nil && copied {
		debug.RunscPanicLog = filepath.Join(failureDir, "runsc-panic.log")
	}
	debugCopyDir := filepath.Join(failureDir, "runsc-debug")
	if copied, err := copyDirIfExists(debug.RunscDebugDir, debugCopyDir); err == nil && copied {
		debug.RunscDebugDir = debugCopyDir
	}
	return debug
}

func copyIfExists(srcPath, dstPath string) (bool, error) {
	srcFile, err := os.Open(srcPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer util.SafeClose(srcFile, "close sandbox debug source file")

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return false, err
	}
	dstFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		util.SafeClose(dstFile, "close sandbox debug destination file after copy failure")
		return false, err
	}
	if err := dstFile.Sync(); err != nil {
		util.SafeClose(dstFile, "close sandbox debug destination file after sync failure")
		return false, err
	}
	util.SafeClose(dstFile, "close sandbox debug destination file")
	return true, nil
}

func copyDirIfExists(srcDir, dstDir string) (bool, error) {
	info, err := os.Stat(srcDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("%s is not a directory", srcDir)
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return false, err
	}
	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dstDir, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		_, err = copyIfExists(path, target)
		return err
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *Daemon) startRunsc(ctx context.Context, sb SandboxRecord) error {
	if err := d.buildBundle(sb); err != nil {
		return err
	}
	if d.runscDebug {
		if err := os.MkdirAll(d.runscDebugDir(sb.ID), 0o755); err != nil {
			return fmt.Errorf("create runsc debug dir: %w", err)
		}
	}
	logPath := filepath.Join(d.sandboxDir(sb.ID), "runsc-run.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer util.SafeClose(logFile, "close runsc start log")
	cmd := d.runscCmd(ctx, sb.ID, "run", "--bundle="+d.bundleDir(sb.ID), "--detach=true", sb.RunscID)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		out, readErr := os.ReadFile(logPath)
		if readErr != nil {
			return d.annotateSandboxError(sb.ID, fmt.Errorf("runsc run: %w (read log: %v)", err, readErr))
		}
		return d.annotateSandboxError(sb.ID, fmt.Errorf("runsc run: %w: %s", err, strings.TrimSpace(string(out))))
	}
	if err := d.waitRunscReady(ctx, sb); err != nil {
		return d.annotateSandboxError(sb.ID, err)
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
