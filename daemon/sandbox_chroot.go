//go:build linux

package daemon

import (
	"bytes"
	"context"
	"os/exec"
)

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole/fsbackend"
)

type chrootSandboxRuntime struct {
	backend *fsbackend.Backend
}

func execSandboxCommand(ctx context.Context, backend *fsbackend.Backend, volume string, cmdStr string) (ExecResult, error) {
	runtime := chrootSandboxRuntime{backend: backend}
	cmd, err := runtime.command(ctx, volume, cmdStr)
	if err != nil {
		return ExecResult{}, err
	}
	return runExecCommand(cmd)
}

func (r *chrootSandboxRuntime) command(ctx context.Context, volume string, cmdStr string) (*exec.Cmd, error) {
	if volume != "" && r.backend != nil {
		if _, err := r.backend.FSForVolume(ctx, volume); err == nil {
			if mountpoint := mountpointForVolume(r.backend, volume); mountpoint != "" {
				return chrootCmdContext(ctx, mountpoint, r.backend, "sh", "-c", cmdStr), nil
			}
		}
	}
	return exec.CommandContext(ctx, "sh", "-c", cmdStr), nil
}

func runExecCommand(cmd *exec.Cmd) (ExecResult, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			return ExecResult{}, err
		}
		exitCode = exitErr.ExitCode()
	}

	return ExecResult{
		ExitCode: exitCode,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
	}, nil
}

func mountpointForVolume(backend *fsbackend.Backend, volume string) string {
	for mp, volName := range backend.Mounts() {
		if volName == volume {
			return mp
		}
	}
	return ""
}

// preparedChroots tracks which mountpoints have had their chroot
// environment set up (bind mounts for /proc, /sys, /dev, etc.).
var preparedChroots sync.Map // mountpoint → true

// chrootEnv is the environment used for chrooted commands.
var chrootEnv = []string{
	"TERM=xterm-256color",
	"HOME=/root",
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

// prepareChrootEnv sets up bind mounts and config files inside a chroot
// mountpoint. This is done once per mountpoint and cached. A before_unmount
// callback is registered on the backend to tear down bind mounts automatically.
func prepareChrootEnv(mountpoint string, backend *fsbackend.Backend) {
	if _, loaded := preparedChroots.LoadOrStore(mountpoint, true); loaded {
		return
	}

	// Bind mounts: /proc, /sys, /dev, /dev/pts, /dev/shm.
	// /dev uses a plain bind (NOT rbind) to avoid propagating host sub-mounts
	// (shm, hugepages, mqueue, host devpts) which would prevent clean unmount.
	binds := []struct {
		src, dst, fstype, data string
		flags                  uintptr
	}{
		{src: "proc", dst: "proc", fstype: "proc"},
		{src: "/sys", dst: "sys", flags: unix.MS_BIND},
		{src: "/dev", dst: "dev", flags: unix.MS_BIND},
		{src: "devpts", dst: "dev/pts", fstype: "devpts", data: "newinstance,ptmxmode=0666"},
		{src: "tmpfs", dst: "dev/shm", fstype: "tmpfs", flags: unix.MS_NOSUID | unix.MS_NODEV},
	}
	for _, b := range binds {
		dst := filepath.Join(mountpoint, b.dst)
		_ = os.MkdirAll(dst, 0o755)
		if err := unix.Mount(b.src, dst, b.fstype, b.flags, b.data); err != nil {
			slog.Warn("chroot mount failed", "dst", dst, "error", err)
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
		if err := unix.Mount(self, dst, "", unix.MS_BIND, ""); err != nil {
			slog.Warn("chroot loophole binary bind mount failed", "error", err)
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
		for _, sub := range []string{"usr/bin/loophole", "dev/shm", "dev/pts", "dev", "sys", "proc"} {
			dst := filepath.Join(mountpoint, sub)
			if err := unix.Unmount(dst, unix.MNT_DETACH); err != nil {
				slog.Warn("chroot umount failed", "dst", dst, "error", err)
			}
		}
	})
}

// chrootCmd builds an exec.Cmd that runs argv inside a chroot at mountpoint.
// It ensures the chroot environment (proc, sys, dev, resolv.conf) is set up.
func chrootCmd(mountpoint string, backend *fsbackend.Backend, argv ...string) *exec.Cmd {
	prepareChrootEnv(mountpoint, backend)
	args := append([]string{mountpoint}, argv...)
	cmd := exec.Command("/usr/sbin/chroot", args...)
	cmd.Dir = "/"
	cmd.Env = chrootEnv
	return cmd
}

func chrootCmdContext(ctx context.Context, mountpoint string, backend *fsbackend.Backend, argv ...string) *exec.Cmd {
	prepareChrootEnv(mountpoint, backend)
	args := append([]string{mountpoint}, argv...)
	cmd := exec.CommandContext(ctx, "/usr/sbin/chroot", args...)
	cmd.Dir = "/"
	cmd.Env = chrootEnv
	return cmd
}

func ensureGuestBinaryExecutable(fi fs.FileInfo) error {
	if fi.Mode()&0o111 != 0 {
		return nil
	}
	return fmt.Errorf("guest binary is not executable: mode=%v", fi.Mode())
}
