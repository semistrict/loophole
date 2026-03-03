//go:build linux

// Package ext4 provides helpers for mounting ext4 filesystems on loop devices.

// XXX: we should really consider just merging this with linuxutil I don't really know what the difference is

// XXX: in any case I think we should start moving these kinds of packages into internal/

package ext4

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/semistrict/loophole/linuxutil"
)

// mkfsFeatures is the pinned set of ext4 features for mkfs.ext4.
// Pinning avoids surprises when mkfs.ext4 defaults change across distro versions.
// These match Debian bookworm e2fsprogs 1.47 defaults and are all supported by lwext4.
const mkfsFeatures = "has_journal,ext_attr,resize_inode,dir_index,filetype," +
	"extent,64bit,flex_bg,sparse_super,large_file,huge_file," +
	"dir_nlink,extra_isize,metadata_csum"

// Format creates an ext4 filesystem on a block device via a loop device.
// optimalIOSize hints the preferred I/O size to the loop device's block queue
// (0 = no hint).
func Format(ctx context.Context, devicePath string, optimalIOSize int) error {
	dev, err := linuxutil.LoopAttach(devicePath, optimalIOSize)
	if err != nil {
		return fmt.Errorf("loop attach: %w", err)
	}
	defer func() {
		if err := dev.Detach(); err != nil {
			slog.Warn("loop detach failed", "error", err)
		}
	}()

	if err := run(ctx, "mkfs.ext4", "-q", "-O", mkfsFeatures, "-E", "lazy_itable_init=1,nodiscard", dev.Path); err != nil {
		return fmt.Errorf("mkfs.ext4: %w", err)
	}
	return nil
}

// FormatDirect creates an ext4 filesystem directly on a block device
// (e.g. /dev/nbdN) without loop device setup.
func FormatDirect(ctx context.Context, blockDev string) error {
	if err := run(ctx, "mkfs.ext4", "-q", "-O", mkfsFeatures, "-E", "lazy_itable_init=1,nodiscard", blockDev); err != nil {
		return fmt.Errorf("mkfs.ext4: %w", err)
	}
	return nil
}

// Mount attaches a loop device to devicePath and mounts ext4 at mountpoint.
// The device must already contain an ext4 filesystem.
// optimalIOSize hints the preferred I/O size to the loop device's block queue
// (0 = no hint).
// Returns the loop device path so the caller can track it for cleanup.
func Mount(ctx context.Context, devicePath, mountpoint string, optimalIOSize int) (loopDevice string, err error) {
	dev, err := linuxutil.LoopAttach(devicePath, optimalIOSize)
	if err != nil {
		return "", fmt.Errorf("loop attach: %w", err)
	}

	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}

	if err := linuxutil.Mount(linuxutil.MountOpts{Source: dev.Path, Mountpoint: mountpoint, FSType: "ext4", NoAtime: true}); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}
	return dev.Path, nil
}

// LosetupDetach detaches a loop device by path.
func LosetupDetach(ctx context.Context, loopDev string) {
	if err := linuxutil.LoopDetachPath(loopDev); err != nil {
		fmt.Fprintf(os.Stderr, "loop detach %s: %v\n", loopDev, err) // XXX: slog
	}
}

// XXX: can we actually statically enforce the use of slog in this codebase and not fmt.Fprintf?

// MountDirect mounts ext4 directly on a block device (e.g. /dev/nbdN)
// without loop device setup. The device must already contain an ext4 filesystem.
func MountDirect(ctx context.Context, blockDev, mountpoint string) error {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}
	return linuxutil.Mount(linuxutil.MountOpts{Source: blockDev, Mountpoint: mountpoint, FSType: "ext4", NoAtime: true})
}

// UnmountDirect unmounts a filesystem without loop device cleanup.
func UnmountDirect(ctx context.Context, mountpoint string) error {
	return linuxutil.Unmount(mountpoint)
}

// Unmount unmounts the filesystem and detaches the loop device.
func Unmount(ctx context.Context, mountpoint string) error {
	loopDev, _ := linuxutil.FindMount(mountpoint)

	slog.Info("ext4: unmount start", "mountpoint", mountpoint, "loopDev", loopDev)
	if err := linuxutil.Unmount(mountpoint); err != nil {
		slog.Info("ext4: unmount failed", "mountpoint", mountpoint, "error", err)
		return err
	}
	slog.Info("ext4: unmount done", "mountpoint", mountpoint)

	if loopDev != "" {
		LosetupDetach(ctx, loopDev)
	}
	return nil
}

// Freeze quiesces the filesystem at mountpoint.
func Freeze(ctx context.Context, mountpoint string) error {
	return linuxutil.Freeze(mountpoint)
}

// Thaw resumes the filesystem at mountpoint.
func Thaw(ctx context.Context, mountpoint string) error {
	return linuxutil.Thaw(mountpoint)
}

// IsMounted checks if path is an active mount point.
func IsMounted(path string) bool {
	return linuxutil.IsMounted(path)
}

func run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
