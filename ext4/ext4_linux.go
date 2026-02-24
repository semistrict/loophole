//go:build linux

// Package ext4 provides helpers for mounting ext4 filesystems on loop devices.
package ext4

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Format creates an ext4 filesystem on a block device via a loop device.
func Format(ctx context.Context, devicePath string) error {
	loopDev, err := losetup(ctx, devicePath)
	if err != nil {
		return fmt.Errorf("losetup: %w", err)
	}
	defer LosetupDetach(ctx, loopDev)

	if err := run(ctx, "mkfs.ext4", "-q", loopDev); err != nil {
		return fmt.Errorf("mkfs.ext4: %w", err)
	}
	return nil
}

// FormatDirect creates an ext4 filesystem directly on a block device
// (e.g. /dev/nbdN) without loop device setup.
func FormatDirect(ctx context.Context, blockDev string) error {
	if err := run(ctx, "mkfs.ext4", "-q", blockDev); err != nil {
		return fmt.Errorf("mkfs.ext4: %w", err)
	}
	return nil
}

// Mount attaches a loop device to devicePath and mounts ext4 at mountpoint.
// The device must already contain an ext4 filesystem.
// Returns the loop device path so the caller can track it for cleanup.
func Mount(ctx context.Context, devicePath, mountpoint string) (loopDevice string, err error) {
	loopDev, err := losetup(ctx, devicePath)
	if err != nil {
		return "", fmt.Errorf("losetup: %w", err)
	}

	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		LosetupDetach(ctx, loopDev)
		return "", err
	}

	if err := run(ctx, "mount", loopDev, mountpoint); err != nil {
		LosetupDetach(ctx, loopDev)
		return "", fmt.Errorf("mount: %w", err)
	}
	return loopDev, nil
}

// LosetupDetach detaches a loop device. Exported for drivers that track
// loop devices themselves.
func LosetupDetach(ctx context.Context, loopDev string) {
	if err := exec.CommandContext(ctx, "losetup", "--detach", loopDev).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "losetup --detach %s: %v\n", loopDev, err)
	}
}

// MountDirect mounts ext4 directly on a block device (e.g. /dev/nbdN)
// without loop device setup. The device must already contain an ext4 filesystem.
func MountDirect(ctx context.Context, blockDev, mountpoint string) error {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}

	if err := run(ctx, "mount", blockDev, mountpoint); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	return nil
}

// UnmountDirect unmounts a filesystem without loop device cleanup.
func UnmountDirect(ctx context.Context, mountpoint string) error {
	if err := run(ctx, "umount", mountpoint); err != nil {
		return fmt.Errorf("umount %s: %w", mountpoint, err)
	}
	return nil
}

// Unmount unmounts the filesystem and detaches the loop device.
func Unmount(ctx context.Context, mountpoint string) error {
	loopDev, err := findLoopDevice(ctx, mountpoint)
	if err != nil {
		loopDev = ""
	}

	if err := run(ctx, "umount", mountpoint); err != nil {
		return fmt.Errorf("umount %s: %w", mountpoint, err)
	}

	if loopDev != "" {
		LosetupDetach(ctx, loopDev)
	}
	return nil
}

// Freeze quiesces the filesystem at mountpoint (fsfreeze -f).
func Freeze(ctx context.Context, mountpoint string) error {
	return run(ctx, "fsfreeze", "-f", mountpoint)
}

// Thaw resumes the filesystem at mountpoint (fsfreeze -u).
func Thaw(ctx context.Context, mountpoint string) error {
	return run(ctx, "fsfreeze", "-u", mountpoint)
}

func losetup(ctx context.Context, devicePath string) (string, error) {
	out, err := exec.CommandContext(ctx, "losetup", "--direct-io=on", "--find", "--show", devicePath).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func findLoopDevice(ctx context.Context, mountpoint string) (string, error) {
	out, err := exec.CommandContext(ctx, "findmnt", "-n", "-o", "SOURCE", mountpoint).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// IsMounted checks if path is an active mount point.
func IsMounted(path string) bool {
	return exec.Command("findmnt", "-n", path).Run() == nil
}

func run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
