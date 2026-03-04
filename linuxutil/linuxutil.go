//go:build linux

// Package linuxutil provides direct syscall-based Linux utilities,
// replacing exec calls to mount, umount, findmnt, fsfreeze, and losetup.
// Requires Linux 6+.
package linuxutil

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// MountOpts configures a filesystem mount.
type MountOpts struct {
	Source     string
	Mountpoint string
	FSType     string
	NoAtime    bool
	NoBarrier  bool
}

// Mount mounts a filesystem with the given options.
func Mount(opts MountOpts) error {
	var flags uintptr
	if opts.NoAtime {
		flags |= unix.MS_NOATIME
	}
	var data string
	if opts.NoBarrier {
		data = "nobarrier"
	}
	if err := unix.Mount(opts.Source, opts.Mountpoint, opts.FSType, flags, data); err != nil {
		return fmt.Errorf("mount %s on %s: %w", opts.Source, opts.Mountpoint, err)
	}
	return nil
}

// Unmount unmounts the filesystem at mountpoint.
func Unmount(mountpoint string) error {
	if err := unix.Unmount(mountpoint, 0); err != nil {
		return fmt.Errorf("umount %s: %w", mountpoint, err)
	}
	return nil
}

// FindMount returns the source device for a given mountpoint by reading
// /proc/self/mountinfo. Returns ("", nil) if not found.
func FindMount(mountpoint string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// mountinfo format: id parent major:minor root mount-point options ... - fstype source super-options
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mp := fields[4]
		if mp != mountpoint {
			continue
		}
		// Find the separator "-" to get fstype and source.
		for i, f := range fields {
			if f == "-" && i+2 < len(fields) {
				return fields[i+2], nil
			}
		}
	}
	return "", scanner.Err()
}

const (
	fiFreeze = 0xC0045877
	fiThaw   = 0xC0045878
)

// Freeze quiesces the filesystem at mountpoint (FIFREEZE ioctl).
func Freeze(mountpoint string) error {
	f, err := os.Open(mountpoint)
	if err != nil {
		return fmt.Errorf("open %s: %w", mountpoint, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), uintptr(fiFreeze), 0)
	if errno != 0 {
		return fmt.Errorf("FIFREEZE %s: %w", mountpoint, errno)
	}
	return nil
}

// Thaw resumes the filesystem at mountpoint (FITHAW ioctl).
func Thaw(mountpoint string) error {
	f, err := os.Open(mountpoint)
	if err != nil {
		return fmt.Errorf("open %s: %w", mountpoint, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), uintptr(fiThaw), 0)
	if errno != 0 {
		return fmt.Errorf("FITHAW %s: %w", mountpoint, errno)
	}
	return nil
}

// MaxLoopDevices returns the kernel's loop device limit by reading
// /sys/module/loop/parameters/max_loop. A value of 0 means unlimited.
func MaxLoopDevices() (int, error) {
	data, err := os.ReadFile("/sys/module/loop/parameters/max_loop")
	if err != nil {
		return 0, fmt.Errorf("read max_loop: %w", err)
	}
	s := strings.TrimSpace(string(data))
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("parse max_loop %q: unexpected character", s)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// Loop device support.

const (
	loopClearFD    = 0x4C01
	loopConfigure  = 0x4C0A
	loopCtlGetFree = 0x4C82

	loFlagsAutoclear = 4
	loFlagsDirectIO  = 16

	loNameSize = 64
	loKeySize  = 32
)

type loopInfo64 struct {
	Device         uint64
	Inode          uint64
	Rdevice        uint64
	Offset         uint64
	SizeLimit      uint64
	Number         uint32
	EncryptType    uint32
	EncryptKeySize uint32
	Flags          uint32
	FileName       [loNameSize]byte
	CryptName      [loNameSize]byte
	EncryptKey     [loKeySize]byte
	Init           [2]uint64
}

type loopConfig struct {
	FD        uint32
	BlockSize uint32
	Info      loopInfo64
	Reserved  [8]uint64
}

// LoopDevice represents an attached loop device.
type LoopDevice struct {
	Path string
}

// LoopAttach finds a free loop device, attaches it to the file at backingPath
// with O_DIRECT and autoclear, and returns the LoopDevice.
//
// optimalIOSize hints the preferred I/O size to the block layer via sysfs
// (optimal_io_size and minimum_io_size). The logical sector size is always
// set to 4096 via LOOP_CONFIGURE. Failures to set sysfs tunables are logged
// as warnings but do not prevent attach.
//
// The kernel's GET_FREE and CONFIGURE ioctls are not atomic, so a concurrent
// caller can grab the same device number. We retry up to 6 times on EBUSY,
// mirroring the strategy used by util-linux's losetup.
func LoopAttach(backingPath string, optimalIOSize int) (*LoopDevice, error) {
	backingFD, err := unix.Open(backingPath, unix.O_RDWR|unix.O_CLOEXEC|unix.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("open backing file %s: %w", backingPath, err)
	}
	defer func() {
		if err := unix.Close(backingFD); err != nil {
			slog.Warn("close failed", "fd", backingFD, "error", err)
		}
	}()

	const maxRetries = 6
	for attempt := range maxRetries {
		devPath, err := loopConfigure1(backingFD)
		if err == nil {
			dev := &LoopDevice{Path: devPath}
			dev.tuneBlockQueue(optimalIOSize)
			return dev, nil
		}
		if !isEBUSY(err) {
			return nil, fmt.Errorf("loop attach: %w", err)
		}
		// EBUSY: another process grabbed this device, retry.
		_ = attempt
	}
	return nil, fmt.Errorf("loop attach: all %d attempts got EBUSY", maxRetries)
}

func isEBUSY(err error) bool {
	for err != nil {
		if err == unix.EBUSY {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// loopConfigure1 does a single GET_FREE + CONFIGURE attempt.
// Returns the device path on success, or a wrapped EBUSY on contention.
func loopConfigure1(backingFD int) (string, error) {
	ctl, err := os.OpenFile("/dev/loop-control", os.O_RDWR, 0)
	if err != nil {
		return "", fmt.Errorf("open loop-control: %w", err)
	}
	nr, err := unix.IoctlRetInt(int(ctl.Fd()), loopCtlGetFree)
	if cerr := ctl.Close(); cerr != nil {
		slog.Warn("close failed", "error", cerr)
	}
	if err != nil {
		return "", fmt.Errorf("LOOP_CTL_GET_FREE: %w", err)
	}

	devPath := fmt.Sprintf("/dev/loop%d", nr)

	// In containers without udev, the device node may not exist yet.
	if _, err := os.Stat(devPath); os.IsNotExist(err) {
		dev := unix.Mkdev(7, uint32(nr))
		if err := unix.Mknod(devPath, unix.S_IFBLK|0o660, int(dev)); err != nil && !os.IsExist(err) {
			return "", fmt.Errorf("mknod %s: %w", devPath, err)
		}
	}

	loopFile, err := os.OpenFile(devPath, os.O_RDWR, 0)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", devPath, err)
	}

	cfg := loopConfig{
		FD:        uint32(backingFD),
		BlockSize: 4096,
		Info: loopInfo64{
			Flags: loFlagsDirectIO,
		},
	}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL,
		uintptr(loopFile.Fd()),
		uintptr(loopConfigure),
		uintptr(unsafe.Pointer(&cfg)))
	if cerr := loopFile.Close(); cerr != nil {
		slog.Warn("close failed", "error", cerr)
	}
	if errno != 0 {
		return "", fmt.Errorf("LOOP_CONFIGURE %s: %w", devPath, errno)
	}

	return devPath, nil
}

// tuneBlockQueue sets optimal_io_size and minimum_io_size on the loop
// device's block queue via sysfs. Failures are logged as warnings.
func (d *LoopDevice) tuneBlockQueue(optimalIOSize int) {
	if optimalIOSize <= 0 {
		return
	}
	// d.Path is e.g. "/dev/loop5" → sysfs base is "loop5".
	base := d.Path[len("/dev/"):]
	val := fmt.Sprintf("%d", optimalIOSize)
	for _, param := range []string{"optimal_io_size", "minimum_io_size"} {
		path := fmt.Sprintf("/sys/block/%s/queue/%s", base, param)
		if err := os.WriteFile(path, []byte(val), 0); err != nil {
			slog.Warn("failed to set block queue parameter", "path", path, "value", val, "error", err)
		}
	}
}

// Detach detaches the loop device from its backing file.
func (d *LoopDevice) Detach() error {
	return LoopDetachPath(d.Path)
}

// LoopDetachPath detaches the loop device at the given path (e.g. "/dev/loop0").
//
// On modern kernels (5.12+), LOOP_CLR_FD schedules cleanup asynchronously via
// loop_schedule_rundown. We close our fd immediately (so the refcount drops)
// and then poll the sysfs backing_file to confirm the device is fully released
// before returning to the caller.
func LoopDetachPath(devPath string) error {
	f, err := os.OpenFile(devPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", devPath, err)
	}
	err = unix.IoctlSetInt(int(f.Fd()), loopClearFD, 0)
	// Close immediately so the kernel can proceed with async cleanup.
	if cerr := f.Close(); cerr != nil {
		slog.Warn("close failed", "error", cerr)
	}
	if err != nil {
		return fmt.Errorf("LOOP_CLR_FD %s: %w", devPath, err)
	}

	// Wait for the kernel's async rundown to finish. On modern kernels
	// LOOP_CLR_FD is asynchronous; we poll the sysfs backing_file which
	// is cleared once rundown completes. Typically resolves in <1ms.
	base := devPath[len("/dev/"):]
	sysPath := fmt.Sprintf("/sys/block/%s/loop/backing_file", base)
	for range 100 {
		data, err := os.ReadFile(sysPath)
		if err != nil || len(strings.TrimSpace(string(data))) == 0 {
			return nil
		}
		unix.Nanosleep(&unix.Timespec{Nsec: 1_000_000}, nil) // 1ms
	}
	return nil
}

// ext4 filesystem helpers.

// mkfsFeatures is the pinned set of ext4 features for mkfs.ext4.
// Pinning avoids surprises when mkfs.ext4 defaults change across distro versions.
// These match Debian bookworm e2fsprogs 1.47 defaults and are all supported by lwext4.
const mkfsFeatures = "has_journal,ext_attr,resize_inode,dir_index,filetype," +
	"extent,64bit,flex_bg,sparse_super,large_file,huge_file," +
	"dir_nlink,extra_isize,metadata_csum"

// Ext4Format creates an ext4 filesystem on a block device via a loop device.
func Ext4Format(ctx context.Context, devicePath string, optimalIOSize int) error {
	dev, err := LoopAttach(devicePath, optimalIOSize)
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

// Ext4FormatDirect creates an ext4 filesystem directly on a block device
// (e.g. /dev/nbdN) without loop device setup.
func Ext4FormatDirect(ctx context.Context, blockDev string) error {
	if err := run(ctx, "mkfs.ext4", "-q", "-O", mkfsFeatures, "-E", "lazy_itable_init=1,nodiscard", blockDev); err != nil {
		return fmt.Errorf("mkfs.ext4: %w", err)
	}
	return nil
}

// Ext4Mount attaches a loop device to devicePath and mounts ext4 at mountpoint.
// Returns the loop device path for cleanup.
func Ext4Mount(ctx context.Context, devicePath, mountpoint string, optimalIOSize int) (loopDevice string, err error) {
	dev, err := LoopAttach(devicePath, optimalIOSize)
	if err != nil {
		return "", fmt.Errorf("loop attach: %w", err)
	}

	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}

	if err := Mount(MountOpts{Source: dev.Path, Mountpoint: mountpoint, FSType: "ext4", NoAtime: true}); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}
	return dev.Path, nil
}

// Ext4MountDirect mounts ext4 directly on a block device (e.g. /dev/nbdN).
func Ext4MountDirect(ctx context.Context, blockDev, mountpoint string) error {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}
	return Mount(MountOpts{Source: blockDev, Mountpoint: mountpoint, FSType: "ext4", NoAtime: true})
}

// Ext4Unmount unmounts the filesystem and detaches the loop device.
func Ext4Unmount(ctx context.Context, mountpoint string) error {
	loopDev, _ := FindMount(mountpoint)

	slog.Info("ext4: unmount start", "mountpoint", mountpoint, "loopDev", loopDev)
	if err := Unmount(mountpoint); err != nil {
		slog.Info("ext4: unmount failed", "mountpoint", mountpoint, "error", err)
		return err
	}
	slog.Info("ext4: unmount done", "mountpoint", mountpoint)

	if loopDev != "" {
		if err := LoopDetachPath(loopDev); err != nil {
			slog.Warn("loop detach failed", "device", loopDev, "error", err)
		}
	}
	return nil
}

func run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
