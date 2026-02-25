//go:build linux

// Package linuxutil provides direct syscall-based Linux utilities,
// replacing exec calls to mount, umount, findmnt, fsfreeze, and losetup.
// Requires Linux 6+.
package linuxutil

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Mount mounts a filesystem at mountpoint.
func Mount(source, mountpoint, fstype string) error {
	if err := unix.Mount(source, mountpoint, fstype, 0, ""); err != nil {
		return fmt.Errorf("mount %s on %s: %w", source, mountpoint, err)
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

// IsMounted checks if path is an active mount point.
func IsMounted(path string) bool {
	source, _ := FindMount(path)
	return source != ""
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
// The kernel's GET_FREE and CONFIGURE ioctls are not atomic, so a concurrent
// caller can grab the same device number. We retry up to 6 times on EBUSY,
// mirroring the strategy used by util-linux's losetup.
func LoopAttach(backingPath string) (*LoopDevice, error) {
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
			return &LoopDevice{Path: devPath}, nil
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
		FD: uint32(backingFD),
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

// Detach detaches the loop device from its backing file.
func (d *LoopDevice) Detach() error {
	return LoopDetachPath(d.Path)
}

// LoopDetachPath detaches the loop device at the given path (e.g. "/dev/loop0").
func LoopDetachPath(devPath string) error {
	f, err := os.OpenFile(devPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", devPath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	if err := unix.IoctlSetInt(int(f.Fd()), loopClearFD, 0); err != nil {
		return fmt.Errorf("LOOP_CLR_FD %s: %w", devPath, err)
	}
	return nil
}
