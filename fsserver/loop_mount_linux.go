//go:build linux

package fsserver

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

type mountOpts struct {
	Source     string
	Mountpoint string
	NoAtime    bool
	NoBarrier  bool
}

func mountExt4(opts mountOpts) error {
	var flags uintptr
	if opts.NoAtime {
		flags |= unix.MS_NOATIME
	}
	var parts []string
	if opts.NoBarrier {
		parts = append(parts, "nobarrier")
	}
	data := strings.Join(parts, ",")
	if err := unix.Mount(opts.Source, opts.Mountpoint, "ext4", flags, data); err != nil {
		return fmt.Errorf("mount %s on %s: %w", opts.Source, opts.Mountpoint, err)
	}
	return nil
}

func unmountFS(mountpoint string) error {
	if err := unix.Unmount(mountpoint, 0); err != nil {
		return fmt.Errorf("umount %s: %w", mountpoint, err)
	}
	return nil
}

func findMountSource(mountpoint string) (string, error) {
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
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		if fields[4] != mountpoint {
			continue
		}
		for i, field := range fields {
			if field == "-" && i+2 < len(fields) {
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

func freezeFS(mountpoint string) error {
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

func thawFS(mountpoint string) error {
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

const (
	loopClearFD    = 0x4C01
	loopConfigure  = 0x4C0A
	loopCtlGetFree = 0x4C82

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

type loopDevice struct {
	Path string
}

type loopAttachOpts struct {
	OptimalIOSize int
}

func loopAttach(backingPath string, opts loopAttachOpts) (*loopDevice, error) {
	backingFD, err := unix.Open(backingPath, unix.O_RDWR|unix.O_CLOEXEC, 0)
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
			dev := &loopDevice{Path: devPath}
			dev.tuneBlockQueue(opts.OptimalIOSize)
			return dev, nil
		}
		if !isEBUSY(err) {
			return nil, fmt.Errorf("loop attach: %w", err)
		}
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
	}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(loopFile.Fd()), uintptr(loopConfigure), uintptr(unsafe.Pointer(&cfg)))
	if cerr := loopFile.Close(); cerr != nil {
		slog.Warn("close failed", "error", cerr)
	}
	if errno != 0 {
		return "", fmt.Errorf("LOOP_CONFIGURE %s: %w", devPath, errno)
	}

	return devPath, nil
}

func (d *loopDevice) tuneBlockQueue(optimalIOSize int) {
	if optimalIOSize <= 0 {
		return
	}
	base := d.Path[len("/dev/"):]
	val := fmt.Sprintf("%d", optimalIOSize)
	for _, param := range []string{"optimal_io_size", "minimum_io_size"} {
		path := fmt.Sprintf("/sys/block/%s/queue/%s", base, param)
		if err := os.WriteFile(path, []byte(val), 0); err != nil {
			slog.Warn("failed to set block queue parameter", "path", path, "value", val, "error", err)
		}
	}
}

func (d *loopDevice) Detach() error {
	return loopDetachPath(d.Path)
}

func loopDetachPath(devPath string) error {
	f, err := os.OpenFile(devPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", devPath, err)
	}
	err = unix.IoctlSetInt(int(f.Fd()), loopClearFD, 0)
	if cerr := f.Close(); cerr != nil {
		slog.Warn("close failed", "error", cerr)
	}
	if err != nil {
		return fmt.Errorf("LOOP_CLR_FD %s: %w", devPath, err)
	}

	// LOOP_CLR_FD is only a detach request. The loop device may remain visible
	// until its backing file stack fully unwinds (notably when the backing file
	// is provided by our FUSE server). Do a short best-effort poll so plain
	// unmounts still clean up quickly, but don't block shutdown on a detach that
	// will complete asynchronously once the FUSE server closes.
	base := devPath[len("/dev/"):]
	sysPath := fmt.Sprintf("/sys/block/%s/loop/backing_file", base)
	for range 100 {
		data, err := os.ReadFile(sysPath)
		if err != nil || len(strings.TrimSpace(string(data))) == 0 {
			return nil
		}
		_ = unix.Nanosleep(&unix.Timespec{Nsec: 10_000_000}, nil)
	}
	slog.Debug("loop detach still pending after request", "device", devPath)
	return nil
}

const mkfsExt4Features = "has_journal,ext_attr,resize_inode,dir_index,filetype," +
	"extent,64bit,flex_bg,sparse_super,large_file,huge_file," +
	"dir_nlink,extra_isize,metadata_csum"

func mkfsArgs(device string) (string, []string) {
	return "mkfs.ext4", []string{"-q", "-O", mkfsExt4Features, "-E", "lazy_itable_init=1,nodiscard", device}
}

func formatFS(ctx context.Context, devicePath string, optimalIOSize int) error {
	dev, err := loopAttach(devicePath, loopAttachOpts{OptimalIOSize: optimalIOSize})
	if err != nil {
		return fmt.Errorf("loop attach: %w", err)
	}
	defer func() {
		if err := dev.Detach(); err != nil {
			slog.Warn("loop detach failed", "error", err)
		}
	}()

	cmd, args := mkfsArgs(dev.Path)
	if err := runHelper(ctx, cmd, args...); err != nil {
		return fmt.Errorf("%s: %w", cmd, err)
	}
	return nil
}

func mountFS(ctx context.Context, devicePath, mountpoint string, opts loopAttachOpts) (loopDevicePath string, err error) {
	dev, err := loopAttach(devicePath, opts)
	if err != nil {
		return "", fmt.Errorf("loop attach: %w", err)
	}

	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}

	if err := mountExt4(mountOpts{Source: dev.Path, Mountpoint: mountpoint, NoAtime: true}); err != nil {
		if derr := dev.Detach(); derr != nil {
			slog.Warn("loop detach failed", "error", derr)
		}
		return "", err
	}
	return dev.Path, nil
}

func unmountLoop(ctx context.Context, mountpoint string) error {
	loopDev, _ := findMountSource(mountpoint)

	slog.Info("unmount start", "mountpoint", mountpoint, "loopDev", loopDev)
	if err := unmountFS(mountpoint); err != nil {
		slog.Info("unmount failed", "mountpoint", mountpoint, "error", err)
		return err
	}
	slog.Info("unmount done", "mountpoint", mountpoint)

	if loopDev != "" {
		if err := loopDetachPath(loopDev); err != nil {
			slog.Warn("loop detach failed", "device", loopDev, "error", err)
		}
	}
	_ = ctx
	return nil
}

func runHelper(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
