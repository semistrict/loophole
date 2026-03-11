//go:build linux

package mmap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

var linuxVMPageSize = unix.Getpagesize()

// ---------------------------------------------------------------------------
// userfaultfd ioctl constants and structs
// ---------------------------------------------------------------------------

func _ioc(dir, typ, nr, size uintptr) uintptr {
	return dir<<30 | size<<16 | typ<<8 | nr
}

func _ior(typ, nr, size uintptr) uintptr  { return _ioc(2, typ, nr, size) }
func _iow(typ, nr, size uintptr) uintptr  { return _ioc(1, typ, nr, size) }
func _iowr(typ, nr, size uintptr) uintptr { return _ioc(3, typ, nr, size) }

var (
	_UFFDIO_API          = _iowr(0xAA, 0x3F, unsafe.Sizeof(uffdioAPI{}))
	_UFFDIO_REGISTER     = _iowr(0xAA, 0x00, unsafe.Sizeof(uffdioRegister{}))
	_UFFDIO_UNREGISTER   = _iow(0xAA, 0x01, unsafe.Sizeof(uffdioRange{}))
	_UFFDIO_WAKE         = _ior(0xAA, 0x02, unsafe.Sizeof(uffdioRange{}))
	_UFFDIO_COPY         = _iowr(0xAA, 0x03, unsafe.Sizeof(ufdioCopy{}))
	_UFFDIO_ZEROPAGE     = _iowr(0xAA, 0x04, unsafe.Sizeof(ufdioZeropage{}))
	_UFFDIO_WRITEPROTECT = _iowr(0xAA, 0x06, unsafe.Sizeof(uffdioWriteprotect{}))
)

const (
	_UFFD_API                       = 0xAA
	_UFFDIO_REGISTER_MODE_MISSING   = 1 << 0
	_UFFDIO_REGISTER_MODE_WP        = 1 << 1
	_UFFD_EVENT_PAGEFAULT           = 0x12
	_UFFD_PAGEFAULT_FLAG_WRITE      = 1 << 0
	_UFFD_PAGEFAULT_FLAG_WP         = 1 << 1
	_UFFD_FEATURE_PAGEFAULT_FLAG_WP = 1 << 0
	_UFFDIO_WRITEPROTECT_MODE_WP    = 1 << 0
	_UFFDIO_COPY_MODE_DONTWAKE      = 1 << 0
	_UFFDIO_ZEROPAGE_MODE_DONTWAKE  = 1 << 0
)

type uffdioAPI struct {
	API      uint64
	Features uint64
	Ioctls   uint64
}

type uffdioRange struct {
	Start uint64
	Len   uint64
}

type uffdioRegister struct {
	Range  uffdioRange
	Mode   uint64
	Ioctls uint64
}

type uffdMsg struct {
	Event uint8
	_     [3]uint8
	_     uint32
	Arg   [24]byte
}

func (m *uffdMsg) faultAddress() uint64 {
	return binary.LittleEndian.Uint64(m.Arg[8:16])
}

func (m *uffdMsg) faultFlags() uint64 {
	return binary.LittleEndian.Uint64(m.Arg[0:8])
}

type ufdioCopy struct {
	Dst  uint64
	Src  uint64
	Len  uint64
	Mode uint64
	Copy int64
}

type ufdioZeropage struct {
	Range    uffdioRange
	Mode     uint64
	Zeropage int64
}

type uffdioWriteprotect struct {
	Range uffdioRange
	Mode  uint64
}

// ---------------------------------------------------------------------------
// Platform-specific state and helpers
// ---------------------------------------------------------------------------

// platformState holds Linux-specific file descriptors.
type platformState struct {
	uffdFD  int
	epollFD int
	eventFD int
	backend Backend
}

// anonMmap creates an anonymous RW mapping.
func anonMmap(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
}

// munmap unmaps a region.
func munmap(data []byte) error {
	return unix.Munmap(data)
}

func ioctl(fd int, req uintptr, arg unsafe.Pointer) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(arg))
	if errno != 0 {
		return errno
	}
	return nil
}

func (mr *MappedRegion) chooseLinuxBackend(wpSupported bool) (Backend, error) {
	switch mr.cfg.backend {
	case BackendAuto:
		if wpSupported {
			return BackendMissingWP, nil
		}
		return BackendMissing, nil
	case BackendMissing:
		return BackendMissing, nil
	case BackendMissingWP:
		if !wpSupported {
			return "", fmt.Errorf("mmap: requested backend %q but kernel lacks UFFD write-protect support", BackendMissingWP)
		}
		return BackendMissingWP, nil
	default:
		return "", fmt.Errorf("mmap: unknown backend %q", mr.cfg.backend)
	}
}

// platformInit sets up userfaultfd, epoll, and eventfd for the mapped region.
func (mr *MappedRegion) platformInit() error {
	mr.uffdFD = -1
	mr.epollFD = -1
	mr.eventFD = -1

	// Create userfaultfd.
	uffdFD, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|unix.O_NONBLOCK, 0, 0)
	if errno != 0 {
		return fmt.Errorf("mmap: userfaultfd: %w", errno)
	}

	// UFFDIO_API handshake. Some kernels reject the handshake outright when
	// we request write-protect support. Retry without optional features so we
	// can still detect missing-page UFFD support and return a precise "no WP"
	// error from backend selection below.
	api := uffdioAPI{
		API:      _UFFD_API,
		Features: _UFFD_FEATURE_PAGEFAULT_FLAG_WP,
	}
	if err := ioctl(int(uffdFD), _UFFDIO_API, unsafe.Pointer(&api)); err != nil {
		if !errors.Is(err, unix.EINVAL) {
			unix.Close(int(uffdFD))
			return fmt.Errorf("mmap: UFFDIO_API: %w", err)
		}

		api = uffdioAPI{API: _UFFD_API}
		if err := ioctl(int(uffdFD), _UFFDIO_API, unsafe.Pointer(&api)); err != nil {
			unix.Close(int(uffdFD))
			return fmt.Errorf("mmap: UFFDIO_API: %w", err)
		}
	}

	backend, err := mr.chooseLinuxBackend(api.Features&_UFFD_FEATURE_PAGEFAULT_FLAG_WP != 0)
	if err != nil {
		unix.Close(int(uffdFD))
		return err
	}

	// Register the range for missing-page faults.
	reg := uffdioRegister{
		Range: uffdioRange{Start: uint64(mr.addr), Len: mr.size},
		Mode:  _UFFDIO_REGISTER_MODE_MISSING,
	}
	if backend == BackendMissingWP {
		reg.Mode |= _UFFDIO_REGISTER_MODE_WP
	}
	if err := ioctl(int(uffdFD), _UFFDIO_REGISTER, unsafe.Pointer(&reg)); err != nil {
		unix.Close(int(uffdFD))
		return fmt.Errorf("mmap: UFFDIO_REGISTER: %w", err)
	}

	// Create epoll + eventfd for shutdown signaling.
	epollFD, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		unix.Close(int(uffdFD))
		return fmt.Errorf("mmap: epoll_create1: %w", err)
	}

	eventFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		unix.Close(int(uffdFD))
		unix.Close(epollFD)
		return fmt.Errorf("mmap: eventfd: %w", err)
	}

	if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, int(uffdFD), &unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(uffdFD),
	}); err != nil {
		unix.Close(int(uffdFD))
		unix.Close(epollFD)
		unix.Close(eventFD)
		return fmt.Errorf("mmap: epoll_ctl uffd: %w", err)
	}
	if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, eventFD, &unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(eventFD),
	}); err != nil {
		unix.Close(int(uffdFD))
		unix.Close(epollFD)
		unix.Close(eventFD)
		return fmt.Errorf("mmap: epoll_ctl eventfd: %w", err)
	}

	mr.uffdFD = int(uffdFD)
	mr.epollFD = epollFD
	mr.eventFD = eventFD
	mr.backend = backend
	return nil
}

// platformSignalStop signals handler goroutines to exit via eventfd.
func (mr *MappedRegion) platformSignalStop() {
	if mr.eventFD < 0 {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	_, _ = unix.Write(mr.eventFD, buf[:])
}

// platformClose cleans up Linux-specific file descriptors.
func (mr *MappedRegion) platformClose() error {
	var errs []error

	// Close uffd first — implicitly unregisters all ranges.
	if mr.uffdFD >= 0 {
		if err := unix.Close(mr.uffdFD); err != nil {
			errs = append(errs, fmt.Errorf("mmap: close uffd: %w", err))
		}
		mr.uffdFD = -1
	}
	if mr.epollFD >= 0 {
		if err := unix.Close(mr.epollFD); err != nil {
			errs = append(errs, fmt.Errorf("mmap: close epollfd: %w", err))
		}
		mr.epollFD = -1
	}
	if mr.eventFD >= 0 {
		if err := unix.Close(mr.eventFD); err != nil {
			errs = append(errs, fmt.Errorf("mmap: close eventfd: %w", err))
		}
		mr.eventFD = -1
	}
	return errors.Join(errs...)
}

func (mr *MappedRegion) platformPostInit() error {
	return nil
}

func (mr *MappedRegion) platformSyncDirtyPages() error {
	return nil
}

func (mr *MappedRegion) wakeFault(pageAddr uint64) error {
	wake := uffdioRange{Start: pageAddr, Len: pageSize}
	if err := ioctl(mr.uffdFD, _UFFDIO_WAKE, unsafe.Pointer(&wake)); err != nil {
		return fmt.Errorf("mmap: UFFDIO_WAKE: %w", err)
	}
	return nil
}

func (mr *MappedRegion) platformPageSpan(pageAddr uint64) (uint64, int) {
	base := pageAddr &^ uint64(linuxVMPageSize-1)
	remaining := int(uint64(mr.addr) + mr.size - base)
	if remaining < linuxVMPageSize {
		return base, remaining
	}
	return base, linuxVMPageSize
}

func (mr *MappedRegion) platformProtectCleanPage(pageAddr uint64) error {
	if mr.backend == BackendMissing {
		return nil
	}
	base, span := mr.platformPageSpan(pageAddr)
	wp := uffdioWriteprotect{
		Range: uffdioRange{Start: base, Len: uint64(span)},
		Mode:  _UFFDIO_WRITEPROTECT_MODE_WP,
	}
	if err := ioctl(mr.uffdFD, _UFFDIO_WRITEPROTECT, unsafe.Pointer(&wp)); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("mmap: UFFDIO_WRITEPROTECT set: %w", err)
		}
	}
	return nil
}

func (mr *MappedRegion) platformMarkDirty(pageAddr uint64) error {
	if mr.backend == BackendMissing {
		return nil
	}
	base, span := mr.platformPageSpan(pageAddr)
	wp := uffdioWriteprotect{
		Range: uffdioRange{Start: base, Len: uint64(span)},
	}
	if err := ioctl(mr.uffdFD, _UFFDIO_WRITEPROTECT, unsafe.Pointer(&wp)); err != nil {
		if !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("mmap: UFFDIO_WRITEPROTECT clear: %w", err)
		}
	}
	return nil
}

func (mr *MappedRegion) platformPrepareFlush(pageAddr uint64) error {
	if mr.backend == BackendMissing {
		return nil
	}
	return mr.platformProtectCleanPage(pageAddr)
}

func (mr *MappedRegion) platformEvictPage(pageAddr uint64) error {
	if mr.backend == BackendMissing {
		return nil
	}
	if err := mr.platformMarkDirty(pageAddr); err != nil {
		return err
	}
	base, span := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(base)
	page := mr.data[startIdx*pageSize : startIdx*pageSize+span]
	if err := unix.Madvise(page, unix.MADV_DONTNEED); err != nil {
		return fmt.Errorf("mmap: madvise(DONTNEED): %w", err)
	}
	return nil
}

// handleFaults is the per-goroutine fault handler loop.
func (mr *MappedRegion) handleFaults() {
	defer mr.wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Allocate a page-aligned buffer for UFFDIO_COPY source data.
	srcBuf, err := unix.Mmap(-1, 0, pageSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		slog.Error("mmap: failed to allocate aligned src buffer", "error", err)
		return
	}
	defer unix.Munmap(srcBuf)

	events := make([]unix.EpollEvent, 2)
	var msg uffdMsg
	ctx := context.Background()

	for {
		n, err := unix.EpollWait(mr.epollFD, events, -1)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			slog.Error("mmap: epoll_wait failed", "error", err)
			return
		}

		for i := range n {
			if events[i].Fd == int32(mr.eventFD) {
				return
			}

			// Read fault messages from uffd.
			for {
				_, err := unix.Read(mr.uffdFD, unsafe.Slice((*byte)(unsafe.Pointer(&msg)), int(unsafe.Sizeof(msg))))
				if err != nil {
					if errors.Is(err, unix.EAGAIN) {
						break
					}
					slog.Error("mmap: read uffd failed", "error", err)
					return
				}

				if msg.Event != _UFFD_EVENT_PAGEFAULT {
					continue
				}

				faultAddr := msg.faultAddress()
				flags := msg.faultFlags()
				if mr.backend == BackendMissingWP && flags&_UFFD_PAGEFAULT_FLAG_WP != 0 {
					mr.notePageDirty(faultAddr)
					if err := mr.platformMarkDirty(faultAddr); err != nil {
						slog.Error("mmap: clear write-protect failed", "error", err)
					}
					continue
				}
				mr.resolveFault(ctx, faultAddr, srcBuf, flags&_UFFD_PAGEFAULT_FLAG_WRITE != 0)

				// Readahead: resolve subsequent pages.
				for j := range mr.cfg.readahead {
					nextAddr := faultAddr + uint64(j+1)*pageSize
					if nextAddr >= uint64(mr.addr)+mr.size {
						break
					}
					mr.resolveFault(ctx, nextAddr, srcBuf, false)
				}
			}
		}
	}
}

// resolveFault resolves a single page fault at the given address.
func (mr *MappedRegion) resolveFault(ctx context.Context, faultAddr uint64, srcBuf []byte, isWrite bool) {
	resolvedClean := false
	copyMode := uint64(0)
	zeropageMode := uint64(0)

	if err := mr.readPage(ctx, srcBuf, faultAddr); err != nil {
		slog.Error("mmap: Read failed, resolving with zeros",
			"fault_addr", fmt.Sprintf("0x%x", faultAddr),
			"vol_offset", mr.volOff+(faultAddr-uint64(mr.addr)),
			"error", err,
		)
		zp := ufdioZeropage{
			Range: uffdioRange{Start: faultAddr, Len: pageSize},
			Mode:  zeropageMode,
		}
		if err := ioctl(mr.uffdFD, _UFFDIO_ZEROPAGE, unsafe.Pointer(&zp)); err != nil {
			if !errors.Is(err, unix.EEXIST) {
				slog.Error("mmap: UFFDIO_ZEROPAGE failed", "error", err)
			}
			return
		}
		resolvedClean = !isWrite
		if !resolvedClean {
			mr.notePageDirty(faultAddr)
			return
		}
		if mr.backend == BackendMissing {
			mr.notePageLoadedClean(faultAddr)
			return
		}
		if err := mr.platformProtectCleanPage(faultAddr); err != nil {
			slog.Error("mmap: protect clean zero page failed", "error", err)
			return
		}
		mr.notePageLoadedClean(faultAddr)
		return
	}

	cp := ufdioCopy{
		Dst:  faultAddr,
		Src:  uint64(uintptr(unsafe.Pointer(&srcBuf[0]))),
		Len:  pageSize,
		Mode: copyMode,
	}
	if err := ioctl(mr.uffdFD, _UFFDIO_COPY, unsafe.Pointer(&cp)); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			slog.Error("mmap: UFFDIO_COPY failed", "error", err)
		}
		return
	}
	if isWrite {
		mr.notePageDirty(faultAddr)
		return
	}
	if mr.backend == BackendMissing {
		mr.notePageLoadedClean(faultAddr)
		return
	}
	if err := mr.platformProtectCleanPage(faultAddr); err != nil {
		slog.Error("mmap: protect clean page failed", "error", err)
		return
	}
	mr.notePageLoadedClean(faultAddr)
}
