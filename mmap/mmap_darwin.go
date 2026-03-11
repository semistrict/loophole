//go:build darwin

package mmap

/*
#include <mach/mach_error.h>
#include <mach/kern_return.h>
#include <stdint.h>

kern_return_t mmap_darwin_install_exception_handler(void);
kern_return_t mmap_darwin_uninstall_exception_handler(void);
kern_return_t mmap_darwin_resolve_page(uint64_t page_addr, const void *src, uintptr_t len);
*/
import "C"

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

var darwinVMPageSize = unix.Getpagesize()

type platformState struct {
	stopCh   chan struct{}
	stopOnce sync.Once
}

var darwinMappings struct {
	mu      sync.Mutex
	refs    int
	regions map[*MappedRegion]struct{}
}

func anonMmap(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
}

func munmap(data []byte) error {
	return unix.Munmap(data)
}

func (mr *MappedRegion) platformInit() error {
	mr.stopCh = make(chan struct{})

	if err := unix.Mprotect(mr.data, unix.PROT_NONE); err != nil {
		return fmt.Errorf("mmap: mprotect(PROT_NONE): %w", err)
	}

	darwinMappings.mu.Lock()
	defer darwinMappings.mu.Unlock()

	if darwinMappings.regions == nil {
		darwinMappings.regions = make(map[*MappedRegion]struct{})
	}
	if darwinMappings.refs == 0 {
		if kr := C.mmap_darwin_install_exception_handler(); kr != C.KERN_SUCCESS {
			return darwinMachError("install Mach exception handler", kr)
		}
	}

	darwinMappings.refs++
	darwinMappings.regions[mr] = struct{}{}
	return nil
}

func (mr *MappedRegion) platformSignalStop() {
	mr.stopOnce.Do(func() {
		close(mr.stopCh)
	})
}

func (mr *MappedRegion) platformClose() error {
	needUninstall := false

	darwinMappings.mu.Lock()
	delete(darwinMappings.regions, mr)
	darwinMappings.refs--
	if darwinMappings.refs == 0 {
		needUninstall = true
	}
	darwinMappings.mu.Unlock()

	if needUninstall {
		if kr := C.mmap_darwin_uninstall_exception_handler(); kr != C.KERN_SUCCESS {
			return darwinMachError("uninstall Mach exception handler", kr)
		}
	}
	return nil
}

func (mr *MappedRegion) platformPostInit() error {
	return nil
}

func (mr *MappedRegion) platformSyncDirtyPages() error {
	return nil
}

func (mr *MappedRegion) platformPageSpan(pageAddr uint64) (uint64, int) {
	base := pageAddr &^ uint64(darwinVMPageSize-1)
	remaining := int(uint64(mr.addr) + mr.size - base)
	if remaining < darwinVMPageSize {
		return base, remaining
	}
	return base, darwinVMPageSize
}

func (mr *MappedRegion) platformProtectCleanPage(pageAddr uint64) error {
	base, span := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(base)
	page := mr.data[startIdx*pageSize : startIdx*pageSize+span]
	if err := unix.Mprotect(page, unix.PROT_READ); err != nil {
		return fmt.Errorf("mmap: mprotect(PROT_READ): %w", err)
	}
	return nil
}

func (mr *MappedRegion) platformMarkDirty(pageAddr uint64) error {
	base, span := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(base)
	page := mr.data[startIdx*pageSize : startIdx*pageSize+span]
	if err := unix.Mprotect(page, unix.PROT_READ|unix.PROT_WRITE); err != nil {
		return fmt.Errorf("mmap: mprotect(PROT_READ|PROT_WRITE): %w", err)
	}
	return nil
}

func (mr *MappedRegion) platformPrepareFlush(pageAddr uint64) error {
	return mr.platformProtectCleanPage(pageAddr)
}

func (mr *MappedRegion) platformEvictPage(pageAddr uint64) error {
	base, span := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(base)
	page := mr.data[startIdx*pageSize : startIdx*pageSize+span]
	if err := unix.Mprotect(page, unix.PROT_READ|unix.PROT_WRITE); err != nil {
		return fmt.Errorf("mmap: mprotect(PROT_READ|PROT_WRITE) before evict: %w", err)
	}
	if err := unix.Madvise(page, unix.MADV_DONTNEED); err != nil {
		return fmt.Errorf("mmap: madvise(DONTNEED): %w", err)
	}
	if err := unix.Mprotect(page, unix.PROT_NONE); err != nil {
		return fmt.Errorf("mmap: mprotect(PROT_NONE) after evict: %w", err)
	}
	return nil
}

func (mr *MappedRegion) handleFaults() {
	defer mr.wg.Done()
	<-mr.stopCh
}

func darwinMachError(op string, kr C.kern_return_t) error {
	return fmt.Errorf("mmap: %s: %s", op, C.GoString(C.mach_error_string(kr)))
}

func darwinLookupRegion(addr uint64) *MappedRegion {
	darwinMappings.mu.Lock()
	defer darwinMappings.mu.Unlock()

	for mr := range darwinMappings.regions {
		start := uint64(mr.addr)
		end := start + mr.size
		if addr >= start && addr < end {
			return mr
		}
	}
	return nil
}

func (mr *MappedRegion) resolveFaultDarwin(ctx context.Context, faultAddr uint64) C.kern_return_t {
	pageAddr := faultAddr &^ uint64(pageSize-1)
	if mr.isPageResident(pageAddr) {
		mr.notePageDirty(pageAddr)
		if err := mr.platformMarkDirty(pageAddr); err != nil {
			slog.Error("mmap: mark dirty failed",
				"fault_addr", fmt.Sprintf("0x%x", pageAddr),
				"error", err,
			)
			return C.KERN_FAILURE
		}
		return C.KERN_SUCCESS
	}

	vmBase, vmSpan := mr.platformPageSpan(pageAddr)
	page := make([]byte, vmSpan)
	volOffset := mr.volOff + (vmBase - uint64(mr.addr))
	if _, err := mr.vol.Read(ctx, page, volOffset); err != nil {
		slog.Error("mmap: Read failed, resolving with zeros",
			"fault_addr", fmt.Sprintf("0x%x", vmBase),
			"vol_offset", volOffset,
			"error", err,
		)
	}

	kr := C.mmap_darwin_resolve_page(
		C.uint64_t(vmBase),
		unsafe.Pointer(&page[0]),
		C.uintptr_t(vmSpan),
	)
	if kr != C.KERN_SUCCESS {
		slog.Error("mmap: mach_vm_protect/copy failed",
			"fault_addr", fmt.Sprintf("0x%x", pageAddr),
			"error", C.GoString(C.mach_error_string(kr)),
		)
		return kr
	}
	if err := mr.platformProtectCleanPage(vmBase); err != nil {
		slog.Error("mmap: protect clean page failed",
			"fault_addr", fmt.Sprintf("0x%x", vmBase),
			"error", err,
		)
		return C.KERN_FAILURE
	}
	mr.notePageLoadedClean(vmBase)

	return C.KERN_SUCCESS
}

//export goMmapDarwinHandleFault
func goMmapDarwinHandleFault(faultAddr C.uint64_t, code0 C.int64_t) C.kern_return_t {
	addr := uint64(faultAddr)
	if addr == 0 {
		return C.KERN_FAILURE
	}

	switch code0 {
	case C.int64_t(C.KERN_PROTECTION_FAILURE), C.int64_t(C.KERN_INVALID_ADDRESS):
	default:
		return C.KERN_FAILURE
	}

	mr := darwinLookupRegion(addr)
	if mr == nil {
		return C.KERN_FAILURE
	}

	return mr.resolveFaultDarwin(context.Background(), addr)
}
