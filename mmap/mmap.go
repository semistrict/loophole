// Package mmap provides demand-paged memory mappings backed by loophole
// volumes. When user code reads an address in the mapped region, a page
// fault is intercepted by a platform-specific mechanism (userfaultfd on
// Linux, Mach exception ports on macOS), the corresponding 4KB page is
// read from the volume, and the fault is resolved.
//
// GOMAXPROCS must be >= 2 so handler goroutines can run while a faulting
// goroutine is suspended in the kernel.
package mmap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

const pageSize = 4096

const (
	defaultMaxResidentPages = 256
	backgroundFlushInterval = 50 * time.Millisecond
)

// ErrNotSupported is returned on platforms that do not support demand-paged
// mappings.
var ErrNotSupported = errors.New("mmap: not supported on this platform")

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

type config struct {
	readahead        int
	workers          int
	maxResidentPages int
	backend          Backend
}

// Backend selects the runtime fault/write-barrier implementation.
type Backend string

const (
	BackendAuto      Backend = "auto"
	BackendMissing   Backend = "missing"
	BackendMissingWP Backend = "missing+wp"
)

// Option configures MapVolume behavior.
type Option func(*config)

// WithReadahead sets the number of extra pages to prefetch after each fault.
func WithReadahead(pages int) Option {
	return func(c *config) { c.readahead = pages }
}

// WithWorkers sets the number of handler goroutines (default 1).
func WithWorkers(n int) Option {
	return func(c *config) { c.workers = n }
}

// WithMaxResidentPages bounds the number of resident mmap pages kept in memory
// before clean pages are evicted and faulted back in on demand.
func WithMaxResidentPages(n int) Option {
	return func(c *config) { c.maxResidentPages = n }
}

// WithBackend overrides the runtime backend selection. The default is auto,
// which resolves to missing+wp when the kernel supports UFFD write-protect
// and otherwise falls back to missing.
func WithBackend(backend Backend) Option {
	return func(c *config) { c.backend = backend }
}

type pageState struct {
	resident bool
	dirty    bool
	flushing bool
}

// ---------------------------------------------------------------------------
// MappedRegion
// ---------------------------------------------------------------------------

// MappedRegion is a demand-paged memory region backed by a loophole volume.
// The platformState embedded struct holds platform-specific file descriptors
// and handles.
type MappedRegion struct {
	data   []byte
	addr   uintptr
	size   uint64
	volOff uint64
	vol    loophole.Volume
	direct bool
	wg     sync.WaitGroup
	once   sync.Once

	cfg      config
	backend  Backend
	pages    []pageState
	pageMu   sync.Mutex
	flushMu  sync.Mutex
	closeCh  chan struct{}
	flushCh  chan struct{}
	resident int

	platformState // platform-specific fields (mmap_linux.go / mmap_darwin.go)
}

// Bytes returns the mapped region as a Go byte slice.
// Accessing elements of the slice may trigger page faults that are resolved
// by reading from the underlying volume.
func (mr *MappedRegion) Bytes() []byte { return mr.data }

// Ptr returns the base address of the mapped region.
func (mr *MappedRegion) Ptr() unsafe.Pointer { return unsafe.Pointer(mr.addr) }

// Backend returns the selected runtime backend for this mapping.
func (mr *MappedRegion) Backend() Backend { return mr.backend }

// Size returns the size of the mapped region in bytes.
func (mr *MappedRegion) Size() uint64 { return mr.size }

// Flush writes all dirty mmap pages back to the volume.
func (mr *MappedRegion) Flush() error {
	if mr.backend == BackendMissing {
		return nil
	}
	mr.flushMu.Lock()
	defer mr.flushMu.Unlock()

	for {
		if err := mr.platformSyncDirtyPages(); err != nil {
			return err
		}
		work := mr.beginFlushPass()
		if len(work) == 0 {
			return nil
		}
		if err := mr.flushPages(context.Background(), work); err != nil {
			return err
		}
	}
}

// Close unmaps the region, stops handler goroutines, and releases the volume
// reference. Safe to call multiple times.
func (mr *MappedRegion) Close() error {
	var err error
	mr.once.Do(func() {
		err = mr.close()
	})
	return err
}

func (mr *MappedRegion) close() error {
	var errs []error

	if mr.backend != BackendMissing {
		if err := mr.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush before close: %w", err))
		}
	}

	// Signal handlers to stop and wait for exit.
	close(mr.closeCh)
	mr.platformSignalStop()
	mr.wg.Wait()

	// Platform-specific cleanup (close fds, etc).
	if err := mr.platformClose(); err != nil {
		errs = append(errs, err)
	}

	// Unmap the anonymous region.
	if err := munmap(mr.data); err != nil {
		errs = append(errs, fmt.Errorf("munmap: %w", err))
	}

	// Release volume reference.
	if err := mr.vol.DisableDirectWriteback(); err != nil {
		errs = append(errs, fmt.Errorf("disable direct writeback: %w", err))
	}
	if err := mr.vol.ReleaseRef(); err != nil {
		errs = append(errs, fmt.Errorf("ReleaseRef: %w", err))
	}

	return errors.Join(errs...)
}

// readPage reads a single page from the volume into buf. buf must be at
// least pageSize bytes. This is the shared I/O path used by all platform
// fault handlers.
func (mr *MappedRegion) readPage(ctx context.Context, buf []byte, faultAddr uint64) error {
	volOffset := mr.volOff + (faultAddr - uint64(mr.addr))
	_, err := mr.vol.Read(ctx, buf[:pageSize], volOffset)
	return err
}

func (mr *MappedRegion) pageIndexFromAddr(pageAddr uint64) int {
	return int((pageAddr - uint64(mr.addr)) / pageSize)
}

func (mr *MappedRegion) pageAddr(idx int) uint64 {
	return uint64(mr.addr) + uint64(idx)*pageSize
}

func (mr *MappedRegion) pageSlice(idx int) []byte {
	off := idx * pageSize
	return mr.data[off : off+pageSize]
}

func (mr *MappedRegion) notePageLoadedClean(pageAddr uint64) {
	baseAddr, spanBytes := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(baseAddr)
	count := spanBytes / pageSize

	mr.pageMu.Lock()
	for i := 0; i < count; i++ {
		idx := startIdx + i
		if !mr.pages[idx].resident {
			mr.pages[idx].resident = true
			mr.resident++
		}
		mr.pages[idx].dirty = false
	}
	mr.pageMu.Unlock()

	mr.trimResidentPages()
}

func (mr *MappedRegion) notePageDirty(pageAddr uint64) {
	baseAddr, spanBytes := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(baseAddr)
	count := spanBytes / pageSize
	shouldWake := false

	mr.pageMu.Lock()
	for i := 0; i < count; i++ {
		idx := startIdx + i
		if !mr.pages[idx].resident {
			mr.pages[idx].resident = true
			mr.resident++
		}
		if !mr.pages[idx].dirty {
			mr.pages[idx].dirty = true
			shouldWake = true
		}
	}
	mr.pageMu.Unlock()

	if shouldWake {
		select {
		case mr.flushCh <- struct{}{}:
		default:
		}
	}
}

func (mr *MappedRegion) isPageResident(pageAddr uint64) bool {
	baseAddr, spanBytes := mr.platformPageSpan(pageAddr)
	startIdx := mr.pageIndexFromAddr(baseAddr)
	count := spanBytes / pageSize
	mr.pageMu.Lock()
	defer mr.pageMu.Unlock()
	for i := 0; i < count; i++ {
		if mr.pages[startIdx+i].resident {
			return true
		}
	}
	return false
}

func (mr *MappedRegion) beginFlushPass() []int {
	mr.pageMu.Lock()
	defer mr.pageMu.Unlock()

	var work []int
	for idx := range mr.pages {
		if !mr.pages[idx].dirty || mr.pages[idx].flushing {
			continue
		}
		mr.pages[idx].dirty = false
		mr.pages[idx].flushing = true
		work = append(work, idx)
	}
	return work
}

func (mr *MappedRegion) flushPages(ctx context.Context, work []int) error {
	if mr.direct {
		dirtyPages := make([]loophole.DirectPage, 0, len(work))
		prepared := make([]int, 0, len(work))
		for _, idx := range work {
			pageAddr := mr.pageAddr(idx)
			if err := mr.platformPrepareFlush(pageAddr); err != nil {
				for _, preparedIdx := range prepared {
					restoreAddr := mr.pageAddr(preparedIdx)
					_ = mr.platformMarkDirty(restoreAddr)
					mr.restoreDirtyPage(preparedIdx)
				}
				mr.restoreDirtyPage(idx)
				return err
			}
			prepared = append(prepared, idx)
			dirtyPages = append(dirtyPages, loophole.DirectPage{
				Offset: mr.volOff + uint64(idx)*pageSize,
				Data:   append([]byte(nil), mr.pageSlice(idx)...),
			})
		}

		if err := mr.vol.WritePagesDirect(dirtyPages); err != nil {
			for _, idx := range prepared {
				pageAddr := mr.pageAddr(idx)
				_ = mr.platformMarkDirty(pageAddr)
				mr.restoreDirtyPage(idx)
			}
			return fmt.Errorf("direct write pages: %w", err)
		}

		for _, idx := range prepared {
			_ = mr.finishFlushPage(idx)
		}
		mr.trimResidentPages()
		return nil
	}

	for _, idx := range work {
		pageAddr := mr.pageAddr(idx)
		if err := mr.platformPrepareFlush(pageAddr); err != nil {
			mr.restoreDirtyPage(idx)
			return err
		}

		snapshot := append([]byte(nil), mr.pageSlice(idx)...)
		if err := mr.vol.Write(snapshot, mr.volOff+uint64(idx)*pageSize); err != nil {
			_ = mr.platformMarkDirty(pageAddr)
			mr.restoreDirtyPage(idx)
			return fmt.Errorf("write page %d: %w", idx, err)
		}

		clean := mr.finishFlushPage(idx)
		if !clean {
			continue
		}
	}

	mr.trimResidentPages()
	return nil
}

func (mr *MappedRegion) restoreDirtyPage(idx int) {
	mr.pageMu.Lock()
	mr.pages[idx].flushing = false
	mr.pages[idx].dirty = true
	mr.pageMu.Unlock()
}

func (mr *MappedRegion) finishFlushPage(idx int) bool {
	mr.pageMu.Lock()
	defer mr.pageMu.Unlock()

	mr.pages[idx].flushing = false
	return !mr.pages[idx].dirty
}

func (mr *MappedRegion) trimResidentPages() {
	if mr.backend == BackendMissing {
		return
	}
	if mr.cfg.maxResidentPages <= 0 {
		return
	}
	if err := mr.platformSyncDirtyPages(); err != nil {
		slog.Error("mmap: sync dirty pages before trim failed", "error", err)
		return
	}

	for {
		idx := -1

		mr.pageMu.Lock()
		if mr.resident <= mr.cfg.maxResidentPages {
			mr.pageMu.Unlock()
			return
		}
		for i := range mr.pages {
			baseAddr, spanBytes := mr.platformPageSpan(mr.pageAddr(i))
			startIdx := mr.pageIndexFromAddr(baseAddr)
			count := spanBytes / pageSize
			if startIdx != i {
				continue
			}
			canEvict := true
			for j := 0; j < count; j++ {
				page := mr.pages[startIdx+j]
				if !page.resident || page.dirty || page.flushing {
					canEvict = false
					break
				}
			}
			if !canEvict {
				continue
			}
			for j := 0; j < count; j++ {
				mr.pages[startIdx+j].resident = false
				mr.resident--
			}
			idx = i
			break
		}
		mr.pageMu.Unlock()

		if idx < 0 {
			return
		}
		if err := mr.platformEvictPage(mr.pageAddr(idx)); err != nil {
			slog.Error("mmap: evict page failed", "page", idx, "error", err)
			mr.pageMu.Lock()
			if !mr.pages[idx].resident {
				mr.pages[idx].resident = true
				mr.resident++
			}
			mr.pageMu.Unlock()
			return
		}
	}
}

func (mr *MappedRegion) backgroundFlushLoop() {
	defer mr.wg.Done()

	ticker := time.NewTicker(backgroundFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mr.closeCh:
			return
		case <-mr.flushCh:
		case <-ticker.C:
		}

		if err := mr.Flush(); err != nil {
			slog.Error("mmap: background flush failed", "error", err)
		}
	}
}

// MapVolume creates an anonymous memory region of size bytes backed by the
// volume starting at offset. Reads from the returned region trigger page
// faults that are resolved by reading from the volume.
//
// Both offset and size must be page-aligned (4096).
func MapVolume(vol loophole.Volume, offset, size uint64, opts ...Option) (*MappedRegion, error) {
	if offset%pageSize != 0 {
		return nil, fmt.Errorf("mmap: offset %d is not page-aligned", offset)
	}
	if size%pageSize != 0 || size == 0 {
		return nil, fmt.Errorf("mmap: size %d is not page-aligned or is zero", size)
	}
	if runtime.GOMAXPROCS(0) < 2 {
		return nil, fmt.Errorf("mmap: GOMAXPROCS must be >= 2 (currently %d)", runtime.GOMAXPROCS(0))
	}

	cfg := config{
		workers:          1,
		maxResidentPages: defaultMaxResidentPages,
		backend:          backendFromEnv(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.maxResidentPages < 1 {
		cfg.maxResidentPages = defaultMaxResidentPages
	}

	// Acquire a volume reference.
	if err := vol.AcquireRef(); err != nil {
		return nil, fmt.Errorf("mmap: AcquireRef: %w", err)
	}
	directEnabled := false
	releaseOnErr := true
	defer func() {
		if releaseOnErr {
			if directEnabled {
				_ = vol.DisableDirectWriteback()
			}
			_ = vol.ReleaseRef()
		}
	}()

	// Create anonymous mapping.
	data, err := anonMmap(int(size))
	if err != nil {
		return nil, fmt.Errorf("mmap: mmap: %w", err)
	}
	addr := uintptr(unsafe.Pointer(&data[0]))

	mr := &MappedRegion{
		data:    data,
		addr:    addr,
		size:    size,
		volOff:  offset,
		vol:     vol,
		cfg:     cfg,
		backend: cfg.backend,
		pages:   make([]pageState, size/pageSize),
		closeCh: make(chan struct{}),
		flushCh: make(chan struct{}, 1),
	}

	// Platform-specific setup (userfaultfd on Linux, Mach exception ports on macOS).
	if err := mr.platformInit(); err != nil {
		munmap(data)
		return nil, err
	}

	slog.Info("mmap: MapVolume initialized",
		"backend", string(mr.backend),
		"size", size,
		"pages", size/pageSize,
		"maxResident", cfg.maxResidentPages,
		"direct", true,
		"addr", fmt.Sprintf("%p", unsafe.Pointer(addr)),
	)

	if err := vol.EnableDirectWriteback(); err != nil {
		util.SafeClose(mr, "mmap: close after direct writeback init failure")
		return nil, fmt.Errorf("mmap: enable direct writeback: %w", err)
	}
	directEnabled = true
	if mr.backend != BackendMissing {
		mr.direct = true
	}

	// Install OnBeforeClose hook as safety net.
	vol.OnBeforeClose(func() {
		if err := mr.Close(); err != nil {
			slog.Error("mmap: safety-net close failed", "error", err)
		}
	})

	// Start handler goroutines.
	for range cfg.workers {
		mr.wg.Add(1)
		go mr.handleFaults()
	}
	if mr.direct {
		mr.wg.Add(1)
		go mr.backgroundFlushLoop()
	}

	if err := mr.platformPostInit(); err != nil {
		util.SafeClose(mr, "mmap: close after post-init failure")
		return nil, err
	}

	releaseOnErr = false
	return mr, nil
}

func backendFromEnv() Backend {
	value := os.Getenv("LOOPHOLE_MMAP_BACKEND")
	switch Backend(value) {
	case "", BackendAuto:
		return BackendAuto
	case BackendMissing:
		return BackendMissing
	case BackendMissingWP:
		return BackendMissingWP
	default:
		slog.Warn("mmap: ignoring unknown backend override", "backend", value)
		return BackendAuto
	}
}
