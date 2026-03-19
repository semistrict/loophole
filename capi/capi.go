// Package capi exposes loophole volumes as a C-callable static library.
//
// Build with:
//
//	CGO_ENABLED=1 go build -buildmode=c-archive -o libloophole.a ./capi
//
// The resulting libloophole.a + libloophole.h can be linked into Firecracker
// (or any C/Rust program) to serve block I/O from loophole volumes.
//
// API:
//
//	int64_t loophole_open(config_dir, profile, name, name_len) — open a volume
//	int32_t loophole_read(handle, buf, offset, count)          — read bytes
//	int32_t loophole_write(handle, buf, offset, count)         — write bytes
//	int32_t loophole_flush(handle)                             — flush to store
//	uint64_t loophole_size(handle)                             — volume size
//	int32_t loophole_clone(handle, clone_name, clone_name_len) — clone volume
//	int32_t loophole_close(handle)                             — close handle
//
// The handle value is a pointer to the internal state. Read/write use it
// directly with no map lookup. The map exists only to prevent GC and for
// cleanup on close.
package main

// #include <stdint.h>
import "C"
import (
	"context"
	"log/slog"
	"os"
	"sync"
	"unsafe"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage"
	"github.com/semistrict/loophole/volserver"
)

// handle bundles everything needed for a single open volume.
type handle struct {
	vol       *storage.Volume
	manager   *storage.Manager
	diskCache *storage.PageCache
	srv       *volserver.Server
	srvCancel context.CancelFunc
}

// alive keeps handle pointers reachable so the GC doesn't collect them.
// Only touched on open (insert) and close (delete) — never on the data path.
var (
	aliveMu sync.Mutex
	alive   = map[uintptr]*handle{}
)

func ptrToInt64(h *handle) C.int64_t {
	return C.int64_t(uintptr(unsafe.Pointer(h)))
}

func int64ToPtr(id C.int64_t) *handle {
	//nolint:govet // Intentional: handle value IS the pointer, passed through C FFI as int64.
	return (*handle)(unsafe.Pointer(uintptr(id)))
}

// loophole_open opens a volume by name. Each call creates its own store,
// cache, and manager — the returned handle is fully self-contained.
// A volserver is started on a UDS socket so the CLI can discover and
// interact with the volume (status, checkpoint, clone, etc.).
//
// config_dir: path to the config directory (e.g. ~/.loophole), or NULL to
// use the default. The LOOPHOLE_HOME env var overrides the default.
//
// profile: profile name, or NULL to use the default profile.
//
// name / name_len: volume name (required).
//
// Returns a handle (>0) on success, negative on error.
//
//export loophole_open
func loophole_open(config_dir *C.char, profile *C.char, name *C.char, name_len C.uint32_t) C.int64_t {
	var dir env.Dir
	if config_dir != nil {
		dir = env.Dir(C.GoString(config_dir))
	} else {
		dir = env.DefaultDir()
	}

	var profileName string
	if profile != nil {
		profileName = C.GoString(profile)
	}

	cfg, err := env.LoadConfig(dir)
	if err != nil {
		slog.Error("loophole_open: load config", "error", err)
		return -1
	}
	inst, err := cfg.Resolve(profileName)
	if err != nil {
		slog.Error("loophole_open: resolve profile", "error", err)
		return -2
	}

	ctx := context.Background()

	diskCache, err := storage.OpenPageCacheForProfile(dir, inst)
	if err != nil {
		slog.Error("loophole_open: open page cache", "error", err)
		return -3
	}

	manager, err := storage.OpenManagerForProfile(ctx, inst, dir, diskCache)
	if err != nil {
		slog.Error("loophole_open: open manager", "error", err)
		util.SafeClose(diskCache, "close page cache after manager failure")
		return -4
	}

	goName := C.GoStringN(name, C.int(name_len))
	vol, err := manager.OpenVolume(goName)
	if err != nil {
		slog.Error("loophole_open: open volume", "volume", goName, "error", err)
		util.SafeClose(manager, "close manager after open volume failure")
		util.SafeClose(diskCache, "close page cache after open volume failure")
		return -5
	}

	if err := vol.AcquireRef(); err != nil {
		slog.Error("loophole_open: acquire ref", "volume", goName, "error", err)
		util.SafeClose(manager, "close manager after acquire ref failure")
		util.SafeClose(diskCache, "close page cache after acquire ref failure")
		return -6
	}

	// Start a volserver so the CLI can interact with this volume.
	socketPath := dir.VolumeSocket(goName)
	srv, err := volserver.Start(vol, socketPath)
	if err != nil {
		slog.Error("loophole_open: start volserver", "volume", goName, "error", err)
		// Non-fatal: the volume is still usable via the C API, just not
		// discoverable by the CLI. Continue with srv=nil.
		srv = nil
	}
	var srvCancel context.CancelFunc
	if srv != nil {
		var srvCtx context.Context
		srvCtx, srvCancel = context.WithCancel(context.Background())
		go func() {
			if err := srv.Serve(srvCtx); err != nil {
				slog.Warn("volserver exited", "volume", goName, "error", err)
			}
		}()
		slog.Info("loophole_open: volserver started", "volume", goName, "socket", socketPath)
	}

	h := &handle{vol: vol, manager: manager, diskCache: diskCache, srv: srv, srvCancel: srvCancel}

	aliveMu.Lock()
	alive[uintptr(unsafe.Pointer(h))] = h
	aliveMu.Unlock()

	return ptrToInt64(h)
}

// loophole_read reads count bytes from the volume at offset into buf.
// Returns bytes read on success, negative on error.
//
//export loophole_read
func loophole_read(id C.int64_t, buf unsafe.Pointer, offset C.uint64_t, count C.uint32_t) C.int32_t {
	h := int64ToPtr(id)
	slice := unsafe.Slice((*byte)(buf), int(count))
	n, err := h.vol.Read(context.Background(), slice, uint64(offset))
	if err != nil {
		slog.Error("loophole_read failed", "error", err)
		return -2
	}
	return C.int32_t(n)
}

// loophole_write writes count bytes from buf to the volume at offset.
// Returns bytes written on success, negative on error.
//
//export loophole_write
func loophole_write(id C.int64_t, buf unsafe.Pointer, offset C.uint64_t, count C.uint32_t) C.int32_t {
	h := int64ToPtr(id)
	slice := unsafe.Slice((*byte)(buf), int(count))
	if err := h.vol.Write(slice, uint64(offset)); err != nil {
		slog.Error("loophole_write failed", "error", err)
		return -2
	}
	return C.int32_t(count)
}

// loophole_flush flushes the volume to the backing store.
// Returns 0 on success, negative on error.
//
//export loophole_flush
func loophole_flush(id C.int64_t) C.int32_t {
	h := int64ToPtr(id)
	if err := h.vol.Flush(); err != nil {
		slog.Error("loophole_flush failed", "error", err)
		return -2
	}
	return 0
}

// loophole_size returns the volume size in bytes, or 0 if the handle is invalid.
//
//export loophole_size
func loophole_size(id C.int64_t) C.uint64_t {
	h := int64ToPtr(id)
	return C.uint64_t(h.vol.Size())
}

// loophole_clone creates a copy-on-write clone of the volume (includes an
// implicit flush). Returns 0 on success, negative on error.
//
//export loophole_clone
func loophole_clone(id C.int64_t, clone_name *C.char, clone_name_len C.uint32_t) C.int32_t {
	h := int64ToPtr(id)
	goName := C.GoStringN(clone_name, C.int(clone_name_len))
	if err := h.vol.Clone(goName); err != nil {
		slog.Error("loophole_clone failed", "error", err)
		return -2
	}
	return 0
}

// loophole_close closes a volume handle, flushing and releasing resources.
// Returns 0 on success, negative on error.
//
//export loophole_close
func loophole_close(id C.int64_t) C.int32_t {
	h := int64ToPtr(id)

	aliveMu.Lock()
	delete(alive, uintptr(unsafe.Pointer(h)))
	aliveMu.Unlock()

	// Stop the volserver first — its shutdown flushes and releases the vol ref.
	if h.srv != nil {
		h.srvCancel()
		h.srv.Close()
	} else {
		// No volserver — flush and release manually.
		if err := h.vol.Flush(); err != nil {
			slog.Error("loophole_close: flush", "error", err)
		}
		if err := h.vol.ReleaseRef(); err != nil {
			slog.Error("loophole_close: release ref", "error", err)
		}
	}

	var ret C.int32_t
	if err := h.manager.Close(); err != nil {
		slog.Error("loophole_close: close manager", "error", err)
		ret = -3
	}
	if err := h.diskCache.Close(); err != nil {
		slog.Error("loophole_close: close page cache", "error", err)
		if ret == 0 {
			ret = -4
		}
	}
	return ret
}

// loophole_strerror returns a human-readable string for an error code.
// The caller must free the returned string with free().
//
//export loophole_strerror
func loophole_strerror(code C.int32_t) *C.char {
	msgs := map[C.int32_t]string{
		0:  "success",
		-1: "invalid handle or missing argument",
		-2: "operation failed",
		-3: "page cache initialization failed",
		-4: "manager initialization failed",
		-5: "volume open failed",
		-6: "volume ref acquisition failed",
	}
	if s, ok := msgs[code]; ok {
		return C.CString(s)
	}
	return C.CString("unknown error")
}

func init() {
	if os.Getenv("LOOPHOLE_CAPI_LOG") != "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
}

func main() {}
