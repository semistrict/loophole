// Package capi exposes loophole volumes as a C-callable static library.
//
// Build with:
//
//	CGO_ENABLED=1 go build -buildmode=c-archive -o libloophole.a ./capi
//
// The resulting libloophole.a + libloophole.h can be linked into Firecracker
// (or any C/Rust program) to serve block I/O from loophole volumes.
package main

// #include <stdint.h>
import "C"
import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/daemon"
	"github.com/semistrict/loophole/mmap"
	"github.com/semistrict/loophole/storage2"
)

// --- handle registry ---

var (
	volHandles    sync.Map // int64 → loophole.Volume
	nextVolHandle atomic.Int64
	globalVM      loophole.VolumeManager
	globalCache   *storage2.PageCache
	embedCleanup  func() // stops the embedded daemon server
	initOnce      sync.Once
	mmapRegions   sync.Map // uintptr → *mmap.MappedRegion
)

func storeHandle(vol loophole.Volume) int64 {
	h := nextVolHandle.Add(1)
	volHandles.Store(h, vol)
	return h
}

func loadHandle(h int64) loophole.Volume {
	v, ok := volHandles.Load(h)
	if !ok {
		return nil
	}
	return v.(loophole.Volume)
}

func deleteHandle(h int64) {
	volHandles.Delete(h)
}

// --- C API ---

// loophole_init initializes the loophole runtime from ~/.loophole/config.toml
// (or the file at LOOPHOLE_CONFIG_FILE env var). Must be called once before
// any other loophole_* function.
//
// config_path: path to config.toml directory, or NULL to use the default
// (~/.loophole). If LOOPHOLE_CONFIG_DIR env var is set, it takes precedence
// over the default but not over an explicit config_path argument.
//
// profile: profile name, or NULL to use the default profile.
// If LOOPHOLE_PROFILE env var is set, it takes precedence over the default
// but not over an explicit profile argument.
//
// Returns 0 on success, negative on error.
//
//export loophole_init
func loophole_init(config_path *C.char, profile *C.char) C.int32_t {
	var retErr C.int32_t
	initOnce.Do(func() {
		// Determine config directory.
		var dir loophole.Dir
		if config_path != nil {
			dir = loophole.Dir(C.GoString(config_path))
		} else if envDir := os.Getenv("LOOPHOLE_CONFIG_DIR"); envDir != "" {
			dir = loophole.Dir(envDir)
		} else {
			dir = loophole.DefaultDir()
		}

		// Determine profile name.
		var profileName string
		if profile != nil {
			profileName = C.GoString(profile)
		} else if envProfile := os.Getenv("LOOPHOLE_PROFILE"); envProfile != "" {
			profileName = envProfile
		}

		// Load config and resolve profile.
		cfg, err := loophole.LoadConfig(dir)
		if err != nil {
			slog.Error("loophole_init: load config", "error", err)
			retErr = -1
			return
		}
		inst, err := cfg.Resolve(profileName)
		if err != nil {
			slog.Error("loophole_init: resolve profile", "error", err)
			retErr = -2
			return
		}

		// Create object store.
		ctx := context.Background()
		var store loophole.ObjectStore
		if inst.LocalDir != "" {
			store, err = loophole.NewFileStore(inst.LocalDir)
			if err != nil {
				slog.Error("loophole_init: create file store", "error", err)
				retErr = -3
				return
			}
		} else {
			store, err = loophole.NewS3Store(ctx, inst)
			if err != nil {
				slog.Error("loophole_init: create S3 store", "error", err)
				retErr = -4
				return
			}
		}

		// Set up cache directory and page cache (same as daemon).
		cacheDir := dir.Cache(inst.ProfileName)
		diskCache, err := storage2.NewPageCache(filepath.Join(cacheDir, "diskcache"))
		if err != nil {
			slog.Error("loophole_init: create page cache", "error", err)
			retErr = -5
			return
		}

		globalCache = diskCache
		globalVM = storage2.NewVolumeManager(store, cacheDir, storage2.Config{}, nil, diskCache)

		// Start the embedded daemon so the loophole CLI can connect via --pid.
		cleanup, daemonErr := daemon.StartEmbedded(globalVM, diskCache, inst)
		if daemonErr != nil {
			slog.Error("loophole_init: embedded daemon failed (non-fatal)", "error", daemonErr)
		} else {
			embedCleanup = cleanup
			slog.Info("loophole_init: embedded daemon started", "socket", daemon.EmbedSocketPath(os.Getpid()))
		}

		slog.Info("loophole_init: initialized", "store", inst.URL(), "profile", inst.ProfileName)
	})
	return retErr
}

// loophole_create creates a new volume with the given name and size in bytes.
// Returns a handle (>0) on success or a negative error code.
//
//export loophole_create
func loophole_create(name *C.char, name_len C.uint32_t, size C.uint64_t) C.int64_t {
	if globalVM == nil {
		return -1
	}
	goName := C.GoStringN(name, C.int(name_len))
	vol, err := globalVM.NewVolume(loophole.CreateParams{
		Volume:   goName,
		Size:     uint64(size),
		NoFormat: true,
	})
	if err != nil {
		return -2
	}
	if err := vol.AcquireRef(); err != nil {
		return -3
	}
	return C.int64_t(storeHandle(vol))
}

// loophole_open opens a volume by name and returns a handle (>0) on success
// or a negative error code.
//
//export loophole_open
func loophole_open(name *C.char, name_len C.uint32_t) C.int64_t {
	if globalVM == nil {
		return -1
	}
	goName := C.GoStringN(name, C.int(name_len))
	vol, err := globalVM.OpenVolume(goName)
	if err != nil {
		slog.Error("loophole_open failed", "volume", goName, "error", err)
		return -2
	}
	// AcquireRef so each loophole_open/loophole_close pair is balanced.
	// OpenVolume may return a cached volume whose initial ref belongs to the
	// Manager, so we need our own ref to prevent premature destruction.
	if err := vol.AcquireRef(); err != nil {
		return -3
	}
	return C.int64_t(storeHandle(vol))
}

// loophole_read reads count bytes from the volume at the given byte offset
// into buf. Returns bytes read (== count) on success, negative on error.
//
//export loophole_read
func loophole_read(handle C.int64_t, buf unsafe.Pointer, offset C.uint64_t, count C.uint32_t) C.int32_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return -1
	}
	slice := unsafe.Slice((*byte)(buf), int(count))
	n, err := vol.Read(context.Background(), slice, uint64(offset))
	if err != nil {
		return -2
	}
	return C.int32_t(n)
}

// loophole_write writes count bytes from buf to the volume at the given byte
// offset. Returns bytes written (== count) on success, negative on error.
//
//export loophole_write
func loophole_write(handle C.int64_t, buf unsafe.Pointer, offset C.uint64_t, count C.uint32_t) C.int32_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return -1
	}
	slice := unsafe.Slice((*byte)(buf), int(count))
	if err := vol.Write(slice, uint64(offset)); err != nil {
		return -2
	}
	return C.int32_t(count)
}

// loophole_flush flushes the volume. Returns 0 on success, negative on error.
//
//export loophole_flush
func loophole_flush(handle C.int64_t) C.int32_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return -1
	}
	if err := vol.Flush(); err != nil {
		return -2
	}
	return 0
}

// loophole_clone clones a volume, creating a writable fork. The clone
// includes an implicit flush. Returns a handle to the new clone volume
// on success, or a negative error code.
//
//export loophole_clone
func loophole_clone(handle C.int64_t, cloneName *C.char, cloneNameLen C.uint32_t) C.int64_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return -1
	}
	goCloneName := C.GoStringN(cloneName, C.int(cloneNameLen))
	cloneVol, err := vol.Clone(goCloneName)
	if err != nil {
		slog.Error("loophole_clone failed", "error", err)
		return -2
	}
	// Release our ref so the clone volume isn't held open by this process.
	// The restoring process will open it fresh with its own lease.
	if err := cloneVol.ReleaseRef(); err != nil {
		slog.Error("loophole_clone: release clone ref failed", "error", err)
	}
	return 0
}

// loophole_size returns the volume size in bytes.
//
//export loophole_size
func loophole_size(handle C.int64_t) C.uint64_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return 0
	}
	return C.uint64_t(vol.Size())
}

// loophole_close closes a volume handle. Returns 0 on success.
//
//export loophole_close
func loophole_close(handle C.int64_t) C.int32_t {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return -1
	}
	deleteHandle(int64(handle))
	if err := vol.ReleaseRef(); err != nil {
		return -2
	}
	return 0
}

// loophole_shutdown shuts down the loophole runtime. Returns 0 on success.
//
//export loophole_shutdown
func loophole_shutdown() C.int32_t {
	if embedCleanup != nil {
		embedCleanup()
		embedCleanup = nil
	}
	if globalVM == nil {
		return 0
	}
	if err := globalVM.Close(context.Background()); err != nil {
		return -1
	}
	globalVM = nil
	if globalCache != nil {
		if err := globalCache.Close(); err != nil {
			return -2
		}
		globalCache = nil
	}
	return 0
}

// loophole_strerror returns a human-readable error string for the given code.
//
//export loophole_strerror
func loophole_strerror(code C.int32_t) *C.char {
	switch code {
	case 0:
		return C.CString("success")
	case -1:
		return C.CString("invalid handle or not initialized")
	case -2:
		return C.CString("operation failed")
	case -3:
		return C.CString("file store initialization failed")
	case -4:
		return C.CString("S3 store initialization failed")
	case -5:
		return C.CString("page cache initialization failed")
	default:
		return C.CString(fmt.Sprintf("unknown error %d", int(code)))
	}
}

// loophole_mmap creates a demand-paged memory mapping of the volume starting
// at the given byte offset, for size bytes. Both offset and size must be
// page-aligned (4096). Returns a pointer to the mapped region on success,
// or NULL on error (including non-Linux platforms).
//
//export loophole_mmap
func loophole_mmap(handle C.int64_t, offset C.uint64_t, size C.uint64_t) unsafe.Pointer {
	vol := loadHandle(int64(handle))
	if vol == nil {
		return nil
	}
	mr, err := mmap.MapVolume(vol, uint64(offset), uint64(size))
	if err != nil {
		slog.Error("loophole_mmap failed", "error", err)
		return nil
	}
	ptr := mr.Ptr()
	mmapRegions.Store(uintptr(ptr), mr)
	return ptr
}

// loophole_munmap unmaps a region previously returned by loophole_mmap.
// Returns 0 on success, -1 on error.
//
//export loophole_munmap
func loophole_munmap(ptr unsafe.Pointer, size C.uint64_t) C.int32_t {
	key := uintptr(ptr)
	v, ok := mmapRegions.LoadAndDelete(key)
	if !ok {
		return -1
	}
	mr := v.(*mmap.MappedRegion)
	if err := mr.Close(); err != nil {
		slog.Error("loophole_munmap failed", "error", err)
		return -1
	}
	return 0
}

func main() {}
