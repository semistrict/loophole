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
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/storage2"
)

// --- handle registry ---

var (
	volHandles    sync.Map // int64 → loophole.Volume
	nextVolHandle atomic.Int64
	globalVM      loophole.VolumeManager
	globalStore   loophole.ObjectStore
	initOnce      sync.Once
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

// InitConfig is the JSON-decoded configuration for loophole_init.
type InitConfig struct {
	// S3 store settings (used when LocalDir is empty).
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`

	// Local file store (mutually exclusive with S3).
	LocalDir string `json:"local_dir"`

	// Local cache directory for layer data.
	CacheDir string `json:"cache_dir"`
}

// loophole_init initializes the loophole runtime. Must be called once before
// any other loophole_* function. config_json is a UTF-8 JSON string of length
// config_len. Returns 0 on success, negative on error.
//
//export loophole_init
func loophole_init(config_json *C.char, config_len C.uint32_t) C.int32_t {
	var retErr C.int32_t
	initOnce.Do(func() {
		buf := C.GoBytes(unsafe.Pointer(config_json), C.int(config_len))
		var cfg InitConfig
		if err := json.Unmarshal(buf, &cfg); err != nil {
			retErr = -1
			return
		}

		ctx := context.Background()
		var store loophole.ObjectStore
		if cfg.LocalDir != "" {
			var err error
			store, err = loophole.NewFileStore(cfg.LocalDir)
			if err != nil {
				retErr = -2
				return
			}
		} else {
			inst := loophole.Instance{
				Bucket:    cfg.Bucket,
				Prefix:    cfg.Prefix,
				Endpoint:  cfg.Endpoint,
				Region:    cfg.Region,
				AccessKey: cfg.AccessKey,
				SecretKey: cfg.SecretKey,
			}
			var err error
			store, err = loophole.NewS3Store(ctx, inst)
			if err != nil {
				retErr = -3
				return
			}
		}

		cacheDir := cfg.CacheDir
		if cacheDir == "" {
			cacheDir = "/tmp/loophole-cache"
		}

		globalStore = store
		globalVM = storage2.NewVolumeManager(store, cacheDir, storage2.Config{}, nil, nil)
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
		return -2
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
	if globalVM == nil {
		return 0
	}
	if err := globalVM.Close(context.Background()); err != nil {
		return -1
	}
	globalVM = nil
	return 0
}

// loophole_strerror returns a human-readable error string for the last
// operation on the given handle. Currently returns a generic message;
// can be extended with per-handle error tracking if needed.
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
		return C.CString("S3 store initialization failed")
	default:
		return C.CString(fmt.Sprintf("unknown error %d", int(code)))
	}
}

func main() {}
