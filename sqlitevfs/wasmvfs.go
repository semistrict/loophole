//go:build js

package sqlitevfs

import (
	"context"
	"fmt"
	"log/slog"
	"syscall/js"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/jsutil"
)

// WasmVFS exposes a loophole Volume as a SQLite VFS to JavaScript (wa-sqlite).
// The JS VFS is just a bridge; file behavior is shared with the native CVFS core.
type WasmVFS struct {
	core *fileCore
}

// NewWasmVFS creates a WasmVFS and registers it on globalThis for JS access.
func NewWasmVFS(ctx context.Context, vol loophole.Volume, name string, syncMode SyncMode) (*WasmVFS, error) {
	core, err := newFileCore(ctx, vol, syncMode)
	if err != nil {
		return nil, fmt.Errorf("open WasmVFS: %w", err)
	}

	v := &WasmVFS{core: core}
	v.registerJS(name)

	h := core.HeaderSnapshot()
	slog.Info("wasmvfs: registered", "name", name, "mainDBSize", h.MainDBSize, "walSize", h.WALSize)
	return v, nil
}

// FlushHeader persists WAL and writes the header if dirty.
func (v *WasmVFS) FlushHeader() error {
	return v.core.FlushHeader()
}

func (v *WasmVFS) registerJS(name string) {
	registry := js.Global().Get("__loophole_vfs")
	if registry.IsUndefined() || registry.IsNull() {
		registry = js.Global().Get("Object").New()
		js.Global().Set("__loophole_vfs", registry)
	}

	vfsObj := js.Global().Get("Object").New()

	vfsObj.Set("xOpen", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		filename := args[1].String()
		flags := args[2].Int()

		_, outFlags, err := v.core.OpenFile(fileID, filename, flags)
		if err != nil {
			return nil, err
		}
		return outFlags, nil
	}))

	vfsObj.Set("xClose", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		if err := v.core.CloseFile(fileID); err != nil {
			slog.Warn("wasmvfs: close flush error", "error", err)
		}
		return nil, nil
	}))

	vfsObj.Set("xRead", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		offset := int64(args[1].Int())
		length := args[2].Int()

		buf := make([]byte, length)
		shortRead, err := v.core.ReadFile(fileID, buf, offset)
		if err != nil {
			return nil, err
		}
		if offset == 0 {
			h := v.core.HeaderSnapshot()
			// Native SQLite leaves the database header in WAL mode even when
			// the persisted WAL payload is empty. wa-sqlite's JS VFS surface
			// does not expose SHM callbacks, so present such snapshots as a
			// plain rollback-journal database to the WASM reader.
			if h.WALSize == 0 && len(buf) >= 20 && buf[18] == 2 && buf[19] == 2 {
				buf[18] = 1
				buf[19] = 1
			}
		}

		result := js.Global().Get("Object").New()
		result.Set("data", jsutil.JSBytes(buf))
		result.Set("shortRead", shortRead)
		return result, nil
	}))

	vfsObj.Set("xWrite", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		data := jsutil.GoBytes(args[1])
		offset := int64(args[2].Int())
		return nil, v.core.WriteFile(fileID, data, offset)
	}))

	vfsObj.Set("xFileSize", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		return v.core.FileSize(fileID)
	}))

	vfsObj.Set("xTruncate", jsutil.Async(func(args []js.Value) (any, error) {
		fileID := args[0].Int()
		size := int64(args[1].Int())
		return nil, v.core.TruncateFile(fileID, size)
	}))

	vfsObj.Set("xSync", jsutil.Async(func(args []js.Value) (any, error) {
		return nil, v.core.SyncFile()
	}))

	vfsObj.Set("xDelete", jsutil.Async(func(args []js.Value) (any, error) {
		filename := args[0].String()
		return nil, v.core.DeleteFile(filename)
	}))

	vfsObj.Set("xAccess", jsutil.Async(func(args []js.Value) (any, error) {
		filename := args[0].String()
		flags := 0
		if len(args) > 1 {
			flags = args[1].Int()
		}
		return v.core.AccessFile(filename, flags), nil
	}))

	registry.Set(name, vfsObj)
}
