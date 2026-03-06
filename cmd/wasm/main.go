//go:build js

package main

import (
	"context"
	"fmt"
	"syscall/js"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/jsutil"
	"github.com/semistrict/loophole/juicefs"
	"github.com/semistrict/loophole/lsm"
)

var backend fsbackend.Service

func main() {
	fmt.Println("loophole wasm: initializing")

	s3 := jsutil.MustGetS3()
	store := jsutil.NewJSObjectStore(s3, "")

	vm := lsm.NewVolumeManager(store, "", lsm.Config{}, nil, nil)
	backend = fsbackend.NewBackend(vm, map[string]fsbackend.AnyDriver{
		loophole.VolumeTypeJuiceFS: juicefs.NewInProcessDriver(juicefs.Config{ObjStore: store}),
	})

	api := js.Global().Get("Object").New()

	api.Set("create", jsutil.Async(func(args []js.Value) (any, error) {
		name := jsutil.MustString(args, 0)
		size := uint64(jsutil.MustInt(args, 1))
		if size == 0 {
			size = 256 * 1024 * 1024 // 256 MB default
		}
		fmt.Println("wasm: create called", name, size)
		err := backend.Create(context.Background(), loophole.CreateParams{
			Volume: name,
			Size:   size,
			Type:   loophole.VolumeTypeJuiceFS,
		})
		if err != nil {
			fmt.Println("wasm: create error:", err)
		} else {
			fmt.Println("wasm: create done")
		}
		return nil, err
	}))

	api.Set("mount", jsutil.Async(func(args []js.Value) (any, error) {
		volume := jsutil.MustString(args, 0)
		mountpoint := jsutil.MustString(args, 1)
		if mountpoint == "" {
			mountpoint = volume
		}
		return nil, backend.Mount(context.Background(), volume, mountpoint)
	}))

	api.Set("unmount", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		return nil, backend.Unmount(context.Background(), mountpoint)
	}))

	api.Set("writeFile", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		path := jsutil.MustString(args, 1)
		data := jsutil.MustBytes(args, 2)
		fs, err := backend.FS(mountpoint)
		if err != nil {
			return nil, err
		}
		return nil, fs.WriteFile(path, data, 0o644)
	}))

	api.Set("readFile", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		path := jsutil.MustString(args, 1)
		fs, err := backend.FS(mountpoint)
		if err != nil {
			return nil, err
		}
		data, err := fs.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return jsutil.JSBytes(data), nil
	}))

	api.Set("readDir", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		path := jsutil.MustString(args, 1)
		fs, err := backend.FS(mountpoint)
		if err != nil {
			return nil, err
		}
		names, err := fs.ReadDir(path)
		if err != nil {
			return nil, err
		}
		arr := js.Global().Get("Array").New(len(names))
		for i, name := range names {
			arr.SetIndex(i, name)
		}
		return arr, nil
	}))

	api.Set("mkdirAll", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		path := jsutil.MustString(args, 1)
		fs, err := backend.FS(mountpoint)
		if err != nil {
			return nil, err
		}
		return nil, fs.MkdirAll(path, 0o755)
	}))

	api.Set("remove", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		path := jsutil.MustString(args, 1)
		fs, err := backend.FS(mountpoint)
		if err != nil {
			return nil, err
		}
		return nil, fs.Remove(path)
	}))

	api.Set("snapshot", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		name := jsutil.MustString(args, 1)
		return nil, backend.Snapshot(context.Background(), mountpoint, name)
	}))

	api.Set("clone", jsutil.Async(func(args []js.Value) (any, error) {
		mountpoint := jsutil.MustString(args, 0)
		cloneName := jsutil.MustString(args, 1)
		cloneMountpoint := jsutil.MustString(args, 2)
		if cloneMountpoint == "" {
			cloneMountpoint = cloneName
		}
		return nil, backend.Clone(context.Background(), mountpoint, cloneName, cloneMountpoint)
	}))

	js.Global().Set("loophole", api)
	fmt.Println("loophole wasm: ready (globalThis.loophole)")

	// Block forever — keep the Go runtime alive for callbacks.
	select {}
}
