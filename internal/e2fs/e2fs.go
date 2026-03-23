// Package e2fs creates ext4 filesystem images using libext2fs (CGo).
package e2fs

/*
#cgo CFLAGS: -I${SRCDIR} -I${SRCDIR}/generated -I${SRCDIR}/../../third_party/e2fsprogs/lib
#cgo CFLAGS: -Wno-unused-function -Wno-unused-variable -w

#include "e2fs.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// FS is a handle to an ext4 filesystem image being constructed.
type FS struct {
	h C.e2fs_t
}

func errStr(code C.errcode_t) error {
	if code == 0 {
		return nil
	}
	return fmt.Errorf("e2fs: error %d", int(code))
}

// Create creates a new ext4 filesystem image at the given path.
// The file must already exist and be truncated to sizeBytes.
func Create(path string, sizeBytes uint64) (*FS, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var h C.e2fs_t
	ret := C.e2fs_create(cpath, C.uint64_t(sizeBytes), &h)
	if ret != 0 {
		return nil, fmt.Errorf("e2fs_create %s: %w", path, errStr(ret))
	}
	return &FS{h: h}, nil
}

// Close flushes and closes the filesystem.
func (fs *FS) Close() error {
	if fs.h == nil {
		return nil
	}
	ret := C.e2fs_close(fs.h)
	fs.h = nil
	return errStr(ret)
}

// Mkdir creates a directory with the given metadata.
func (fs *FS) Mkdir(path string, mode uint32, uid, gid uint32, mtime int64) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return errStr(C.e2fs_mkdir(fs.h, cpath, C.uint32_t(mode),
		C.uint32_t(uid), C.uint32_t(gid), C.int64_t(mtime)))
}

// WriteFile creates a regular file with the given data and metadata.
func (fs *FS) WriteFile(path string, mode uint32, uid, gid uint32, mtime int64, data []byte) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var dataPtr unsafe.Pointer
	if len(data) > 0 {
		dataPtr = unsafe.Pointer(&data[0])
	}
	return errStr(C.e2fs_write_file(fs.h, cpath, C.uint32_t(mode),
		C.uint32_t(uid), C.uint32_t(gid), C.int64_t(mtime),
		dataPtr, C.uint64_t(len(data))))
}

// Symlink creates a symbolic link at path pointing to target.
func (fs *FS) Symlink(path, target string, uid, gid uint32, mtime int64) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ctarget := C.CString(target)
	defer C.free(unsafe.Pointer(ctarget))
	return errStr(C.e2fs_symlink(fs.h, cpath, ctarget,
		C.uint32_t(uid), C.uint32_t(gid), C.int64_t(mtime)))
}

// Mknod creates a device node, FIFO, or socket.
func (fs *FS) Mknod(path string, mode uint32, uid, gid uint32, mtime int64, major, minor uint32) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return errStr(C.e2fs_mknod(fs.h, cpath, C.uint32_t(mode),
		C.uint32_t(uid), C.uint32_t(gid), C.int64_t(mtime),
		C.uint32_t(major), C.uint32_t(minor)))
}

// Hardlink creates a hard link at path pointing to an existing file at target.
func (fs *FS) Hardlink(path, target string) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ctarget := C.CString(target)
	defer C.free(unsafe.Pointer(ctarget))
	return errStr(C.e2fs_hardlink(fs.h, cpath, ctarget))
}
