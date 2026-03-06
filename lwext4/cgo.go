package lwext4

// #cgo LDFLAGS: -llwext4
// #include "lwext4_cgo.h"
import "C"
import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"
)

// BlockDevice is the backing store for an ext4 filesystem.
// Implementations must be safe for concurrent use.
type BlockDevice interface {
	Read(ctx context.Context, buf []byte, offset uint64) (int, error)
	Write(ctx context.Context, data []byte, offset uint64) error
	// ZeroRange zeros the given byte range. Implementations may use
	// punch-hole or similar mechanisms to avoid materializing zeros.
	ZeroRange(ctx context.Context, offset, length uint64) error
}

// handle map: Go ↔ C callback bridge
var (
	handles    sync.Map
	nextHandle atomic.Int64
)

func registerDev(dev BlockDevice) int {
	h := int(nextHandle.Add(1))
	handles.Store(h, dev)
	return h
}

func unregisterDev(handle int) {
	handles.Delete(handle)
}

func lookupDev(handle int) BlockDevice {
	v, ok := handles.Load(handle)
	if !ok {
		return nil
	}
	return v.(BlockDevice)
}

//export goBlockdevOpen
func goBlockdevOpen(handle C.int) C.int {
	return 0
}

//export goBlockdevRead
func goBlockdevRead(handle C.int, buf unsafe.Pointer, blkID C.uint64_t, blkCnt C.uint32_t, blkSize C.uint32_t) C.int {
	dev := lookupDev(int(handle))
	if dev == nil {
		return -1
	}

	offset := uint64(blkID) * uint64(blkSize)
	size := int(blkCnt) * int(blkSize)
	goBuf := unsafe.Slice((*byte)(buf), size)

	_, err := dev.Read(context.Background(), goBuf, offset)
	if err != nil {
		return -1
	}
	return 0
}

//export goBlockdevWrite
func goBlockdevWrite(handle C.int, buf unsafe.Pointer, blkID C.uint64_t, blkCnt C.uint32_t, blkSize C.uint32_t) C.int {
	dev := lookupDev(int(handle))
	if dev == nil {
		return -1
	}

	offset := uint64(blkID) * uint64(blkSize)
	size := int(blkCnt) * int(blkSize)
	goBuf := unsafe.Slice((*byte)(buf), size)

	err := dev.Write(context.Background(), goBuf, offset)
	if err != nil {
		return -1
	}
	return 0
}

//export goBlockdevClose
func goBlockdevClose(handle C.int) C.int {
	return 0
}

func createBlockdev(dev BlockDevice, phBsize uint32, phBcnt uint64) (C.lh_bdev, int) {
	handle := registerDev(dev)
	bdev := C.lh_create_blockdev(C.int(handle), C.uint32_t(phBsize), C.uint64_t(phBcnt))
	if bdev == nil {
		unregisterDev(handle)
		return nil, 0
	}
	return bdev, handle
}

func destroyBlockdev(bdev C.lh_bdev, handle int) {
	C.lh_destroy_blockdev(bdev)
	unregisterDev(handle)
}
