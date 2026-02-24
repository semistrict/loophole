package lwext4

// #cgo CFLAGS: -DCONFIG_USE_DEFAULT_CFG=1
// #cgo CFLAGS: -DCONFIG_EXT4_BLOCKDEVS_COUNT=16
// #cgo CFLAGS: -DCONFIG_EXT4_MOUNTPOINTS_COUNT=16
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_OFLAGS=1
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ERRNO=0
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ASSERT=1
// #cgo CFLAGS: -DCONFIG_DEBUG_PRINTF=0
// #cgo CFLAGS: -DCONFIG_DEBUG_ASSERT=0
// #cgo CFLAGS: -I${SRCDIR}/../third_party/lwext4/include
// #include "blockdev.h"
import "C"
import (
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

// BlockDevice is the backing store for an ext4 filesystem.
// Implementations must be safe for concurrent use.
type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
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

	offset := int64(blkID) * int64(blkSize)
	size := int(blkCnt) * int(blkSize)
	goBuf := unsafe.Slice((*byte)(buf), size)

	_, err := dev.ReadAt(goBuf, offset)
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

	offset := int64(blkID) * int64(blkSize)
	size := int(blkCnt) * int(blkSize)
	goBuf := unsafe.Slice((*byte)(buf), size)

	_, err := dev.WriteAt(goBuf, offset)
	if err != nil {
		return -1
	}
	return 0
}

//export goBlockdevClose
func goBlockdevClose(handle C.int) C.int {
	return 0
}

func createBlockdev(dev BlockDevice, phBsize uint32, phBcnt uint64) (*C.struct_ext4_blockdev, int) {
	handle := registerDev(dev)
	bdev := C.create_blockdev(C.int(handle), C.uint32_t(phBsize), C.uint64_t(phBcnt))
	if bdev == nil {
		unregisterDev(handle)
		return nil, 0
	}
	return bdev, handle
}

func destroyBlockdev(bdev *C.struct_ext4_blockdev, handle int) {
	C.destroy_blockdev(bdev)
	unregisterDev(handle)
}
