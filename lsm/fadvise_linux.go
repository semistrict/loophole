package lsm

import "golang.org/x/sys/unix"

// fadviseDropCache advises the kernel to drop pages from its page cache
// for the given file range. This prevents double-caching when the same
// data is also cached by the kernel page cache on the FUSE/loop device side.
// Runs asynchronously since it's just a hint — callers should not block on it.
func fadviseDropCache(fd int, off, length int64) {
	go unix.Fadvise(fd, off, length, unix.FADV_DONTNEED)
}
