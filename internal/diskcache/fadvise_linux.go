package diskcache

import "golang.org/x/sys/unix"

// fadviseDropCache advises the kernel to drop pages from its page cache
// for the given file range. Runs asynchronously since it's just a hint.
func fadviseDropCache(fd int, off, length int64) {
	go unix.Fadvise(fd, off, length, unix.FADV_DONTNEED)
}
