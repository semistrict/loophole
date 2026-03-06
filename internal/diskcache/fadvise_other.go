//go:build !linux

package diskcache

func fadviseDropCache(fd int, off, length int64) {}
