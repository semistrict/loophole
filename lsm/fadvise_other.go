//go:build !linux

package lsm

func fadviseDropCache(fd int, off, length int64) {}
