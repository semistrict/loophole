//go:build !js

package storage2

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// cacheStore is the platform-specific storage backend for PageCache.
type cacheStore interface {
	ReadSlot(slot int) ([]byte, error)
	ReadSlotPinned(slot int) ([]byte, error)
	WriteSlot(slot int, data []byte) error
	AllocArena(maxSlots int) error
	FreeSpace() int64
	MinReserve() int64
	Close() error
}

// nativeStore implements cacheStore using disk files.
type nativeStore struct {
	dir       string
	arenaFile *os.File
	arenaMmap []byte
}

func newNativeStore(dir string) (*nativeStore, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &nativeStore{dir: dir}, nil
}

func newDefaultStore(dir string) (cacheStore, error) {
	return newNativeStore(dir)
}

func (s *nativeStore) AllocArena(maxSlots int) error {
	path := filepath.Join(s.dir, "arena")

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	_ = os.Remove(path)
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	size := int64(maxSlots) * PageSize
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}

	mapped, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}

	s.arenaFile = f
	s.arenaMmap = mapped
	return nil
}

func (s *nativeStore) ReadSlot(slot int) ([]byte, error) {
	if s.arenaMmap == nil {
		return nil, fmt.Errorf("arena not allocated")
	}
	off := slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, s.arenaMmap[off:off+PageSize])
	return buf, nil
}

func (s *nativeStore) ReadSlotPinned(slot int) ([]byte, error) {
	if s.arenaMmap == nil {
		return nil, fmt.Errorf("arena not allocated")
	}
	off := slot * PageSize
	return s.arenaMmap[off : off+PageSize], nil
}

func (s *nativeStore) WriteSlot(slot int, data []byte) error {
	if s.arenaMmap == nil {
		return fmt.Errorf("arena not allocated")
	}
	off := slot * PageSize
	copy(s.arenaMmap[off:off+PageSize], data)
	return nil
}

func (s *nativeStore) FreeSpace() int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &stat); err != nil {
		return 0
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}

func (s *nativeStore) MinReserve() int64 {
	return 2 * 1024 * 1024 * 1024 // 2GB
}

func (s *nativeStore) Close() error {
	if s.arenaMmap != nil {
		if err := unix.Munmap(s.arenaMmap); err != nil {
			return err
		}
		s.arenaMmap = nil
	}
	if s.arenaFile != nil {
		if err := s.arenaFile.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(s.dir)
}
