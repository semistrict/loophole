//go:build !js

package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole/internal/util"
)

// Store is the platform-specific storage backend for DiskCache.
type Store interface {
	// Page arena: fixed PageSize slot-based storage.
	ReadSlot(slot int) ([]byte, error)
	ReadSlotPinned(slot int) ([]byte, error)
	WriteSlot(slot int, data []byte) error
	AllocArena(maxSlots int) error

	// Blob storage: variable-size items keyed by string.
	ReadBlob(key string) ([]byte, error)
	WriteBlob(key string, data []byte) error
	DeleteBlob(key string) error

	// Budget
	FreeSpace() int64 // bytes free on underlying device (0 if unknown)
	MinReserve() int64

	// Lifecycle
	Close() error
}

// nativeStore implements Store using disk files.
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

func newDefaultStore(dir string) (Store, error) {
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

func (s *nativeStore) blobPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(s.dir, "blobs", name[:2], name)
}

func (s *nativeStore) ReadBlob(key string) ([]byte, error) {
	f, err := os.Open(s.blobPath(key))
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(f, "diskcache: close blob reader")

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 0 {
		return nil, fmt.Errorf("blob size < 0 for %q", key)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	fadviseDropCache(int(f.Fd()), 0, size)
	return buf, nil
}

func (s *nativeStore) WriteBlob(key string, data []byte) error {
	p := s.blobPath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "blob-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		util.SafeClose(tmp, "diskcache: close blob writer")
		_ = os.Remove(tmpName)
		return err
	}
	fadviseDropCache(int(tmp.Fd()), 0, int64(len(data)))
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *nativeStore) DeleteBlob(key string) error {
	return os.Remove(s.blobPath(key))
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
