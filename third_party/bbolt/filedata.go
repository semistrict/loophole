//go:build !js

package bbolt

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// FileData is a file-based implementation of the Data interface.
// It mmaps the file for reads and uses the file handle for writes.
type FileData struct {
	f        *os.File
	path     string
	mmap     []byte
	oldmmaps [][]byte // old mappings kept alive for concurrent readers
}

// OpenFileData opens or creates a file-based Data implementation.
func OpenFileData(path string, mode os.FileMode, readOnly bool) (*FileData, error) {
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}

	f, err := os.OpenFile(path, flag, mode)
	if err != nil {
		if os.IsNotExist(err) && !readOnly {
			f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	d := &FileData{f: f, path: path}

	// mmap if file is non-empty
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() > 0 {
		if err := d.remap(fi.Size()); err != nil {
			f.Close()
			return nil, err
		}
	}

	return d, nil
}

// Path returns the file path.
func (d *FileData) Path() string {
	return d.path
}

func (d *FileData) remap(sz int64) error {
	if sz == 0 {
		if d.mmap != nil {
			if err := unix.Munmap(d.mmap); err != nil {
				return fmt.Errorf("munmap: %w", err)
			}
			d.mmap = nil
		}
		return nil
	}

	b, err := unix.Mmap(int(d.f.Fd()), 0, int(sz), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	_ = unix.Madvise(b, syscall.MADV_RANDOM)
	// Keep old mapping alive so concurrent readers holding slices into it
	// don't get invalidated.
	if d.mmap != nil {
		d.oldmmaps = append(d.oldmmaps, d.mmap)
	}
	d.mmap = b
	return nil
}

func (d *FileData) ReadAt(off int64, n int) ([]byte, func(), error) {
	if d.mmap == nil {
		return nil, nil, fmt.Errorf("filedata: no mapping")
	}
	end := int(off) + n
	if off < 0 || end > len(d.mmap) {
		return nil, nil, fmt.Errorf("filedata: read [%d, %d) out of range [0, %d)", off, end, len(d.mmap))
	}
	buf := make([]byte, n)
	copy(buf, d.mmap[off:end])
	return buf, func() {}, nil
}

func (d *FileData) WriteAt(b []byte, off int64) (int, error) {
	n, err := d.f.WriteAt(b, off)
	if err != nil {
		return n, err
	}
	// Remap if the write extended past the current mapping.
	end := off + int64(n)
	if d.mmap == nil || end > int64(len(d.mmap)) {
		if rerr := d.remap(end); rerr != nil {
			return n, rerr
		}
	}
	return n, nil
}

func (d *FileData) Size() (int64, error) {
	fi, err := d.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (d *FileData) Grow(sz int64) error {
	fi, err := d.f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < sz {
		if err := d.f.Truncate(sz); err != nil {
			return err
		}
	}
	// Remap to cover the new size.
	return d.remap(sz)
}

func (d *FileData) Sync() error {
	return d.f.Sync()
}

func (d *FileData) Close() error {
	for _, m := range d.oldmmaps {
		unix.Munmap(m)
	}
	d.oldmmaps = nil
	if d.mmap != nil {
		unix.Munmap(d.mmap)
		d.mmap = nil
	}
	return d.f.Close()
}
