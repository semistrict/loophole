package loophole_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/semistrict/loophole/storage"
)

// SimpleFS is a trivial flat filesystem on top of a Volume for testing.
// Each file is allocated a fixed-size region of the volume. No directories,
// no growing files — just named byte ranges.
type SimpleFS struct {
	vol      *storage.Volume
	slotSize uint64
	mu       sync.Mutex
	files    map[string]uint64 // name → slot index
	nextSlot uint64
}

func NewSimpleFS(vol *storage.Volume, slotSize uint64) *SimpleFS {
	return &SimpleFS{
		vol:      vol,
		slotSize: slotSize,
		files:    make(map[string]uint64),
	}
}

// Create allocates a new file. Returns error if it already exists.
func (fs *SimpleFS) Create(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[name]; ok {
		return fmt.Errorf("file %q already exists", name)
	}
	fs.files[name] = fs.nextSlot
	fs.nextSlot++
	return nil
}

func (fs *SimpleFS) offset(name string) (uint64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	slot, ok := fs.files[name]
	if !ok {
		return 0, fmt.Errorf("file %q not found", name)
	}
	return slot * fs.slotSize, nil
}

// WriteFile writes data at the given offset within the file's region.
func (fs *SimpleFS) WriteFile(ctx context.Context, name string, off uint64, data []byte) error {
	base, err := fs.offset(name)
	if err != nil {
		return err
	}
	if off+uint64(len(data)) > fs.slotSize {
		return fmt.Errorf("write exceeds file slot size")
	}
	return fs.vol.Write(data, base+off)
}

// ReadFile reads data at the given offset within the file's region.
func (fs *SimpleFS) ReadFile(ctx context.Context, name string, off uint64, buf []byte) (int, error) {
	base, err := fs.offset(name)
	if err != nil {
		return 0, err
	}
	if off+uint64(len(buf)) > fs.slotSize {
		return 0, fmt.Errorf("read exceeds file slot size")
	}
	return fs.vol.Read(ctx, buf, base+off)
}
