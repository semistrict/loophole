package sqlitevfs

import (
	"fmt"
	"io"
	"sync"

	"github.com/ncruces/go-sqlite3/vfs"
)

// volumeFile implements [vfs.File] for a region within a volume.
type volumeFile struct {
	vfs       *VolumeVFS
	regionIdx int
	lockLevel LockLevel
	lockMu    sync.Mutex
}

// Compile-time check.
var _ vfs.File = (*volumeFile)(nil)

func (f *volumeFile) region() *RegionEntry {
	return &f.vfs.sb.Regions[f.regionIdx]
}

func (f *volumeFile) Close() error {
	// Flush superblock on close to persist any in-memory FileSize changes.
	return f.vfs.FlushSuperblock()
}

func (f *volumeFile) ReadAt(p []byte, off int64) (int, error) {
	f.vfs.mu.Lock()
	entry := f.region()
	fileSize := int64(entry.FileSize)
	regionStart := entry.RegionStart
	f.vfs.mu.Unlock()

	if off >= fileSize {
		// Past EOF: fill with zeros per SQLite's short-read semantics.
		clear(p)
		return len(p), io.EOF
	}

	// Clamp to file size.
	n := int64(len(p))
	if off+n > fileSize {
		n = fileSize - off
	}

	readBuf := p[:n]
	_, err := f.vfs.vol.Read(f.vfs.ctx, readBuf, regionStart+uint64(off))
	if err != nil {
		return 0, fmt.Errorf("read at offset %d: %w", off, err)
	}

	// Zero-fill the remainder if we read less than requested.
	if n < int64(len(p)) {
		clear(p[n:])
		return int(n), io.EOF
	}
	return int(n), nil
}

func (f *volumeFile) WriteAt(p []byte, off int64) (int, error) {
	f.vfs.mu.Lock()
	entry := f.region()
	regionStart := entry.RegionStart
	regionCap := int64(entry.RegionCapacity)
	f.vfs.mu.Unlock()

	if off+int64(len(p)) > regionCap {
		return 0, fmt.Errorf("write exceeds region capacity (%d + %d > %d)", off, len(p), regionCap)
	}

	if err := f.vfs.vol.Write(f.vfs.ctx, p, regionStart+uint64(off)); err != nil {
		return 0, fmt.Errorf("write at offset %d: %w", off, err)
	}

	// Extend file size if needed.
	newEnd := uint64(off) + uint64(len(p))
	f.vfs.mu.Lock()
	if newEnd > entry.FileSize {
		entry.FileSize = newEnd
		f.vfs.dirty = true
	}
	f.vfs.mu.Unlock()

	return len(p), nil
}

func (f *volumeFile) Truncate(size int64) error {
	f.vfs.mu.Lock()
	entry := f.region()
	oldSize := entry.FileSize
	entry.FileSize = uint64(size)
	f.vfs.dirty = true
	regionStart := entry.RegionStart
	f.vfs.mu.Unlock()

	// Punch hole for the freed range if shrinking.
	if uint64(size) < oldSize {
		freed := oldSize - uint64(size)
		if err := f.vfs.vol.PunchHole(f.vfs.ctx, regionStart+uint64(size), freed); err != nil {
			return fmt.Errorf("truncate punch hole: %w", err)
		}
	}
	return nil
}

func (f *volumeFile) Sync(flags SyncFlag) error {
	if f.vfs.syncMode == SyncModeAsync {
		return nil // no-op in async mode
	}

	// Sync mode: flush superblock + volume.
	if err := f.vfs.FlushSuperblock(); err != nil {
		return err
	}
	return f.vfs.vol.Flush(f.vfs.ctx)
}

func (f *volumeFile) Size() (int64, error) {
	f.vfs.mu.Lock()
	defer f.vfs.mu.Unlock()
	return int64(f.region().FileSize), nil
}

func (f *volumeFile) Lock(level LockLevel) error {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	// Single-process locking: just track the level.
	// Multi-process locking is enforced by the volume's lease.
	f.lockLevel = level
	return nil
}

func (f *volumeFile) Unlock(level LockLevel) error {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	f.lockLevel = level
	return nil
}

func (f *volumeFile) CheckReservedLock() (bool, error) {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	return f.lockLevel >= LockReserved, nil
}

func (f *volumeFile) SectorSize() int {
	return 4096
}

func (f *volumeFile) DeviceCharacteristics() DeviceCharacteristic {
	chars := IocapAtomic4K | IocapSafeAppend | IocapSequential

	if f.vfs.syncMode == SyncModeAsync {
		chars |= IocapPowersafeOverwrite
	}
	return chars
}
