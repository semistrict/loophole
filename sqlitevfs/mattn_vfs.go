package sqlitevfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/psanford/sqlite3vfs"

	"github.com/semistrict/loophole"
)

// VolumeVFS implements [sqlite3vfs.VFS] backed by a loophole Volume.
// Register it with [sqlite3vfs.RegisterVFS] and open databases using that VFS name.
type VolumeVFS struct {
	mu       sync.Mutex
	vol      loophole.Volume
	sb       *Superblock
	syncMode SyncMode
	dirty    bool
	ctx      context.Context
}

// Compile-time check that VolumeVFS satisfies the psanford VFS interface.
var _ sqlite3vfs.VFS = (*VolumeVFS)(nil)

// NewVolumeVFS opens a VFS on a volume. The volume must already be formatted
// with FormatVolume. Use ctx for background operations (superblock writes).
func NewVolumeVFS(ctx context.Context, vol loophole.Volume, syncMode SyncMode) (*VolumeVFS, error) {
	sb, err := ReadSuperblock(ctx, vol)
	if err != nil {
		return nil, fmt.Errorf("open VFS: %w", err)
	}
	return &VolumeVFS{
		vol:      vol,
		sb:       sb,
		syncMode: syncMode,
		ctx:      ctx,
	}, nil
}

func (v *VolumeVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	idx := regionForName(name)
	if idx < 0 {
		return nil, 0, fmt.Errorf("unknown file %q", name)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]

	if flags&sqlite3vfs.OpenCreate != 0 {
		if !entry.Exists() {
			entry.Flags |= flagExists
			entry.FileSize = 0
			v.dirty = true
		}
	} else if !entry.Exists() {
		return nil, 0, fmt.Errorf("file %q does not exist", name)
	}

	outFlags := sqlite3vfs.OpenFlag(0)
	if v.vol.ReadOnly() {
		outFlags |= sqlite3vfs.OpenReadOnly
	} else {
		outFlags |= sqlite3vfs.OpenReadWrite
	}

	f := &volumeFile{
		vfs:       v,
		regionIdx: idx,
		lockLevel: sqlite3vfs.LockNone,
	}
	return f, outFlags, nil
}

func (v *VolumeVFS) Delete(name string, dirSync bool) error {
	idx := regionForName(name)
	if idx < 0 {
		return fmt.Errorf("unknown file %q", name)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]
	if !entry.Exists() {
		return nil
	}

	if entry.FileSize > 0 {
		if err := v.vol.PunchHole(v.ctx, entry.RegionStart, entry.FileSize); err != nil {
			return fmt.Errorf("punch hole: %w", err)
		}
	}

	entry.Flags = 0
	entry.FileSize = 0
	v.dirty = true
	return nil
}

func (v *VolumeVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	idx := regionForName(name)
	if idx < 0 {
		return false, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]
	switch flags {
	case sqlite3vfs.AccessExists:
		return entry.Exists(), nil
	case sqlite3vfs.AccessReadWrite:
		return entry.Exists() && !v.vol.ReadOnly(), nil
	default:
		return entry.Exists(), nil
	}
}

func (v *VolumeVFS) FullPathname(name string) string {
	return name
}

// FlushSuperblock writes the superblock if dirty.
func (v *VolumeVFS) FlushSuperblock() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushSuperblockLocked()
}

func (v *VolumeVFS) flushSuperblockLocked() error {
	if !v.dirty {
		return nil
	}
	if err := writeSuperblock(v.ctx, v.vol, v.sb); err != nil {
		return err
	}
	v.dirty = false
	return nil
}

// Superblock returns the current superblock (for testing/inspection).
func (v *VolumeVFS) Superblock() *Superblock {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sb
}

// SetSuperblock replaces the in-memory superblock. Used by follow mode
// to refresh file sizes after re-reading from the volume.
func (v *VolumeVFS) SetSuperblock(sb *Superblock) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sb = sb
}

// SyncMode returns the sync mode configured at open time.
func (v *VolumeVFS) SyncMode() SyncMode {
	return v.syncMode
}

// volumeFile implements [sqlite3vfs.File] for a region within a volume.
type volumeFile struct {
	vfs       *VolumeVFS
	regionIdx int
	lockLevel sqlite3vfs.LockType
	lockMu    sync.Mutex
}

var _ sqlite3vfs.File = (*volumeFile)(nil)

func (f *volumeFile) region() *RegionEntry {
	return &f.vfs.sb.Regions[f.regionIdx]
}

func (f *volumeFile) Close() error {
	return f.vfs.FlushSuperblock()
}

func (f *volumeFile) ReadAt(p []byte, off int64) (int, error) {
	f.vfs.mu.Lock()
	entry := f.region()
	fileSize := int64(entry.FileSize)
	regionStart := entry.RegionStart
	f.vfs.mu.Unlock()

	if off >= fileSize {
		clear(p)
		return len(p), io.EOF
	}

	n := int64(len(p))
	if off+n > fileSize {
		n = fileSize - off
	}

	readBuf := p[:n]
	_, err := f.vfs.vol.Read(f.vfs.ctx, readBuf, regionStart+uint64(off))
	if err != nil {
		return 0, fmt.Errorf("read at offset %d: %w", off, err)
	}

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

	if uint64(size) < oldSize {
		freed := oldSize - uint64(size)
		if err := f.vfs.vol.PunchHole(f.vfs.ctx, regionStart+uint64(size), freed); err != nil {
			return fmt.Errorf("truncate punch hole: %w", err)
		}
	}
	return nil
}

func (f *volumeFile) Sync(flag sqlite3vfs.SyncType) error {
	if f.vfs.syncMode == SyncModeAsync {
		return nil
	}
	if err := f.vfs.FlushSuperblock(); err != nil {
		return err
	}
	return f.vfs.vol.Flush(f.vfs.ctx)
}

func (f *volumeFile) FileSize() (int64, error) {
	f.vfs.mu.Lock()
	defer f.vfs.mu.Unlock()
	return int64(f.region().FileSize), nil
}

func (f *volumeFile) Lock(elock sqlite3vfs.LockType) error {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	f.lockLevel = elock
	return nil
}

func (f *volumeFile) Unlock(elock sqlite3vfs.LockType) error {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	f.lockLevel = elock
	return nil
}

func (f *volumeFile) CheckReservedLock() (bool, error) {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	return f.lockLevel >= sqlite3vfs.LockReserved, nil
}

func (f *volumeFile) SectorSize() int64 {
	return 4096
}

func (f *volumeFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	chars := sqlite3vfs.IocapAtomic4K | sqlite3vfs.IocapSafeAppend | sqlite3vfs.IocapSequential
	if f.vfs.syncMode == SyncModeAsync {
		chars |= sqlite3vfs.IocapPowersafeOverwrite
	}
	return chars
}
