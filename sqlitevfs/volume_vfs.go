package sqlitevfs

import (
	"context"
	"fmt"
	"sync"

	"github.com/ncruces/go-sqlite3/vfs"
	"github.com/semistrict/loophole"
)

// VolumeVFS implements [vfs.VFS] backed by a loophole Volume.
// Register it with [vfs.Register] and open databases using that VFS name.
type VolumeVFS struct {
	mu       sync.Mutex
	vol      loophole.Volume
	sb       *Superblock
	syncMode SyncMode
	dirty    bool
	ctx      context.Context
}

// Compile-time check that VolumeVFS satisfies the ncruces VFS interface.
var _ vfs.VFS = (*VolumeVFS)(nil)

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

func (v *VolumeVFS) Open(name string, flags OpenFlag) (vfs.File, OpenFlag, error) {
	idx := regionForName(name)
	if idx < 0 {
		return nil, 0, fmt.Errorf("unknown file %q", name)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]

	if flags&OpenCreate != 0 {
		if !entry.Exists() {
			entry.Flags |= flagExists
			entry.FileSize = 0
			v.dirty = true
		}
	} else if !entry.Exists() {
		return nil, 0, fmt.Errorf("file %q does not exist", name)
	}

	outFlags := OpenFlag(0)
	if v.vol.ReadOnly() {
		outFlags |= OpenReadOnly
	} else {
		outFlags |= OpenReadWrite
	}

	f := &volumeFile{
		vfs:       v,
		regionIdx: idx,
		lockLevel: LockNone,
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
		return nil // SQLite expects Delete to be idempotent.
	}

	// Punch hole to free the region's storage.
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

func (v *VolumeVFS) Access(name string, flags AccessFlag) (bool, error) {
	idx := regionForName(name)
	if idx < 0 {
		return false, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]
	switch flags {
	case AccessExists:
		return entry.Exists(), nil
	case AccessReadWrite:
		return entry.Exists() && !v.vol.ReadOnly(), nil
	default:
		return entry.Exists(), nil
	}
}

func (v *VolumeVFS) FullPathname(name string) (string, error) {
	return name, nil
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
