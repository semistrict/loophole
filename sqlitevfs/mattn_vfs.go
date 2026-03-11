//go:build !tinygo && !js

package sqlitevfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	_ "github.com/mattn/go-sqlite3" // link sqlite3 symbols for lhvfs.c
	"github.com/psanford/sqlite3vfs"

	"github.com/semistrict/loophole"
)

// VolumeVFS implements [sqlite3vfs.VFS] backed by a loophole Volume.
// The main database lives on the volume; WAL, journal, and SHM are in memory.
type VolumeVFS struct {
	mu       sync.Mutex
	vol      loophole.Volume
	header   *Header
	syncMode SyncMode
	dirty    bool

	mainDBExists bool
	wal          memBuffer
	journal      memBuffer
	shm          memBuffer
}

var _ sqlite3vfs.VFS = (*VolumeVFS)(nil)

// NewVolumeVFS opens a VFS on a volume. The volume must already be formatted
// with FormatVolume.
func NewVolumeVFS(ctx context.Context, vol loophole.Volume, syncMode SyncMode) (*VolumeVFS, error) {
	h, err := ReadHeader(ctx, vol)
	if err != nil {
		return nil, fmt.Errorf("open VFS: %w", err)
	}
	v := &VolumeVFS{
		vol:          vol,
		header:       h,
		syncMode:     syncMode,
		mainDBExists: h.MainDBSize > 0,
	}
	// Restore WAL from volume if present (persisted by a previous flush).
	if h.WALSize > 0 {
		walData := make([]byte, h.WALSize)
		if _, err := vol.Read(ctx, walData, vol.Size()-h.WALSize); err != nil {
			return nil, fmt.Errorf("read WAL from volume: %w", err)
		}
		v.wal.restore(walData)
	}
	return v, nil
}

func (v *VolumeVFS) memBuf(k fileKind) *memBuffer {
	switch k {
	case fileWAL:
		return &v.wal
	case fileJournal:
		return &v.journal
	case fileSHM:
		return &v.shm
	default:
		return nil
	}
}

func (v *VolumeVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	kind, ok := fileKindForName(name)
	if !ok {
		return nil, 0, fmt.Errorf("unknown file %q", name)
	}

	if kind == fileMainDB {
		v.mu.Lock()
		if flags&sqlite3vfs.OpenCreate != 0 {
			v.mainDBExists = true
		} else if !v.mainDBExists {
			v.mu.Unlock()
			return nil, 0, fmt.Errorf("file %q does not exist", name)
		}
		v.mu.Unlock()
	} else {
		buf := v.memBuf(kind)
		if flags&sqlite3vfs.OpenCreate != 0 {
			buf.setExists()
		} else if !buf.isExists() {
			return nil, 0, fmt.Errorf("file %q does not exist", name)
		}
	}

	outFlags := sqlite3vfs.OpenFlag(0)
	if v.vol.ReadOnly() {
		outFlags |= sqlite3vfs.OpenReadOnly
	} else {
		outFlags |= sqlite3vfs.OpenReadWrite
	}

	f := &volumeFile{
		vfs:       v,
		kind:      kind,
		lockLevel: sqlite3vfs.LockNone,
	}
	return f, outFlags, nil
}

func (v *VolumeVFS) Delete(name string, dirSync bool) error {
	kind, ok := fileKindForName(name)
	if !ok {
		return fmt.Errorf("unknown file %q", name)
	}

	if kind == fileMainDB {
		v.mu.Lock()
		defer v.mu.Unlock()

		if !v.mainDBExists {
			return nil
		}
		if v.header.MainDBSize > 0 {
			if err := v.vol.PunchHole(mainDBOffset, v.header.MainDBSize); err != nil {
				return fmt.Errorf("punch hole: %w", err)
			}
		}
		v.mainDBExists = false
		v.header.MainDBSize = 0
		v.dirty = true
		return nil
	}

	v.memBuf(kind).reset()
	return nil
}

func (v *VolumeVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	kind, ok := fileKindForName(name)
	if !ok {
		return false, nil
	}

	var exists bool
	if kind == fileMainDB {
		v.mu.Lock()
		exists = v.mainDBExists
		v.mu.Unlock()
	} else {
		exists = v.memBuf(kind).isExists()
	}

	switch flags {
	case sqlite3vfs.AccessExists:
		return exists, nil
	case sqlite3vfs.AccessReadWrite:
		return exists && !v.vol.ReadOnly(), nil
	default:
		return exists, nil
	}
}

func (v *VolumeVFS) FullPathname(name string) string {
	return name
}

// FlushHeader writes the header if dirty.
func (v *VolumeVFS) FlushHeader() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushHeaderLocked()
}

func (v *VolumeVFS) flushHeaderLocked() error {
	// Persist WAL to the end of the volume for snapshot/clone correctness.
	walData := v.wal.snapshot()
	walSize := uint64(len(walData))
	if walSize > 0 {
		if err := v.vol.Write(walData, v.vol.Size()-walSize); err != nil {
			return fmt.Errorf("persist WAL: %w", err)
		}
	}
	if walSize != v.header.WALSize {
		v.header.WALSize = walSize
		v.dirty = true
	}

	if !v.dirty {
		return nil
	}
	if err := writeHeader(v.vol, v.header); err != nil {
		return err
	}
	v.dirty = false
	return nil
}

// Header returns the current header (for testing/inspection).
func (v *VolumeVFS) Header() *Header {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.header
}

// SetHeader replaces the in-memory header. Used by follow mode
// to refresh file sizes after re-reading from the volume.
func (v *VolumeVFS) SetHeader(h *Header) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.header = h
	v.mainDBExists = h.MainDBSize > 0
}

// SyncMode returns the sync mode configured at open time.
func (v *VolumeVFS) SyncMode() SyncMode {
	return v.syncMode
}

// volumeFile implements [sqlite3vfs.File] for a volume-backed VFS.
type volumeFile struct {
	vfs       *VolumeVFS
	kind      fileKind
	lockLevel sqlite3vfs.LockType
	lockMu    sync.Mutex
}

var _ sqlite3vfs.File = (*volumeFile)(nil)

func (f *volumeFile) Close() error {
	return f.vfs.FlushHeader()
}

func (f *volumeFile) ReadAt(p []byte, off int64) (int, error) {
	if f.kind != fileMainDB {
		return f.vfs.memBuf(f.kind).readAt(p, off)
	}

	f.vfs.mu.Lock()
	fileSize := int64(f.vfs.header.MainDBSize)
	f.vfs.mu.Unlock()

	if off >= fileSize {
		clear(p)
		return len(p), io.EOF
	}

	n := int64(len(p))
	if off+n > fileSize {
		n = fileSize - off
	}

	_, err := f.vfs.vol.Read(context.Background(), p[:n], mainDBOffset+uint64(off))
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
	if f.kind != fileMainDB {
		return f.vfs.memBuf(f.kind).writeAt(p, off), nil
	}

	volCap := int64(f.vfs.vol.Size()) - mainDBOffset
	if off+int64(len(p)) > volCap {
		return 0, fmt.Errorf("write exceeds volume capacity (%d + %d > %d)", off, len(p), volCap)
	}

	if err := f.vfs.vol.Write(p, mainDBOffset+uint64(off)); err != nil {
		return 0, fmt.Errorf("write at offset %d: %w", off, err)
	}

	newEnd := uint64(off) + uint64(len(p))
	f.vfs.mu.Lock()
	if newEnd > f.vfs.header.MainDBSize {
		f.vfs.header.MainDBSize = newEnd
		f.vfs.dirty = true
	}
	f.vfs.mu.Unlock()

	return len(p), nil
}

func (f *volumeFile) Truncate(size int64) error {
	if f.kind != fileMainDB {
		f.vfs.memBuf(f.kind).truncate(size)
		return nil
	}

	f.vfs.mu.Lock()
	oldSize := f.vfs.header.MainDBSize
	f.vfs.header.MainDBSize = uint64(size)
	f.vfs.dirty = true
	f.vfs.mu.Unlock()

	if uint64(size) < oldSize {
		if err := f.vfs.vol.PunchHole(mainDBOffset+uint64(size), oldSize-uint64(size)); err != nil {
			return fmt.Errorf("truncate punch hole: %w", err)
		}
	}
	return nil
}

func (f *volumeFile) Sync(flag sqlite3vfs.SyncType) error {
	if f.vfs.syncMode == SyncModeAsync {
		return nil
	}
	if err := f.vfs.FlushHeader(); err != nil {
		return err
	}
	return f.vfs.vol.Flush()
}

func (f *volumeFile) FileSize() (int64, error) {
	if f.kind != fileMainDB {
		return f.vfs.memBuf(f.kind).fileSize(), nil
	}
	f.vfs.mu.Lock()
	defer f.vfs.mu.Unlock()
	return int64(f.vfs.header.MainDBSize), nil
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
