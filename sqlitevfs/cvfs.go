package sqlitevfs

/*
#include "lhvfs.h"
#include <string.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/semistrict/loophole"
)

// ---------- VFS registry (name → *CVFS) ----------

var (
	cvfsRegistry   = map[string]*CVFS{}
	cvfsRegistryMu sync.Mutex
)

// CVFS is a loophole-backed SQLite VFS with full WAL/SHM/locking support.
type CVFS struct {
	name     string
	vol      loophole.Volume
	sb       *Superblock
	syncMode SyncMode
	ctx      context.Context

	mu    sync.Mutex // protects sb, dirty
	dirty bool

	// In-process lock state for the main database file.
	lock dbLock

	// Shared memory for WAL index, shared across all connections.
	shm shmState
}

// NewCVFS creates and registers a new C VFS backed by a loophole Volume.
// The volume must already be formatted with FormatVolume.
func NewCVFS(ctx context.Context, vol loophole.Volume, name string, syncMode SyncMode) (*CVFS, error) {
	sb, err := ReadSuperblock(ctx, vol)
	if err != nil {
		return nil, fmt.Errorf("open CVFS: %w", err)
	}

	v := &CVFS{
		name:     name,
		vol:      vol,
		sb:       sb,
		syncMode: syncMode,
		ctx:      ctx,
	}

	cvfsRegistryMu.Lock()
	cvfsRegistry[name] = v
	cvfsRegistryMu.Unlock()

	cname := C.CString(name)
	// cname is intentionally leaked — sqlite3_vfs.zName must remain valid.
	rc := C.lhvfs_register(cname, 0)
	if rc != C.SQLITE_OK {
		cvfsRegistryMu.Lock()
		delete(cvfsRegistry, name)
		cvfsRegistryMu.Unlock()
		C.free(unsafe.Pointer(cname))
		return nil, fmt.Errorf("lhvfs_register %q: sqlite error %d", name, int(rc))
	}

	return v, nil
}

// FlushSuperblock writes the superblock if dirty.
func (v *CVFS) FlushSuperblock() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.dirty {
		return nil
	}
	if err := writeSuperblock(v.ctx, v.vol, v.sb); err != nil {
		return err
	}
	v.dirty = false
	return nil
}

func cvfsLookup(cvfs *C.sqlite3_vfs) *CVFS {
	name := C.GoString(cvfs.zName)
	cvfsRegistryMu.Lock()
	v := cvfsRegistry[name]
	cvfsRegistryMu.Unlock()
	return v
}

// ---------- File handle registry ----------

var (
	fileMu     sync.Mutex
	nextFileID atomic.Uint64
	fileMap    = map[uint64]*cvfsFile{}
)

type cvfsFile struct {
	vfs       *CVFS
	regionIdx int
	lockLevel int // SQLITE_LOCK_NONE .. SQLITE_LOCK_EXCLUSIVE
}

func fileFromC(cf *C.sqlite3_file) *cvfsFile {
	lf := (*C.lhvfs_file)(unsafe.Pointer(cf))
	id := uint64(lf.id)
	fileMu.Lock()
	f := fileMap[id]
	fileMu.Unlock()
	return f
}

// ---------- VFS callbacks ----------

//export goLHOpen
func goLHOpen(cvfs *C.sqlite3_vfs, cname *C.char, cf *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {
	v := cvfsLookup(cvfs)
	if v == nil {
		return C.SQLITE_ERROR
	}

	name := C.GoString(cname)
	idx := regionForName(name)
	if idx < 0 {
		return C.SQLITE_CANTOPEN
	}

	v.mu.Lock()
	entry := &v.sb.Regions[idx]
	if flags&C.SQLITE_OPEN_CREATE != 0 {
		if !entry.Exists() {
			entry.Flags |= flagExists
			entry.FileSize = 0
			v.dirty = true
		}
	} else if !entry.Exists() {
		v.mu.Unlock()
		return C.SQLITE_CANTOPEN
	}
	v.mu.Unlock()

	if outFlags != nil {
		if v.vol.ReadOnly() {
			*outFlags = C.SQLITE_OPEN_READONLY
		} else {
			*outFlags = C.SQLITE_OPEN_READWRITE
		}
	}

	f := &cvfsFile{
		vfs:       v,
		regionIdx: idx,
		lockLevel: C.SQLITE_LOCK_NONE,
	}

	id := nextFileID.Add(1)
	fileMu.Lock()
	fileMap[id] = f
	fileMu.Unlock()

	lf := (*C.lhvfs_file)(unsafe.Pointer(cf))
	C.memset(unsafe.Pointer(lf), 0, C.sizeof_lhvfs_file)
	lf.id = C.sqlite3_uint64(id)

	return C.SQLITE_OK
}

//export goLHDelete
func goLHDelete(cvfs *C.sqlite3_vfs, cname *C.char, syncDir C.int) C.int {
	v := cvfsLookup(cvfs)
	if v == nil {
		return C.SQLITE_ERROR
	}

	name := C.GoString(cname)
	idx := regionForName(name)
	if idx < 0 {
		return C.SQLITE_OK // unknown file, nothing to delete
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	entry := &v.sb.Regions[idx]
	if !entry.Exists() {
		return C.SQLITE_OK
	}

	if entry.FileSize > 0 {
		if err := v.vol.PunchHole(v.ctx, entry.RegionStart, entry.FileSize); err != nil {
			return C.SQLITE_IOERR_DELETE
		}
	}
	entry.Flags = 0
	entry.FileSize = 0
	v.dirty = true
	return C.SQLITE_OK
}

//export goLHAccess
func goLHAccess(cvfs *C.sqlite3_vfs, cname *C.char, flags C.int, pOut *C.int) C.int {
	v := cvfsLookup(cvfs)
	if v == nil {
		*pOut = 0
		return C.SQLITE_OK
	}

	name := C.GoString(cname)
	idx := regionForName(name)
	if idx < 0 {
		*pOut = 0
		return C.SQLITE_OK
	}

	v.mu.Lock()
	entry := &v.sb.Regions[idx]
	exists := entry.Exists()
	v.mu.Unlock()

	switch flags {
	case C.SQLITE_ACCESS_EXISTS:
		if exists {
			*pOut = 1
		} else {
			*pOut = 0
		}
	case C.SQLITE_ACCESS_READWRITE:
		if exists && !v.vol.ReadOnly() {
			*pOut = 1
		} else {
			*pOut = 0
		}
	default:
		if exists {
			*pOut = 1
		} else {
			*pOut = 0
		}
	}
	return C.SQLITE_OK
}

//export goLHFullPathname
func goLHFullPathname(cvfs *C.sqlite3_vfs, cname *C.char, nOut C.int, zOut *C.char) C.int {
	name := C.GoString(cname)
	if len(name)+1 >= int(nOut) {
		return C.SQLITE_ERROR
	}
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	C.memcpy(unsafe.Pointer(zOut), unsafe.Pointer(cs), C.size_t(len(name)+1))
	return C.SQLITE_OK
}

//export goLHRandomness
func goLHRandomness(cvfs *C.sqlite3_vfs, nByte C.int, zOut *C.char) C.int {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(zOut)), int(nByte))
	n, _ := rand.Read(buf)
	return C.int(n)
}

//export goLHSleep
func goLHSleep(cvfs *C.sqlite3_vfs, us C.int) C.int {
	time.Sleep(time.Duration(us) * time.Microsecond)
	return C.SQLITE_OK
}

//export goLHCurrentTimeInt64
func goLHCurrentTimeInt64(cvfs *C.sqlite3_vfs, pTime *C.sqlite3_int64) C.int {
	const unixEpochJD = 24405875 * 8640000 // Julian day epoch in ms
	*pTime = C.sqlite3_int64(unixEpochJD + time.Now().UnixNano()/1000000)
	return C.SQLITE_OK
}

// ---------- File I/O callbacks ----------

//export goLHClose
func goLHClose(cf *C.sqlite3_file) C.int {
	lf := (*C.lhvfs_file)(unsafe.Pointer(cf))
	id := uint64(lf.id)

	fileMu.Lock()
	f := fileMap[id]
	delete(fileMap, id)
	fileMu.Unlock()

	if f == nil {
		return C.SQLITE_OK
	}

	// Release any held lock.
	if f.lockLevel > C.SQLITE_LOCK_NONE {
		f.vfs.lock.unlock(f, C.SQLITE_LOCK_NONE)
		f.lockLevel = C.SQLITE_LOCK_NONE
	}

	if err := f.vfs.FlushSuperblock(); err != nil {
		slog.Warn("lhvfs: close flush superblock", "error", err)
	}

	return C.SQLITE_OK
}

//export goLHRead
func goLHRead(cf *C.sqlite3_file, buf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_READ
	}

	f.vfs.mu.Lock()
	entry := f.vfs.sb.Regions[f.regionIdx]
	f.vfs.mu.Unlock()

	off := int64(iOfst)
	n := int(iAmt)
	goBuf := unsafe.Slice((*byte)(buf), n)

	fileSize := int64(entry.FileSize)
	if off >= fileSize {
		for i := range goBuf {
			goBuf[i] = 0
		}
		return C.SQLITE_IOERR_SHORT_READ
	}

	toRead := n
	if off+int64(toRead) > fileSize {
		toRead = int(fileSize - off)
	}

	_, err := f.vfs.vol.Read(f.vfs.ctx, goBuf[:toRead], entry.RegionStart+uint64(off))
	if err != nil && err != io.EOF {
		return C.SQLITE_IOERR_READ
	}

	// Zero-fill the rest if short.
	if toRead < n {
		for i := toRead; i < n; i++ {
			goBuf[i] = 0
		}
		return C.SQLITE_IOERR_SHORT_READ
	}
	return C.SQLITE_OK
}

//export goLHWrite
func goLHWrite(cf *C.sqlite3_file, buf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_WRITE
	}

	f.vfs.mu.Lock()
	entry := &f.vfs.sb.Regions[f.regionIdx]
	regionStart := entry.RegionStart
	regionCap := int64(entry.RegionCapacity)
	f.vfs.mu.Unlock()

	off := int64(iOfst)
	n := int(iAmt)

	if off+int64(n) > regionCap {
		return C.SQLITE_FULL
	}

	goBuf := unsafe.Slice((*byte)(buf), n)
	if err := f.vfs.vol.Write(f.vfs.ctx, goBuf, regionStart+uint64(off)); err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	newEnd := uint64(off) + uint64(n)
	f.vfs.mu.Lock()
	if newEnd > entry.FileSize {
		entry.FileSize = newEnd
		f.vfs.dirty = true
	}
	f.vfs.mu.Unlock()

	return C.SQLITE_OK
}

//export goLHTruncate
func goLHTruncate(cf *C.sqlite3_file, size C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	f.vfs.mu.Lock()
	entry := &f.vfs.sb.Regions[f.regionIdx]
	oldSize := entry.FileSize
	entry.FileSize = uint64(size)
	f.vfs.dirty = true
	regionStart := entry.RegionStart
	f.vfs.mu.Unlock()

	if uint64(size) < oldSize {
		if err := f.vfs.vol.PunchHole(f.vfs.ctx, regionStart+uint64(size), oldSize-uint64(size)); err != nil {
			return C.SQLITE_IOERR_TRUNCATE
		}
	}
	return C.SQLITE_OK
}

//export goLHSync
func goLHSync(cf *C.sqlite3_file, flags C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_FSYNC
	}

	if f.vfs.syncMode == SyncModeAsync {
		return C.SQLITE_OK
	}

	if err := f.vfs.FlushSuperblock(); err != nil {
		return C.SQLITE_IOERR_FSYNC
	}
	if err := f.vfs.vol.Flush(f.vfs.ctx); err != nil {
		return C.SQLITE_IOERR_FSYNC
	}
	return C.SQLITE_OK
}

//export goLHFileSize
func goLHFileSize(cf *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_FSTAT
	}

	f.vfs.mu.Lock()
	*pSize = C.sqlite3_int64(f.vfs.sb.Regions[f.regionIdx].FileSize)
	f.vfs.mu.Unlock()
	return C.SQLITE_OK
}

//export goLHSectorSize
func goLHSectorSize(cf *C.sqlite3_file) C.int {
	return 4096
}

//export goLHDeviceCharacteristics
func goLHDeviceCharacteristics(cf *C.sqlite3_file) C.int {
	f := fileFromC(cf)
	if f == nil {
		return 0
	}
	chars := C.SQLITE_IOCAP_ATOMIC4K | C.SQLITE_IOCAP_SAFE_APPEND | C.SQLITE_IOCAP_SEQUENTIAL
	if f.vfs.syncMode == SyncModeAsync {
		chars |= C.SQLITE_IOCAP_POWERSAFE_OVERWRITE
	}
	return C.int(chars)
}

//export goLHFileControl
func goLHFileControl(cf *C.sqlite3_file, op C.int, pArg unsafe.Pointer) C.int {
	return C.SQLITE_NOTFOUND
}

// ---------- Locking ----------
//
// SQLite lock levels: NONE(0) < SHARED(1) < RESERVED(2) < PENDING(3) < EXCLUSIVE(4)
//
// Rules:
//   - Multiple connections can hold SHARED simultaneously.
//   - Only one connection can hold RESERVED (but SHARED holders may coexist).
//   - PENDING prevents new SHARED locks (writer waiting for readers to drain).
//   - EXCLUSIVE means no other connection holds any lock > NONE.

type dbLock struct {
	mu       sync.Mutex
	cond     *sync.Cond
	shared   int  // count of SHARED holders
	reserved bool // is RESERVED held?
	pending  bool // is PENDING held?
	excl     bool // is EXCLUSIVE held?
}

func (l *dbLock) init() {
	l.cond = sync.NewCond(&l.mu)
}

func (l *dbLock) ensureInit() {
	if l.cond == nil {
		l.init()
	}
}

func (l *dbLock) tryLock(f *cvfsFile, target int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensureInit()

	cur := f.lockLevel

	switch {
	case cur == C.SQLITE_LOCK_NONE && target == C.SQLITE_LOCK_SHARED:
		// Can't get SHARED if someone holds PENDING or EXCLUSIVE.
		if l.pending || l.excl {
			return C.SQLITE_BUSY
		}
		l.shared++
		return C.SQLITE_OK

	case cur == C.SQLITE_LOCK_SHARED && target == C.SQLITE_LOCK_RESERVED:
		if l.reserved || l.excl {
			return C.SQLITE_BUSY
		}
		l.reserved = true
		return C.SQLITE_OK

	case cur == C.SQLITE_LOCK_SHARED && target == C.SQLITE_LOCK_EXCLUSIVE:
		// SHARED → EXCLUSIVE: must first acquire RESERVED, then PENDING, then EXCLUSIVE.
		if l.reserved || l.excl {
			return C.SQLITE_BUSY
		}
		l.reserved = true
		l.pending = true
		if l.shared > 1 {
			// Can't promote yet — other readers still active.
			// Revert: release RESERVED and PENDING so we don't deadlock.
			l.reserved = false
			l.pending = false
			return C.SQLITE_BUSY
		}
		// Promote to EXCLUSIVE.
		l.pending = false
		l.reserved = false
		l.shared--
		l.excl = true
		return C.SQLITE_OK

	case cur == C.SQLITE_LOCK_RESERVED && target == C.SQLITE_LOCK_EXCLUSIVE:
		// RESERVED → EXCLUSIVE: set PENDING to block new readers, wait for current readers.
		l.pending = true
		if l.shared > 1 {
			// Leave PENDING set — we own RESERVED and are waiting for readers to drain.
			// SQLite will retry via busy handler. We keep PENDING to prevent new SHARED.
			return C.SQLITE_BUSY
		}
		// All readers drained. Promote to EXCLUSIVE.
		l.pending = false
		l.reserved = false
		l.shared--
		l.excl = true
		return C.SQLITE_OK

	default:
		// Already at or above target level.
		return C.SQLITE_OK
	}
}

func (l *dbLock) unlock(f *cvfsFile, target int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensureInit()

	cur := f.lockLevel
	if target >= cur {
		return
	}

	// Release locks from highest to lowest.
	if cur >= C.SQLITE_LOCK_EXCLUSIVE && target < C.SQLITE_LOCK_EXCLUSIVE {
		l.excl = false
	}
	if cur >= C.SQLITE_LOCK_PENDING && target < C.SQLITE_LOCK_PENDING {
		l.pending = false
	}
	if cur >= C.SQLITE_LOCK_RESERVED && target < C.SQLITE_LOCK_RESERVED {
		l.reserved = false
	}

	// Handle SHARED count.
	if cur >= C.SQLITE_LOCK_EXCLUSIVE && target >= C.SQLITE_LOCK_SHARED {
		// Dropping from EXCLUSIVE back to SHARED: re-add SHARED count.
		l.shared++
	} else if cur >= C.SQLITE_LOCK_SHARED && cur < C.SQLITE_LOCK_EXCLUSIVE && target < C.SQLITE_LOCK_SHARED {
		// Dropping from SHARED (or RESERVED/PENDING) to NONE: decrement SHARED.
		l.shared--
	}
	// Note: EXCLUSIVE→NONE needs no shared count adjustment — it was
	// already decremented during the SHARED→EXCLUSIVE promotion.

	if l.cond != nil {
		l.cond.Broadcast()
	}
}

//export goLHLock
func goLHLock(cf *C.sqlite3_file, eLock C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_ERROR
	}

	// Only the main DB file needs locking.
	if f.regionIdx != RegionMainDB {
		return C.SQLITE_OK
	}

	rc := f.vfs.lock.tryLock(f, int(eLock))
	if rc == C.SQLITE_OK {
		f.lockLevel = int(eLock)
	}
	return C.int(rc)
}

//export goLHUnlock
func goLHUnlock(cf *C.sqlite3_file, eLock C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_ERROR
	}

	if f.regionIdx != RegionMainDB {
		return C.SQLITE_OK
	}

	f.vfs.lock.unlock(f, int(eLock))
	f.lockLevel = int(eLock)
	return C.SQLITE_OK
}

//export goLHCheckReservedLock
func goLHCheckReservedLock(cf *C.sqlite3_file, pOut *C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		*pOut = 0
		return C.SQLITE_OK
	}

	f.vfs.lock.mu.Lock()
	if f.vfs.lock.reserved || f.vfs.lock.pending || f.vfs.lock.excl {
		*pOut = 1
	} else {
		*pOut = 0
	}
	f.vfs.lock.mu.Unlock()
	return C.SQLITE_OK
}

// ---------- SHM (shared memory for WAL index) ----------

type shmState struct {
	mu      sync.Mutex
	regions [][]byte // each region is pgsz bytes (typically 32KB)

	// Lock slots: SQLITE_SHM_NLOCK = 8
	// For each slot, track the number of shared holders and whether exclusive is held.
	lockShared [8]int
	lockExcl   [8]bool
}

//export goLHShmMap
func goLHShmMap(cf *C.sqlite3_file, iRegion C.int, szRegion C.int, bExtend C.int, pp *unsafe.Pointer) C.int {
	f := fileFromC(cf)
	if f == nil {
		*pp = nil
		return C.SQLITE_ERROR
	}

	shm := &f.vfs.shm
	shm.mu.Lock()
	defer shm.mu.Unlock()

	idx := int(iRegion)
	sz := int(szRegion)

	// Extend if needed.
	for len(shm.regions) <= idx {
		if bExtend == 0 {
			*pp = nil
			return C.SQLITE_OK
		}
		shm.regions = append(shm.regions, make([]byte, sz))
	}

	// Return pointer to the region's backing array.
	*pp = unsafe.Pointer(&shm.regions[idx][0])
	return C.SQLITE_OK
}

//export goLHShmLock
func goLHShmLock(cf *C.sqlite3_file, offset C.int, n C.int, flags C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_ERROR
	}

	shm := &f.vfs.shm
	shm.mu.Lock()
	defer shm.mu.Unlock()

	off := int(offset)
	cnt := int(n)
	isLock := flags&C.SQLITE_SHM_LOCK != 0
	isExcl := flags&C.SQLITE_SHM_EXCLUSIVE != 0

	if isLock {
		if isExcl {
			// Acquire EXCLUSIVE on [off, off+cnt).
			for i := off; i < off+cnt; i++ {
				if shm.lockShared[i] > 0 || shm.lockExcl[i] {
					return C.SQLITE_BUSY
				}
			}
			for i := off; i < off+cnt; i++ {
				shm.lockExcl[i] = true
			}
		} else {
			// Acquire SHARED on [off, off+cnt).
			for i := off; i < off+cnt; i++ {
				if shm.lockExcl[i] {
					return C.SQLITE_BUSY
				}
			}
			for i := off; i < off+cnt; i++ {
				shm.lockShared[i]++
			}
		}
	} else {
		// Unlock.
		if isExcl {
			for i := off; i < off+cnt; i++ {
				shm.lockExcl[i] = false
			}
		} else {
			for i := off; i < off+cnt; i++ {
				if shm.lockShared[i] > 0 {
					shm.lockShared[i]--
				}
			}
		}
	}

	return C.SQLITE_OK
}

//export goLHShmBarrier
func goLHShmBarrier(cf *C.sqlite3_file) {
	// Memory barrier. sync.Mutex Lock/Unlock provides this implicitly
	// in the Go memory model. For cross-goroutine visibility we just
	// do a quick lock/unlock on the SHM mutex.
	f := fileFromC(cf)
	if f == nil {
		return
	}
	f.vfs.shm.mu.Lock()
	//nolint:staticcheck // SA2001: intentional empty critical section for memory barrier
	f.vfs.shm.mu.Unlock()
}

//export goLHShmUnmap
func goLHShmUnmap(cf *C.sqlite3_file, deleteFlag C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_OK
	}

	if deleteFlag != 0 {
		shm := &f.vfs.shm
		shm.mu.Lock()
		shm.regions = nil
		shm.mu.Unlock()
	}
	return C.SQLITE_OK
}
