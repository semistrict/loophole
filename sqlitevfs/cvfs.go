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
	"errors"
	"fmt"
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
// The main database lives on the volume; WAL, journal, and SHM are in memory.
type CVFS struct {
	name string
	core *fileCore

	// In-process lock state for the main database file.
	lock dbLock

	// Shared memory for WAL index, shared across all connections.
	shm shmState
}

// NewCVFS creates and registers a new C VFS backed by a loophole Volume.
// The volume must already be formatted with FormatVolume.
func NewCVFS(ctx context.Context, vol loophole.Volume, name string, syncMode SyncMode) (*CVFS, error) {
	core, err := newFileCore(ctx, vol, syncMode)
	if err != nil {
		return nil, fmt.Errorf("open CVFS: %w", err)
	}

	v := &CVFS{
		name: name,
		core: core,
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

// FlushHeader persists the WAL to the volume and writes the header if dirty.
func (v *CVFS) FlushHeader() error {
	return v.core.FlushHeader()
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
	kind      fileKind
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
	id := int(nextFileID.Add(1))
	kind, out, err := v.core.OpenFile(id, name, int(flags))
	if err != nil {
		return C.SQLITE_CANTOPEN
	}
	if outFlags != nil {
		*outFlags = C.int(out)
	}

	f := &cvfsFile{
		vfs:       v,
		kind:      kind,
		lockLevel: C.SQLITE_LOCK_NONE,
	}

	fileMu.Lock()
	fileMap[uint64(id)] = f
	fileMu.Unlock()

	lf := (*C.lhvfs_file)(unsafe.Pointer(cf))
	C.memset(unsafe.Pointer(lf), 0, C.size_t(unsafe.Sizeof(C.lhvfs_file{})))
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
	if err := v.core.DeleteFile(name); err != nil {
		return C.SQLITE_IOERR_DELETE
	}
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
	if v.core.AccessFile(name, int(flags)) {
		*pOut = 1
	} else {
		*pOut = 0
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

	if err := f.vfs.core.CloseFile(int(id)); err != nil {
		slog.Warn("lhvfs: close flush header", "error", err)
	}

	return C.SQLITE_OK
}

//export goLHRead
func goLHRead(cf *C.sqlite3_file, buf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_READ
	}

	off := int64(iOfst)
	n := int(iAmt)
	goBuf := unsafe.Slice((*byte)(buf), n)
	shortRead, err := f.vfs.core.ReadFile(int(((*C.lhvfs_file)(unsafe.Pointer(cf))).id), goBuf, off)
	if err != nil {
		return C.SQLITE_IOERR_READ
	}
	if shortRead {
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

	off := int64(iOfst)
	n := int(iAmt)
	goBuf := unsafe.Slice((*byte)(buf), n)
	if err := f.vfs.core.WriteFile(int(((*C.lhvfs_file)(unsafe.Pointer(cf))).id), goBuf, off); err != nil {
		if errors.Is(err, errVolumeFull) {
			return C.SQLITE_FULL
		}
		return C.SQLITE_IOERR_WRITE
	}
	return C.SQLITE_OK
}

//export goLHTruncate
func goLHTruncate(cf *C.sqlite3_file, size C.sqlite3_int64) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_TRUNCATE
	}
	if err := f.vfs.core.TruncateFile(int(((*C.lhvfs_file)(unsafe.Pointer(cf))).id), int64(size)); err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}
	return C.SQLITE_OK
}

//export goLHSync
func goLHSync(cf *C.sqlite3_file, flags C.int) C.int {
	f := fileFromC(cf)
	if f == nil {
		return C.SQLITE_IOERR_FSYNC
	}
	if err := f.vfs.core.SyncFile(); err != nil {
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
	size, err := f.vfs.core.FileSize(int(((*C.lhvfs_file)(unsafe.Pointer(cf))).id))
	if err != nil {
		return C.SQLITE_IOERR_FSTAT
	}
	*pSize = C.sqlite3_int64(size)
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
	if f.vfs.core.SyncMode() == SyncModeAsync {
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
		if l.reserved || l.excl {
			return C.SQLITE_BUSY
		}
		l.reserved = true
		l.pending = true
		if l.shared > 1 {
			l.reserved = false
			l.pending = false
			return C.SQLITE_BUSY
		}
		l.pending = false
		l.reserved = false
		l.shared--
		l.excl = true
		return C.SQLITE_OK

	case cur == C.SQLITE_LOCK_RESERVED && target == C.SQLITE_LOCK_EXCLUSIVE:
		l.pending = true
		if l.shared > 1 {
			return C.SQLITE_BUSY
		}
		l.pending = false
		l.reserved = false
		l.shared--
		l.excl = true
		return C.SQLITE_OK

	default:
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

	if cur >= C.SQLITE_LOCK_EXCLUSIVE && target < C.SQLITE_LOCK_EXCLUSIVE {
		l.excl = false
	}
	if cur >= C.SQLITE_LOCK_PENDING && target < C.SQLITE_LOCK_PENDING {
		l.pending = false
	}
	if cur >= C.SQLITE_LOCK_RESERVED && target < C.SQLITE_LOCK_RESERVED {
		l.reserved = false
	}

	if cur >= C.SQLITE_LOCK_EXCLUSIVE && target >= C.SQLITE_LOCK_SHARED {
		l.shared++
	} else if cur >= C.SQLITE_LOCK_SHARED && cur < C.SQLITE_LOCK_EXCLUSIVE && target < C.SQLITE_LOCK_SHARED {
		l.shared--
	}

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
	if f.kind != fileMainDB {
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

	if f.kind != fileMainDB {
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

	for len(shm.regions) <= idx {
		if bExtend == 0 {
			*pp = nil
			return C.SQLITE_OK
		}
		shm.regions = append(shm.regions, make([]byte, sz))
	}

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
			for i := off; i < off+cnt; i++ {
				if shm.lockShared[i] > 0 || shm.lockExcl[i] {
					return C.SQLITE_BUSY
				}
			}
			for i := off; i < off+cnt; i++ {
				shm.lockExcl[i] = true
			}
		} else {
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
