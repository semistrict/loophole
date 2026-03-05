// lhvfs.c — Loophole SQLite VFS (iVersion=3, io_methods iVersion=3).
//
// Thin C wrappers that dispatch to Go via //export callbacks.
// All state (locking, SHM, file I/O) lives on the Go side.

#include "lhvfs.h"
#include <stdlib.h>
#include <string.h>

// Go-exported callbacks (defined in cvfs.go).
extern int goLHOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);
extern int goLHDelete(sqlite3_vfs*, const char*, int);
extern int goLHAccess(sqlite3_vfs*, const char*, int, int*);
extern int goLHFullPathname(sqlite3_vfs*, const char*, int, char*);
extern int goLHRandomness(sqlite3_vfs*, int, char*);
extern int goLHSleep(sqlite3_vfs*, int);
extern int goLHCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

extern int goLHClose(sqlite3_file*);
extern int goLHRead(sqlite3_file*, void*, int, sqlite3_int64);
extern int goLHWrite(sqlite3_file*, const void*, int, sqlite3_int64);
extern int goLHTruncate(sqlite3_file*, sqlite3_int64);
extern int goLHSync(sqlite3_file*, int);
extern int goLHFileSize(sqlite3_file*, sqlite3_int64*);
extern int goLHLock(sqlite3_file*, int);
extern int goLHUnlock(sqlite3_file*, int);
extern int goLHCheckReservedLock(sqlite3_file*, int*);
extern int goLHFileControl(sqlite3_file*, int, void*);
extern int goLHSectorSize(sqlite3_file*);
extern int goLHDeviceCharacteristics(sqlite3_file*);

extern int goLHShmMap(sqlite3_file*, int, int, int, void volatile**);
extern int goLHShmLock(sqlite3_file*, int, int, int);
extern void goLHShmBarrier(sqlite3_file*);
extern int goLHShmUnmap(sqlite3_file*, int);

// --- io_methods v3 wrappers ---

static int lhClose(sqlite3_file *f) { return goLHClose(f); }
static int lhRead(sqlite3_file *f, void *b, int n, sqlite3_int64 o) { return goLHRead(f, b, n, o); }
static int lhWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 o) { return goLHWrite(f, b, n, o); }
static int lhTruncate(sqlite3_file *f, sqlite3_int64 s) { return goLHTruncate(f, s); }
static int lhSync(sqlite3_file *f, int flags) { return goLHSync(f, flags); }
static int lhFileSize(sqlite3_file *f, sqlite3_int64 *s) { return goLHFileSize(f, s); }
static int lhLock(sqlite3_file *f, int l) { return goLHLock(f, l); }
static int lhUnlock(sqlite3_file *f, int l) { return goLHUnlock(f, l); }
static int lhCheckReservedLock(sqlite3_file *f, int *o) { return goLHCheckReservedLock(f, o); }
static int lhFileControl(sqlite3_file *f, int op, void *a) { return goLHFileControl(f, op, a); }
static int lhSectorSize(sqlite3_file *f) { return goLHSectorSize(f); }
static int lhDeviceCharacteristics(sqlite3_file *f) { return goLHDeviceCharacteristics(f); }

// v2: SHM
static int lhShmMap(sqlite3_file *f, int r, int s, int w, void volatile **pp) { return goLHShmMap(f, r, s, w, pp); }
static int lhShmLock(sqlite3_file *f, int o, int n, int fl) { return goLHShmLock(f, o, n, fl); }
static void lhShmBarrier(sqlite3_file *f) { goLHShmBarrier(f); }
static int lhShmUnmap(sqlite3_file *f, int del) { return goLHShmUnmap(f, del); }

// v3: memory-mapped I/O (not supported — return NULL pointer)
static int lhFetch(sqlite3_file *f, sqlite3_int64 off, int amt, void **pp) {
	(void)f; (void)off; (void)amt;
	*pp = 0;
	return SQLITE_OK;
}
static int lhUnfetch(sqlite3_file *f, sqlite3_int64 off, void *p) {
	(void)f; (void)off; (void)p;
	return SQLITE_OK;
}

static const sqlite3_io_methods lhvfs_io_methods = {
	3,                          /* iVersion */
	lhClose,
	lhRead,
	lhWrite,
	lhTruncate,
	lhSync,
	lhFileSize,
	lhLock,
	lhUnlock,
	lhCheckReservedLock,
	lhFileControl,
	lhSectorSize,
	lhDeviceCharacteristics,
	/* v2 */
	lhShmMap,
	lhShmLock,
	lhShmBarrier,
	lhShmUnmap,
	/* v3 */
	lhFetch,
	lhUnfetch,
};

// --- VFS wrappers ---

static int lhOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file, int flags, int *outFlags) {
	int rc = goLHOpen(vfs, name, file, flags, outFlags);
	if (rc == SQLITE_OK) {
		file->pMethods = &lhvfs_io_methods;
	}
	return rc;
}

static int lhDelete(sqlite3_vfs *vfs, const char *name, int syncDir) {
	return goLHDelete(vfs, name, syncDir);
}

static int lhAccess(sqlite3_vfs *vfs, const char *name, int flags, int *out) {
	return goLHAccess(vfs, name, flags, out);
}

static int lhFullPathname(sqlite3_vfs *vfs, const char *name, int nOut, char *zOut) {
	return goLHFullPathname(vfs, name, nOut, zOut);
}

static int lhRandomness(sqlite3_vfs *vfs, int nByte, char *zOut) {
	return goLHRandomness(vfs, nByte, zOut);
}

static int lhSleep(sqlite3_vfs *vfs, int us) {
	return goLHSleep(vfs, us);
}

static int lhCurrentTime(sqlite3_vfs *vfs, double *pTime) {
	sqlite3_int64 t = 0;
	int rc = goLHCurrentTimeInt64(vfs, &t);
	*pTime = t / 86400000.0;
	return rc;
}

static int lhGetLastError(sqlite3_vfs *vfs, int nBuf, char *zBuf) {
	(void)vfs;
	if (nBuf > 0) zBuf[0] = '\0';
	return SQLITE_OK;
}

static int lhCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *pTime) {
	return goLHCurrentTimeInt64(vfs, pTime);
}

// v3: system call overrides (not supported)
static int lhSetSystemCall(sqlite3_vfs *vfs, const char *name, sqlite3_syscall_ptr ptr) {
	(void)vfs; (void)name; (void)ptr;
	return SQLITE_NOTFOUND;
}
static sqlite3_syscall_ptr lhGetSystemCall(sqlite3_vfs *vfs, const char *name) {
	(void)vfs; (void)name;
	return 0;
}
static const char *lhNextSystemCall(sqlite3_vfs *vfs, const char *name) {
	(void)vfs; (void)name;
	return 0;
}

int lhvfs_register(const char *name, int make_default) {
	sqlite3_vfs *vfs = (sqlite3_vfs *)calloc(1, sizeof(sqlite3_vfs));
	if (!vfs) return SQLITE_NOMEM;

	// Delegate Dl* to the default VFS.
	sqlite3_vfs *dflt = sqlite3_vfs_find(0);

	vfs->iVersion      = 3;
	vfs->szOsFile      = (int)sizeof(lhvfs_file);
	vfs->mxPathname     = 512;
	vfs->zName          = name;  // Caller must keep name alive.
	vfs->xOpen          = lhOpen;
	vfs->xDelete        = lhDelete;
	vfs->xAccess        = lhAccess;
	vfs->xFullPathname  = lhFullPathname;
	vfs->xDlOpen        = dflt->xDlOpen;
	vfs->xDlError       = dflt->xDlError;
	vfs->xDlSym         = dflt->xDlSym;
	vfs->xDlClose       = dflt->xDlClose;
	vfs->xRandomness    = lhRandomness;
	vfs->xSleep         = lhSleep;
	vfs->xCurrentTime   = lhCurrentTime;
	vfs->xGetLastError  = lhGetLastError;
	/* v2 */
	vfs->xCurrentTimeInt64 = lhCurrentTimeInt64;
	/* v3 */
	vfs->xSetSystemCall  = lhSetSystemCall;
	vfs->xGetSystemCall  = lhGetSystemCall;
	vfs->xNextSystemCall = lhNextSystemCall;

	return sqlite3_vfs_register(vfs, make_default);
}
