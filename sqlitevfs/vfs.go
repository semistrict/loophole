// Package sqlitevfs implements a SQLite VFS backed by loophole volumes.
//
// A single volume stores all SQLite-related files (main db, journal, WAL, SHM)
// in fixed regions. Snapshots and clones of the volume capture the entire
// database state atomically.
//
// VolumeVFS implements [vfs.VFS] from github.com/ncruces/go-sqlite3/vfs and
// can be registered directly with [vfs.Register] for use with real SQLite.
package sqlitevfs

import "github.com/ncruces/go-sqlite3/vfs"

// Re-export ncruces types so callers don't need to import both packages.
type (
	OpenFlag             = vfs.OpenFlag
	AccessFlag           = vfs.AccessFlag
	SyncFlag             = vfs.SyncFlag
	LockLevel            = vfs.LockLevel
	DeviceCharacteristic = vfs.DeviceCharacteristic
)

// Re-export ncruces constants.
const (
	OpenReadOnly      = vfs.OPEN_READONLY
	OpenReadWrite     = vfs.OPEN_READWRITE
	OpenCreate        = vfs.OPEN_CREATE
	OpenDeleteOnClose = vfs.OPEN_DELETEONCLOSE
	OpenExclusive     = vfs.OPEN_EXCLUSIVE
	OpenMainDB        = vfs.OPEN_MAIN_DB
	OpenTempDB        = vfs.OPEN_TEMP_DB
	OpenMainJournal   = vfs.OPEN_MAIN_JOURNAL
	OpenSubJournal    = vfs.OPEN_SUBJOURNAL
	OpenSuperJournal  = vfs.OPEN_SUPER_JOURNAL
	OpenWAL           = vfs.OPEN_WAL

	AccessExists    = vfs.ACCESS_EXISTS
	AccessReadWrite = vfs.ACCESS_READWRITE

	SyncNormal   = vfs.SYNC_NORMAL
	SyncFull     = vfs.SYNC_FULL
	SyncDataOnly = vfs.SYNC_DATAONLY

	LockNone      = vfs.LOCK_NONE
	LockShared    = vfs.LOCK_SHARED
	LockReserved  = vfs.LOCK_RESERVED
	LockPending   = vfs.LOCK_PENDING
	LockExclusive = vfs.LOCK_EXCLUSIVE

	IocapAtomic             = vfs.IOCAP_ATOMIC
	IocapAtomic4K           = vfs.IOCAP_ATOMIC4K
	IocapSafeAppend         = vfs.IOCAP_SAFE_APPEND
	IocapSequential         = vfs.IOCAP_SEQUENTIAL
	IocapPowersafeOverwrite = vfs.IOCAP_POWERSAFE_OVERWRITE
	IocapImmutable          = vfs.IOCAP_IMMUTABLE
	IocapBatchAtomic        = vfs.IOCAP_BATCH_ATOMIC
)

// SyncMode controls how xSync behaves.
type SyncMode int

const (
	// SyncModeSync flushes to S3 on every xSync call.
	SyncModeSync SyncMode = 0
	// SyncModeAsync makes xSync a no-op; data is flushed on a schedule.
	SyncModeAsync SyncMode = 1
)
