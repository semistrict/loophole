// Package sqlitevfs implements a SQLite VFS backed by loophole volumes.
//
// The main database file lives on the volume. WAL, journal, and SHM are kept
// in memory. Snapshots and clones of the volume capture the database state
// atomically (after checkpoint).
//
// VolumeVFS implements [sqlite3vfs.VFS] from github.com/psanford/sqlite3vfs and
// can be registered with [sqlite3vfs.RegisterVFS] for use with mattn/go-sqlite3.
package sqlitevfs

import "github.com/psanford/sqlite3vfs"

// Re-export psanford types so callers don't need to import both packages.
type (
	OpenFlag             = sqlite3vfs.OpenFlag
	AccessFlag           = sqlite3vfs.AccessFlag
	SyncFlag             = sqlite3vfs.SyncType
	LockLevel            = sqlite3vfs.LockType
	DeviceCharacteristic = sqlite3vfs.DeviceCharacteristic
)

// Re-export psanford constants.
const (
	OpenReadOnly      = sqlite3vfs.OpenReadOnly
	OpenReadWrite     = sqlite3vfs.OpenReadWrite
	OpenCreate        = sqlite3vfs.OpenCreate
	OpenDeleteOnClose = sqlite3vfs.OpenDeleteOnClose
	OpenExclusive     = sqlite3vfs.OpenExclusive
	OpenMainDB        = sqlite3vfs.OpenMainDB
	OpenTempDB        = sqlite3vfs.OpenTempDB
	OpenMainJournal   = sqlite3vfs.OpenMainJournal
	OpenSubJournal    = sqlite3vfs.OpenSubJournal
	OpenSuperJournal  = sqlite3vfs.OpenSuperJournal
	OpenWAL           = sqlite3vfs.OpenWAL

	AccessExists    = sqlite3vfs.AccessExists
	AccessReadWrite = sqlite3vfs.AccessReadWrite

	SyncNormal   = sqlite3vfs.SyncNormal
	SyncFull     = sqlite3vfs.SyncFull
	SyncDataOnly = sqlite3vfs.SyncDataOnly

	LockNone      = sqlite3vfs.LockNone
	LockShared    = sqlite3vfs.LockShared
	LockReserved  = sqlite3vfs.LockReserved
	LockPending   = sqlite3vfs.LockPending
	LockExclusive = sqlite3vfs.LockExclusive

	IocapAtomic             = sqlite3vfs.IocapAtomic
	IocapAtomic4K           = sqlite3vfs.IocapAtomic4K
	IocapSafeAppend         = sqlite3vfs.IocapSafeAppend
	IocapSequential         = sqlite3vfs.IocapSequential
	IocapPowersafeOverwrite = sqlite3vfs.IocapPowersafeOverwrite
	IocapImmutable          = sqlite3vfs.IocapImmutable
	IocapBatchAtomic        = sqlite3vfs.IocapBatchAtomic
)

// RegisterVFS registers a VFS with the SQLite driver.
var RegisterVFS = sqlite3vfs.RegisterVFS

// SyncMode controls how xSync behaves.
type SyncMode int

const (
	// SyncModeSync flushes to S3 on every xSync call.
	SyncModeSync SyncMode = 0
	// SyncModeAsync makes xSync a no-op; data is flushed on a schedule.
	SyncModeAsync SyncMode = 1
)
