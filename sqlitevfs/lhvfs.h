// lhvfs.h — Loophole SQLite VFS (iVersion=3, io_methods iVersion=3).
//
// This VFS dispatches all I/O to Go callbacks, implements proper in-process
// locking (supporting WAL mode), and provides SHM via in-memory regions.

#ifndef LHVFS_H
#define LHVFS_H

#include "sqlite3-binding.h"

// Per-file handle embedded in sqlite3_file.
typedef struct lhvfs_file {
	sqlite3_file base;       // Must be first — SQLite casts sqlite3_file* to this.
	sqlite3_uint64 id;       // Go-side file handle ID.
} lhvfs_file;

// Register a new VFS with the given name. Returns SQLITE_OK on success.
int lhvfs_register(const char *name, int make_default);

#endif // LHVFS_H
