package sqlitevfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBCreateAndOpen(t *testing.T) {
	mgr := testManager(t)

	db, err := Create(t.Context(), mgr, "test-db")
	require.NoError(t, err)

	// VFS should be usable.
	vfs := db.VFS()
	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync(SyncFull))
	require.NoError(t, f.Close())

	require.NoError(t, db.Close(t.Context()))

	// Reopen.
	db2, err := Open(t.Context(), mgr, "test-db")
	require.NoError(t, err)

	vfs2 := db2.VFS()
	f2, _, err := vfs2.Open("main.db", OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	buf := make([]byte, 5)
	n, err := f2.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), buf)
	require.NoError(t, f2.Close())

	require.NoError(t, db2.Close(t.Context()))
}

func TestDBSnapshot(t *testing.T) {
	mgr := testManager(t)

	db, err := Create(t.Context(), mgr, "snap-db")
	require.NoError(t, err)

	// Write data.
	vfs := db.VFS()
	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("snapshot data"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Snapshot.
	require.NoError(t, db.Snapshot(t.Context(), "snap1"))

	// Original DB still works.
	f2, _, err := vfs.Open("main.db", OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	buf := make([]byte, 13)
	_, err = f2.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("snapshot data"), buf)
	require.NoError(t, f2.Close())

	require.NoError(t, db.Close(t.Context()))
}

func TestDBBranch(t *testing.T) {
	mgr := testManager(t)

	db, err := Create(t.Context(), mgr, "branch-db")
	require.NoError(t, err)

	// Write initial data.
	vfs := db.VFS()
	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Branch.
	branch, err := db.Branch(t.Context(), "branch1")
	require.NoError(t, err)

	// Branch should see original data.
	bvfs := branch.VFS()
	bf, _, err := bvfs.Open("main.db", OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	buf := make([]byte, 8)
	_, err = bf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), buf)

	// Write different data in branch.
	_, err = bf.WriteAt([]byte("branched"), 0)
	require.NoError(t, err)
	require.NoError(t, bf.Close())

	// Original should still have "original".
	f2, _, err := vfs.Open("main.db", OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	buf2 := make([]byte, 8)
	_, err = f2.ReadAt(buf2, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), buf2)
	require.NoError(t, f2.Close())

	// Branch should have "branched".
	bf2, _, err := bvfs.Open("main.db", OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	buf3 := make([]byte, 8)
	_, err = bf2.ReadAt(buf3, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("branched"), buf3)
	require.NoError(t, bf2.Close())

	require.NoError(t, branch.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestDBCreateTooSmall(t *testing.T) {
	mgr := testManager(t)

	_, err := Create(t.Context(), mgr, "tiny", WithSize(64*1024*1024))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestDBAsyncMode(t *testing.T) {
	mgr := testManager(t)

	db, err := Create(t.Context(), mgr, "async-db", WithSyncMode(SyncModeAsync))
	require.NoError(t, err)

	// VFS should report async mode.
	vvfs := db.VFS()
	require.Equal(t, SyncModeAsync, vvfs.SyncMode())

	// Write and explicit flush.
	f, _, err := db.VFS().Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("async data"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, db.Flush(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestDBDoubleClose(t *testing.T) {
	mgr := testManager(t)

	db, err := Create(t.Context(), mgr, "double-close")
	require.NoError(t, err)

	require.NoError(t, db.Close(t.Context()))
	// Second close should be a no-op (sync.Once).
	require.NoError(t, db.Close(t.Context()))
}
