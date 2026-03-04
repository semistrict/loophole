//go:build linux

package e2e

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/vfs"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lsm"
	"github.com/semistrict/loophole/sqlitevfs"
)

// newSQLiteDB creates a fresh SQLite database on a loophole volume backed by S3.
// Returns the open *sql.DB and the sqlitevfs.DB handle (for flush/snapshot/branch).
func newSQLiteDB(t *testing.T, name string) (*sql.DB, *sqlitevfs.DB) {
	t.Helper()
	trackMetrics(t)

	inst := uniqueInstance(t)
	ctx := t.Context()

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{}, nil)
	t.Cleanup(func() { vm.Close(ctx) })

	db, err := sqlitevfs.Create(ctx, vm, name)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close(ctx) })

	vfsName := fmt.Sprintf("e2e-%s-%s", t.Name(), name)
	vfs.Register(vfsName, db.VFS())

	sqlDB, err := driver.Open("file:main.db?vfs=" + vfsName)
	require.NoError(t, err)
	t.Cleanup(func() { sqlDB.Close() })

	return sqlDB, db
}

// openSQLiteDB opens an existing SQLite database volume.
func openSQLiteDB(t *testing.T, vm loophole.VolumeManager, name, vfsName string) (*sql.DB, *sqlitevfs.DB) {
	t.Helper()
	ctx := t.Context()

	db, err := sqlitevfs.Open(ctx, vm, name)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close(ctx) })

	vfs.Register(vfsName, db.VFS())

	sqlDB, err := driver.Open("file:main.db?vfs=" + vfsName)
	require.NoError(t, err)
	t.Cleanup(func() { sqlDB.Close() })

	return sqlDB, db
}

func TestE2E_SQLiteCreateInsertQuery(t *testing.T) {
	sqlDB, _ := newSQLiteDB(t, "basic")

	_, err := sqlDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "alice", "alice@example.com")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "bob", "bob@example.com")
	require.NoError(t, err)

	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	var name, email string
	err = sqlDB.QueryRow("SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "alice", name)
	require.Equal(t, "alice@example.com", email)
}

func TestE2E_SQLiteFlushAndReopen(t *testing.T) {
	inst := uniqueInstance(t)
	ctx := t.Context()
	trackMetrics(t)

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	cacheDir := t.TempDir()
	vm := lsm.NewVolumeManager(store, cacheDir, lsm.Config{}, nil)

	// Create and populate.
	db, err := sqlitevfs.Create(ctx, vm, "reopen-test")
	require.NoError(t, err)

	vfs.Register("e2e-reopen-1", db.VFS())
	sqlDB, err := driver.Open("file:main.db?vfs=e2e-reopen-1")
	require.NoError(t, err)

	_, err = sqlDB.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO kv VALUES ('hello', 'world')")
	require.NoError(t, err)

	// Flush + close.
	require.NoError(t, sqlDB.Close())
	require.NoError(t, db.Flush(ctx))
	require.NoError(t, db.Close(ctx))
	vm.Close(ctx)

	// Reopen with a fresh manager (simulates restart).
	vm2 := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{}, nil)
	defer vm2.Close(ctx)

	db2, err := sqlitevfs.Open(ctx, vm2, "reopen-test")
	require.NoError(t, err)
	defer db2.Close(ctx)

	vfs.Register("e2e-reopen-2", db2.VFS())
	sqlDB2, err := driver.Open("file:main.db?vfs=e2e-reopen-2")
	require.NoError(t, err)
	defer sqlDB2.Close()

	var value string
	err = sqlDB2.QueryRow("SELECT value FROM kv WHERE key = 'hello'").Scan(&value)
	require.NoError(t, err)
	require.Equal(t, "world", value)
}

func TestE2E_SQLiteSnapshot(t *testing.T) {
	inst := uniqueInstance(t)
	ctx := t.Context()
	trackMetrics(t)

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{}, nil)
	defer vm.Close(ctx)

	// Create and populate.
	db, err := sqlitevfs.Create(ctx, vm, "snap-parent")
	require.NoError(t, err)

	vfs.Register("e2e-snap-parent", db.VFS())
	sqlDB, err := driver.Open("file:main.db?vfs=e2e-snap-parent")
	require.NoError(t, err)

	_, err = sqlDB.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO items VALUES (1, 'before-snapshot')")
	require.NoError(t, err)

	// Snapshot.
	require.NoError(t, sqlDB.Close())
	require.NoError(t, db.Snapshot(ctx, "snap-child"))

	// Write more to parent after snapshot.
	vfs.Register("e2e-snap-parent-2", db.VFS())
	sqlDB, err = driver.Open("file:main.db?vfs=e2e-snap-parent-2")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO items VALUES (2, 'after-snapshot')")
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	require.NoError(t, db.Close(ctx))

	// Open snapshot — should only see the pre-snapshot data.
	snapDB, err := sqlitevfs.OpenSnapshot(ctx, vm, "snap-child")
	require.NoError(t, err)
	defer snapDB.Close(ctx)

	vfs.Register("e2e-snap-child", snapDB.VFS())
	snapSQL, err := driver.Open("file:main.db?vfs=e2e-snap-child&mode=ro")
	require.NoError(t, err)
	defer snapSQL.Close()

	var val string
	err = snapSQL.QueryRow("SELECT val FROM items WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "before-snapshot", val)

	var count int
	err = snapSQL.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestE2E_SQLiteBranch(t *testing.T) {
	inst := uniqueInstance(t)
	ctx := t.Context()
	trackMetrics(t)

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{}, nil)
	defer vm.Close(ctx)

	// Create parent with initial data.
	parentDB, err := sqlitevfs.Create(ctx, vm, "branch-parent")
	require.NoError(t, err)

	vfs.Register("e2e-branch-parent", parentDB.VFS())
	parentSQL, err := driver.Open("file:main.db?vfs=e2e-branch-parent")
	require.NoError(t, err)

	_, err = parentSQL.Exec("CREATE TABLE counters (name TEXT PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)
	_, err = parentSQL.Exec("INSERT INTO counters VALUES ('hits', 100)")
	require.NoError(t, err)
	require.NoError(t, parentSQL.Close())

	// Branch.
	branchDB, err := parentDB.Branch(ctx, "branch-child")
	require.NoError(t, err)
	defer branchDB.Close(ctx)
	require.NoError(t, parentDB.Close(ctx))

	// Write to branch — should be independent.
	vfs.Register("e2e-branch-child", branchDB.VFS())
	branchSQL, err := driver.Open("file:main.db?vfs=e2e-branch-child")
	require.NoError(t, err)
	defer branchSQL.Close()

	_, err = branchSQL.Exec("UPDATE counters SET val = 200 WHERE name = 'hits'")
	require.NoError(t, err)

	var val int
	err = branchSQL.QueryRow("SELECT val FROM counters WHERE name = 'hits'").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, 200, val)

	// Reopen parent — should still have original value.
	parentDB2, err := sqlitevfs.Open(ctx, vm, "branch-parent")
	require.NoError(t, err)
	defer parentDB2.Close(ctx)

	vfs.Register("e2e-branch-parent-2", parentDB2.VFS())
	parentSQL2, err := driver.Open("file:main.db?vfs=e2e-branch-parent-2&mode=ro")
	require.NoError(t, err)
	defer parentSQL2.Close()

	err = parentSQL2.QueryRow("SELECT val FROM counters WHERE name = 'hits'").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, 100, val)
}

func TestE2E_SQLiteLargeDataset(t *testing.T) {
	sqlDB, _ := newSQLiteDB(t, "large")

	_, err := sqlDB.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, ts TEXT, msg TEXT)")
	require.NoError(t, err)

	// Insert 10k rows in a transaction.
	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare("INSERT INTO logs (ts, msg) VALUES (?, ?)")
	require.NoError(t, err)
	for i := range 10000 {
		_, err = stmt.Exec(fmt.Sprintf("2026-01-%05d", i), fmt.Sprintf("log message %d", i))
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	// Verify count.
	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM logs").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10000, count)

	// Range query.
	var rangeCount int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM logs WHERE ts BETWEEN '2026-01-05000' AND '2026-01-05999'").Scan(&rangeCount)
	require.NoError(t, err)
	require.Equal(t, 1000, rangeCount)
}
