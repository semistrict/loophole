package sqlitevfs_test

import (
	"database/sql"
	"testing"

	"github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/vfs"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lsm"
	"github.com/semistrict/loophole/sqlitevfs"
)

func conformanceManager(t *testing.T) loophole.VolumeManager {
	t.Helper()
	store := loophole.NewMemStore()
	m := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{
		FlushThreshold:  16 * lsm.PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * lsm.PageSize,
	}, nil)
	t.Cleanup(func() { m.Close(t.Context()) })
	return m
}

func openConformanceDB(t *testing.T, mgr loophole.VolumeManager, name string) (*sql.DB, *sqlitevfs.DB) {
	t.Helper()

	db, err := sqlitevfs.Create(t.Context(), mgr, name)
	require.NoError(t, err)

	vfsName := "loophole-" + name
	vfs.Register(vfsName, db.VFS())

	sqlDB, err := driver.Open("file:main.db?vfs=" + vfsName)
	require.NoError(t, err)

	t.Cleanup(func() {
		sqlDB.Close()
		db.Close(t.Context())
	})

	return sqlDB, db
}

// registerAndOpenSQL registers a VFS and opens a SQL connection to it.
// The vfsName must be unique across the process (ncruces uses a global registry).
func registerAndOpenSQL(t *testing.T, db *sqlitevfs.DB, vfsName string) *sql.DB {
	t.Helper()
	vfs.Register(vfsName, db.VFS())
	sqlDB, err := driver.Open("file:main.db?vfs=" + vfsName)
	require.NoError(t, err)
	return sqlDB
}

func TestConformanceBasicCRUD(t *testing.T) {
	mgr := conformanceManager(t)
	sqlDB, _ := openConformanceDB(t, mgr, "crud")

	// Create table.
	_, err := sqlDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	require.NoError(t, err)

	// Insert.
	_, err = sqlDB.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Bob", "bob@example.com")
	require.NoError(t, err)

	// Select.
	var name, email string
	err = sqlDB.QueryRow("SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)

	// Count.
	var count int
	err = sqlDB.QueryRow("SELECT count(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Update.
	_, err = sqlDB.Exec("UPDATE users SET email = ? WHERE name = ?", "alice@new.com", "Alice")
	require.NoError(t, err)

	err = sqlDB.QueryRow("SELECT email FROM users WHERE name = 'Alice'").Scan(&email)
	require.NoError(t, err)
	require.Equal(t, "alice@new.com", email)

	// Delete.
	_, err = sqlDB.Exec("DELETE FROM users WHERE name = 'Bob'")
	require.NoError(t, err)

	err = sqlDB.QueryRow("SELECT count(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestConformanceTransaction(t *testing.T) {
	mgr := conformanceManager(t)
	sqlDB, _ := openConformanceDB(t, mgr, "txn")

	_, err := sqlDB.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	require.NoError(t, err)

	// Committed transaction.
	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO kv VALUES ('a', '1')")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO kv VALUES ('b', '2')")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var val string
	err = sqlDB.QueryRow("SELECT value FROM kv WHERE key = 'a'").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "1", val)

	// Rolled back transaction.
	tx2, err := sqlDB.Begin()
	require.NoError(t, err)
	_, err = tx2.Exec("INSERT INTO kv VALUES ('c', '3')")
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback())

	var count int
	err = sqlDB.QueryRow("SELECT count(*) FROM kv WHERE key = 'c'").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestConformanceBranchThenDiverge(t *testing.T) {
	mgr := conformanceManager(t)

	db, err := sqlitevfs.Create(t.Context(), mgr, "branch-diverge")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-branch-diverge")

	_, err = sqlDB.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO items (val) VALUES ('original')")
	require.NoError(t, err)

	// Close SQL connection before branching.
	require.NoError(t, sqlDB.Close())

	// Branch.
	branchDB, err := db.Branch(t.Context(), "diverged")
	require.NoError(t, err)

	branchSQL := registerAndOpenSQL(t, branchDB, "loophole-diverged")

	// Branch sees original data.
	var val string
	err = branchSQL.QueryRow("SELECT val FROM items WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "original", val)

	// Diverge: insert in branch.
	_, err = branchSQL.Exec("INSERT INTO items (val) VALUES ('branch-only')")
	require.NoError(t, err)

	var branchCount int
	err = branchSQL.QueryRow("SELECT count(*) FROM items").Scan(&branchCount)
	require.NoError(t, err)
	require.Equal(t, 2, branchCount)

	// Reopen original — should still have only 1 row.
	sqlDB2 := registerAndOpenSQL(t, db, "loophole-branch-diverge-reopen")

	var origCount int
	err = sqlDB2.QueryRow("SELECT count(*) FROM items").Scan(&origCount)
	require.NoError(t, err)
	require.Equal(t, 1, origCount)

	require.NoError(t, branchSQL.Close())
	require.NoError(t, sqlDB2.Close())
	require.NoError(t, branchDB.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestConformanceSnapshotPreservesState(t *testing.T) {
	mgr := conformanceManager(t)

	db, err := sqlitevfs.Create(t.Context(), mgr, "snap-preserve")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-snap-preserve")

	// Insert initial rows.
	_, err = sqlDB.Exec("CREATE TABLE counters (name TEXT PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO counters VALUES ('hits', 100)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO counters VALUES ('misses', 5)")
	require.NoError(t, err)

	// Snapshot WITHOUT closing the SQL connection — db.Snapshot flushes
	// the volume, and committed data is already on disk via xSync.
	require.NoError(t, db.Snapshot(t.Context(), "snap-v1"))

	// Continue writing to the original through the same connection.
	_, err = sqlDB.Exec("UPDATE counters SET val = 200 WHERE name = 'hits'")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO counters VALUES ('errors', 42)")
	require.NoError(t, err)

	// Original should see the updates.
	var hits, errors int
	err = sqlDB.QueryRow("SELECT val FROM counters WHERE name = 'hits'").Scan(&hits)
	require.NoError(t, err)
	require.Equal(t, 200, hits)
	err = sqlDB.QueryRow("SELECT val FROM counters WHERE name = 'errors'").Scan(&errors)
	require.NoError(t, err)
	require.Equal(t, 42, errors)

	var count int
	err = sqlDB.QueryRow("SELECT count(*) FROM counters").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	require.NoError(t, sqlDB.Close())
	require.NoError(t, db.Close(t.Context()))

	// Open the snapshot directly — it's read-only, no clone needed.
	snapDB, err := sqlitevfs.OpenSnapshot(t.Context(), mgr, "snap-v1")
	require.NoError(t, err)

	snapSQL := registerAndOpenSQL(t, snapDB, "loophole-snap-v1")

	// Snapshot should have the original 2 rows with original values.
	var snapHits int
	err = snapSQL.QueryRow("SELECT val FROM counters WHERE name = 'hits'").Scan(&snapHits)
	require.NoError(t, err)
	require.Equal(t, 100, snapHits)

	var snapCount int
	err = snapSQL.QueryRow("SELECT count(*) FROM counters").Scan(&snapCount)
	require.NoError(t, err)
	require.Equal(t, 2, snapCount) // no 'errors' row

	require.NoError(t, snapSQL.Close())
	require.NoError(t, snapDB.Close(t.Context()))
}

func TestConformanceBranchMultiTable(t *testing.T) {
	mgr := conformanceManager(t)

	db, err := sqlitevfs.Create(t.Context(), mgr, "branch-multi")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-branch-multi")

	// Create multiple tables with data.
	_, err = sqlDB.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE tags (id INTEGER PRIMARY KEY, post_id INTEGER, tag TEXT);
	`)
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO posts VALUES (1, 1, 'Hello'), (2, 1, 'World'), (3, 2, 'Bye')")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO tags VALUES (1, 1, 'greeting'), (2, 2, 'greeting'), (3, 3, 'farewell')")
	require.NoError(t, err)

	require.NoError(t, sqlDB.Close())

	// Branch.
	branchDB, err := db.Branch(t.Context(), "branch-multi-fork")
	require.NoError(t, err)

	branchSQL := registerAndOpenSQL(t, branchDB, "loophole-branch-multi-fork")

	// Branch: delete a user and their posts/tags.
	_, err = branchSQL.Exec("DELETE FROM tags WHERE post_id IN (SELECT id FROM posts WHERE user_id = 1)")
	require.NoError(t, err)
	_, err = branchSQL.Exec("DELETE FROM posts WHERE user_id = 1")
	require.NoError(t, err)
	_, err = branchSQL.Exec("DELETE FROM users WHERE id = 1")
	require.NoError(t, err)

	// Branch: add new user.
	_, err = branchSQL.Exec("INSERT INTO users VALUES (3, 'Charlie')")
	require.NoError(t, err)

	// Branch should have: users=2 (Bob, Charlie), posts=1, tags=1.
	var branchUsers, branchPosts, branchTags int
	err = branchSQL.QueryRow("SELECT count(*) FROM users").Scan(&branchUsers)
	require.NoError(t, err)
	require.Equal(t, 2, branchUsers)
	err = branchSQL.QueryRow("SELECT count(*) FROM posts").Scan(&branchPosts)
	require.NoError(t, err)
	require.Equal(t, 1, branchPosts)
	err = branchSQL.QueryRow("SELECT count(*) FROM tags").Scan(&branchTags)
	require.NoError(t, err)
	require.Equal(t, 1, branchTags)

	// Original should be unchanged: users=2 (Alice, Bob), posts=3, tags=3.
	origSQL := registerAndOpenSQL(t, db, "loophole-branch-multi-orig")
	var origUsers, origPosts, origTags int
	err = origSQL.QueryRow("SELECT count(*) FROM users").Scan(&origUsers)
	require.NoError(t, err)
	require.Equal(t, 2, origUsers)
	err = origSQL.QueryRow("SELECT count(*) FROM posts").Scan(&origPosts)
	require.NoError(t, err)
	require.Equal(t, 3, origPosts)
	err = origSQL.QueryRow("SELECT count(*) FROM tags").Scan(&origTags)
	require.NoError(t, err)
	require.Equal(t, 3, origTags)

	require.NoError(t, branchSQL.Close())
	require.NoError(t, origSQL.Close())
	require.NoError(t, branchDB.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestConformanceBranchChain(t *testing.T) {
	mgr := conformanceManager(t)

	// Create → branch → branch (chain of 3 generations).
	db, err := sqlitevfs.Create(t.Context(), mgr, "chain-root")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-chain-root")
	_, err = sqlDB.Exec("CREATE TABLE log (id INTEGER PRIMARY KEY, gen INTEGER, msg TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO log (gen, msg) VALUES (1, 'root')")
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	// First branch.
	branch1, err := db.Branch(t.Context(), "chain-gen2")
	require.NoError(t, err)

	b1SQL := registerAndOpenSQL(t, branch1, "loophole-chain-gen2")
	_, err = b1SQL.Exec("INSERT INTO log (gen, msg) VALUES (2, 'gen2')")
	require.NoError(t, err)
	require.NoError(t, b1SQL.Close())

	// Second branch (from first branch).
	branch2, err := branch1.Branch(t.Context(), "chain-gen3")
	require.NoError(t, err)

	b2SQL := registerAndOpenSQL(t, branch2, "loophole-chain-gen3")
	_, err = b2SQL.Exec("INSERT INTO log (gen, msg) VALUES (3, 'gen3')")
	require.NoError(t, err)

	// Gen3 should see all three rows.
	var gen3Count int
	err = b2SQL.QueryRow("SELECT count(*) FROM log").Scan(&gen3Count)
	require.NoError(t, err)
	require.Equal(t, 3, gen3Count)

	// Verify each row.
	rows, err := b2SQL.Query("SELECT gen, msg FROM log ORDER BY gen")
	require.NoError(t, err)
	expected := []struct {
		gen int
		msg string
	}{{1, "root"}, {2, "gen2"}, {3, "gen3"}}
	i := 0
	for rows.Next() {
		var gen int
		var msg string
		require.NoError(t, rows.Scan(&gen, &msg))
		require.Equal(t, expected[i].gen, gen)
		require.Equal(t, expected[i].msg, msg)
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 3, i)

	// Root should still have only 1 row.
	rootSQL := registerAndOpenSQL(t, db, "loophole-chain-root-check")
	var rootCount int
	err = rootSQL.QueryRow("SELECT count(*) FROM log").Scan(&rootCount)
	require.NoError(t, err)
	require.Equal(t, 1, rootCount)

	// Gen2 should have 2 rows.
	b1SQL2 := registerAndOpenSQL(t, branch1, "loophole-chain-gen2-check")
	var gen2Count int
	err = b1SQL2.QueryRow("SELECT count(*) FROM log").Scan(&gen2Count)
	require.NoError(t, err)
	require.Equal(t, 2, gen2Count)

	require.NoError(t, b2SQL.Close())
	require.NoError(t, b1SQL2.Close())
	require.NoError(t, rootSQL.Close())
	require.NoError(t, branch2.Close(t.Context()))
	require.NoError(t, branch1.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestConformanceSnapshotThenBranch(t *testing.T) {
	mgr := conformanceManager(t)

	db, err := sqlitevfs.Create(t.Context(), mgr, "snap-branch")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-snap-branch")

	_, err = sqlDB.Exec("CREATE TABLE versions (id INTEGER PRIMARY KEY, ver TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO versions VALUES (1, 'v1.0')")
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	// Snapshot at v1.0.
	require.NoError(t, db.Snapshot(t.Context(), "snap-at-v1"))

	// Continue writing in original.
	sqlDB2 := registerAndOpenSQL(t, db, "loophole-snap-branch-2")
	_, err = sqlDB2.Exec("INSERT INTO versions VALUES (2, 'v2.0')")
	require.NoError(t, err)
	require.NoError(t, sqlDB2.Close())

	// Snapshot at v2.0.
	require.NoError(t, db.Snapshot(t.Context(), "snap-at-v2"))

	// Branch from original (which is now at v2.0).
	branch, err := db.Branch(t.Context(), "snap-branch-fork")
	require.NoError(t, err)

	branchSQL := registerAndOpenSQL(t, branch, "loophole-snap-branch-fork")

	// Branch should see both v1.0 and v2.0.
	var branchCount int
	err = branchSQL.QueryRow("SELECT count(*) FROM versions").Scan(&branchCount)
	require.NoError(t, err)
	require.Equal(t, 2, branchCount)

	// Add v3.0 in branch only.
	_, err = branchSQL.Exec("INSERT INTO versions VALUES (3, 'v3.0')")
	require.NoError(t, err)

	var branchCountAfter int
	err = branchSQL.QueryRow("SELECT count(*) FROM versions").Scan(&branchCountAfter)
	require.NoError(t, err)
	require.Equal(t, 3, branchCountAfter)

	// Original should still have 2.
	sqlDB3 := registerAndOpenSQL(t, db, "loophole-snap-branch-orig-check")
	var origCount int
	err = sqlDB3.QueryRow("SELECT count(*) FROM versions").Scan(&origCount)
	require.NoError(t, err)
	require.Equal(t, 2, origCount)

	require.NoError(t, branchSQL.Close())
	require.NoError(t, sqlDB3.Close())
	require.NoError(t, branch.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestConformanceBranchSchemaChange(t *testing.T) {
	mgr := conformanceManager(t)

	db, err := sqlitevfs.Create(t.Context(), mgr, "branch-schema")
	require.NoError(t, err)

	sqlDB := registerAndOpenSQL(t, db, "loophole-branch-schema")
	_, err = sqlDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO t1 VALUES (1, 'hello')")
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	// Branch.
	branch, err := db.Branch(t.Context(), "branch-schema-fork")
	require.NoError(t, err)

	branchSQL := registerAndOpenSQL(t, branch, "loophole-branch-schema-fork")

	// Add a column and a new table in the branch.
	_, err = branchSQL.Exec("ALTER TABLE t1 ADD COLUMN extra INTEGER DEFAULT 0")
	require.NoError(t, err)
	_, err = branchSQL.Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref INTEGER)")
	require.NoError(t, err)
	_, err = branchSQL.Exec("INSERT INTO t2 VALUES (1, 1)")
	require.NoError(t, err)

	// Branch should see the new column and table.
	var extra int
	err = branchSQL.QueryRow("SELECT extra FROM t1 WHERE id = 1").Scan(&extra)
	require.NoError(t, err)
	require.Equal(t, 0, extra) // default value

	var t2Count int
	err = branchSQL.QueryRow("SELECT count(*) FROM t2").Scan(&t2Count)
	require.NoError(t, err)
	require.Equal(t, 1, t2Count)

	// Original should NOT have the column or table.
	origSQL := registerAndOpenSQL(t, db, "loophole-branch-schema-orig")

	// t2 shouldn't exist.
	_, err = origSQL.Exec("SELECT * FROM t2")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such table")

	// t1 shouldn't have 'extra' column.
	_, err = origSQL.Exec("SELECT extra FROM t1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such column")

	// But t1 original data is intact.
	var val string
	err = origSQL.QueryRow("SELECT val FROM t1 WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "hello", val)

	require.NoError(t, branchSQL.Close())
	require.NoError(t, origSQL.Close())
	require.NoError(t, branch.Close(t.Context()))
	require.NoError(t, db.Close(t.Context()))
}

func TestConformanceLargeData(t *testing.T) {
	mgr := conformanceManager(t)
	sqlDB, _ := openConformanceDB(t, mgr, "large")

	_, err := sqlDB.Exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)")
	require.NoError(t, err)

	// Insert a 1MB blob.
	blob := make([]byte, 1024*1024)
	for i := range blob {
		blob[i] = byte(i % 251) // prime to avoid patterns
	}
	_, err = sqlDB.Exec("INSERT INTO blobs (data) VALUES (?)", blob)
	require.NoError(t, err)

	// Read it back.
	var got []byte
	err = sqlDB.QueryRow("SELECT data FROM blobs WHERE id = 1").Scan(&got)
	require.NoError(t, err)
	require.Equal(t, blob, got)
}
