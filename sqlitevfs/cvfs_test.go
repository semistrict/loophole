package sqlitevfs

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// openCVFSDB creates a formatted volume, registers a CVFS, and opens a *sql.DB through it.
func openCVFSDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	vol := testVolume(t, 512*1024*1024)
	err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	cvfs, err := NewCVFS(t.Context(), vol, name, SyncModeAsync)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = cvfs.FlushHeader()
	})

	dsn := fmt.Sprintf("file:main.db?vfs=%s&cache=private&_busy_timeout=5000&_txlock=immediate", name)
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Enable WAL mode — this is what JuiceFS uses and what our VFS needs to handle.
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)

	return db
}

func TestCVFS_BasicCRUD(t *testing.T) {
	db := openCVFSDB(t, "cvfs-crud")

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t (val) VALUES ('hello')")
	require.NoError(t, err)

	var val string
	err = db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "hello", val)
}

func TestCVFS_WALMode(t *testing.T) {
	db := openCVFSDB(t, "cvfs-wal")

	var mode string
	err := db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	require.NoError(t, err)
	require.Equal(t, "wal", mode)

	// Write and read back through WAL.
	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO t (val) VALUES (?)", i)
		require.NoError(t, err)
	}

	var count int
	err = db.QueryRow("SELECT count(*) FROM t").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 100, count)
}

// TestCVFS_ConcurrentReaders exercises multiple goroutines reading simultaneously.
func TestCVFS_ConcurrentReaders(t *testing.T) {
	db := openCVFSDB(t, "cvfs-conc-read")

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	// Insert seed data.
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		_, err = tx.Exec("INSERT INTO t (val) VALUES (?)", i)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// Concurrent readers.
	const readers = 10
	var wg sync.WaitGroup

	for r := 0; r < readers; r++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				var count int
				if err := db.QueryRow("SELECT count(*) FROM t").Scan(&count); err != nil {
					t.Errorf("reader query: %v", err)
					return
				}
				if count != 1000 {
					t.Errorf("expected 1000 rows, got %d", count)
					return
				}
			}
		})
	}

	wg.Wait()
}

// TestCVFS_ConcurrentWriters exercises multiple goroutines writing simultaneously.
func TestCVFS_ConcurrentWriters(t *testing.T) {
	db := openCVFSDB(t, "cvfs-conc-write")

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, writer INTEGER)")
	require.NoError(t, err)

	const writers = 8
	const rowsPerWriter = 100
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Go(func() {
			for i := 0; i < rowsPerWriter; i++ {
				_, err := db.Exec("INSERT INTO t (val, writer) VALUES (?, ?)", i, w)
				if err != nil {
					t.Errorf("writer %d insert %d: %v", w, i, err)
					return
				}
			}
		})
	}

	wg.Wait()

	var count int
	err = db.QueryRow("SELECT count(*) FROM t").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, writers*rowsPerWriter, count)
}

// TestCVFS_ConcurrentReadWrite exercises readers and writers running simultaneously.
func TestCVFS_ConcurrentReadWrite(t *testing.T) {
	db := openCVFSDB(t, "cvfs-conc-rw")

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	require.NoError(t, err)

	const totalInserts = 500
	var inserted atomic.Int64
	var wg sync.WaitGroup

	// Writer goroutine.
	wg.Go(func() {
		for i := 0; i < totalInserts; i++ {
			_, err := db.Exec("INSERT INTO t (val) VALUES (?)", i)
			if err != nil {
				t.Errorf("writer insert %d: %v", i, err)
				return
			}
			inserted.Add(1)
		}
	})

	// Reader goroutines — read count periodically while writer is active.
	const readers = 5
	for r := 0; r < readers; r++ {
		wg.Go(func() {
			for inserted.Load() < totalInserts {
				var count int
				if err := db.QueryRow("SELECT count(*) FROM t").Scan(&count); err != nil {
					t.Errorf("reader %d: %v", r, err)
					return
				}
				if count < 0 || count > totalInserts {
					t.Errorf("reader %d: unexpected count %d", r, count)
					return
				}
				runtime.Gosched()
			}
		})
	}

	wg.Wait()

	var finalCount int
	err = db.QueryRow("SELECT count(*) FROM t").Scan(&finalCount)
	require.NoError(t, err)
	require.Equal(t, totalInserts, finalCount)
}

// TestCVFS_ConcurrentTransactions exercises concurrent explicit transactions.
func TestCVFS_ConcurrentTransactions(t *testing.T) {
	db := openCVFSDB(t, "cvfs-conc-txn")

	_, err := db.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	require.NoError(t, err)

	// Create accounts.
	const numAccounts = 10
	const initialBalance = 1000
	for i := 0; i < numAccounts; i++ {
		_, err = db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", i, initialBalance)
		require.NoError(t, err)
	}

	// Concurrent transfers between random accounts.
	const workers = 6
	const transfersPerWorker = 50
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := 0; i < transfersPerWorker; i++ {
				from := (w + i) % numAccounts
				to := (w + i + 1) % numAccounts
				amount := 1

				tx, err := db.Begin()
				if err != nil {
					t.Errorf("worker %d txn %d begin: %v", w, i, err)
					return
				}

				var fromBal int
				err = tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", from).Scan(&fromBal)
				if err != nil {
					tx.Rollback()
					t.Errorf("worker %d txn %d select: %v", w, i, err)
					return
				}

				if fromBal < amount {
					tx.Rollback()
					continue
				}

				_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, from)
				if err != nil {
					tx.Rollback()
					t.Errorf("worker %d txn %d debit: %v", w, i, err)
					return
				}

				_, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, to)
				if err != nil {
					tx.Rollback()
					t.Errorf("worker %d txn %d credit: %v", w, i, err)
					return
				}

				if err := tx.Commit(); err != nil {
					t.Errorf("worker %d txn %d commit: %v", w, i, err)
					return
				}
			}
		})
	}

	wg.Wait()

	// Total balance must be conserved.
	var totalBalance int
	err = db.QueryRow("SELECT sum(balance) FROM accounts").Scan(&totalBalance)
	require.NoError(t, err)
	require.Equal(t, numAccounts*initialBalance, totalBalance)
}

// TestCVFS_MultipleConnections opens separate sql.DB connections to the same VFS
// and exercises them concurrently to test cross-connection locking/SHM.
func TestCVFS_MultipleConnections(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfsName := "cvfs-multi-conn"
	cvfs, err := NewCVFS(t.Context(), vol, vfsName, SyncModeAsync)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cvfs.FlushHeader() })

	dsn := fmt.Sprintf("file:main.db?vfs=%s&cache=private&_busy_timeout=5000&_txlock=immediate", vfsName)

	// Open two separate connections.
	db1, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db1.Close() })

	db2, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db2.Close() })

	// Use WAL on both.
	_, err = db1.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)

	_, err = db1.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, conn INTEGER)")
	require.NoError(t, err)

	// Write from both connections concurrently.
	const rowsPer = 200
	var wg sync.WaitGroup

	for connID, db := range []*sql.DB{db1, db2} {
		wg.Go(func() {
			for i := 0; i < rowsPer; i++ {
				_, err := db.Exec("INSERT INTO t (val, conn) VALUES (?, ?)",
					fmt.Sprintf("row-%d-%d", connID, i), connID)
				if err != nil {
					t.Errorf("conn %d insert %d: %v", connID, i, err)
					return
				}
			}
		})
	}

	wg.Wait()

	// Both connections should see all rows.
	for connID, db := range []*sql.DB{db1, db2} {
		var count int
		err = db.QueryRow("SELECT count(*) FROM t").Scan(&count)
		require.NoError(t, err, "conn %d count", connID)
		require.Equal(t, 2*rowsPer, count, "conn %d", connID)
	}

	// Verify each connection wrote exactly rowsPer.
	for connID, db := range []*sql.DB{db1, db2} {
		var count int
		err = db.QueryRow("SELECT count(*) FROM t WHERE conn = ?", connID).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, rowsPer, count, "conn %d own rows", connID)
	}
}

// TestCVFS_ManyConnections opens 16 separate sql.DB instances on the same VFS,
// each writing and reading concurrently, to stress cross-connection locking/SHM.
func TestCVFS_ManyConnections(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfsName := "cvfs-many-conn"
	cvfs, err := NewCVFS(t.Context(), vol, vfsName, SyncModeAsync)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cvfs.FlushHeader() })

	dsn := fmt.Sprintf("file:main.db?vfs=%s&cache=private&_busy_timeout=10000&_txlock=immediate", vfsName)

	const numConns = 16
	const rowsPerConn = 50

	// Open the first connection to set up schema.
	db0, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	_, err = db0.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db0.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, conn INTEGER)")
	require.NoError(t, err)
	require.NoError(t, db0.Close())

	// Open numConns separate sql.DB instances.
	dbs := make([]*sql.DB, numConns)
	for i := range dbs {
		db, err := sql.Open("sqlite3", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1) // Force one underlying connection per sql.DB.
		dbs[i] = db
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			db.Close()
		}
	})

	// Each connection writes rowsPerConn rows concurrently.
	var wg sync.WaitGroup

	for connID, db := range dbs {
		wg.Go(func() {
			for i := 0; i < rowsPerConn; i++ {
				_, err := db.Exec("INSERT INTO t (val, conn) VALUES (?, ?)", i, connID)
				if err != nil {
					t.Errorf("conn %d insert %d: %v", connID, i, err)
					return
				}
			}
		})
	}

	wg.Wait()

	// Every connection should see all rows.
	expectedTotal := numConns * rowsPerConn
	for connID, db := range dbs {
		var count int
		err = db.QueryRow("SELECT count(*) FROM t").Scan(&count)
		require.NoError(t, err, "conn %d", connID)
		require.Equal(t, expectedTotal, count, "conn %d total", connID)
	}

	// Verify per-connection row counts.
	verifyDB := dbs[0]
	for connID := 0; connID < numConns; connID++ {
		var count int
		err = verifyDB.QueryRow("SELECT count(*) FROM t WHERE conn = ?", connID).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, rowsPerConn, count, "conn %d own rows", connID)
	}

	// Now do concurrent reads from all connections simultaneously.
	for connID, db := range dbs {
		wg.Go(func() {
			for i := 0; i < 20; i++ {
				var sum int
				err := db.QueryRow("SELECT sum(val) FROM t WHERE conn = ?", connID).Scan(&sum)
				if err != nil {
					t.Errorf("conn %d read %d: %v", connID, i, err)
					return
				}
				expectedSum := rowsPerConn * (rowsPerConn - 1) / 2
				if sum != expectedSum {
					t.Errorf("conn %d read %d: sum=%d, want %d", connID, i, sum, expectedSum)
					return
				}
			}
		})
	}

	wg.Wait()
}

// TestCVFS_StressInsertQuery hammers the VFS with many small transactions
// from many goroutines and verifies final state.
func TestCVFS_StressInsertQuery(t *testing.T) {
	db := openCVFSDB(t, "cvfs-stress")

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, seq INTEGER)")
	require.NoError(t, err)

	const goroutines = 16
	const insertsPerGoroutine = 100
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Go(func() {
			for i := 0; i < insertsPerGoroutine; i++ {
				_, err := db.Exec("INSERT INTO t (g, seq) VALUES (?, ?)", g, i)
				if err != nil {
					t.Errorf("g=%d seq=%d: %v", g, i, err)
					return
				}
			}
		})
	}

	wg.Wait()

	var total int
	err = db.QueryRow("SELECT count(*) FROM t").Scan(&total)
	require.NoError(t, err)
	require.Equal(t, goroutines*insertsPerGoroutine, total)

	// Verify each goroutine wrote exactly insertsPerGoroutine rows.
	for g := 0; g < goroutines; g++ {
		var count int
		err = db.QueryRow("SELECT count(*) FROM t WHERE g = ?", g).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, insertsPerGoroutine, count, "goroutine %d", g)
	}
}
