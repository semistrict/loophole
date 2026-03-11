//go:build !js

package storage2

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/semistrict/loophole/internal/util"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// sqliteStore implements cacheStore with a persistent mmap arena and a SQLite index.
type sqliteStore struct {
	dir       string
	db        *sql.DB
	arenaFile *os.File
	arenaMmap []byte
	slots     int

	stmtLookup *sql.Stmt
	stmtInsert *sql.Stmt
	stmtDelete *sql.Stmt
	stmtBump   *sql.Stmt
}

func newSQLiteStore(dir string) (*sqliteStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", filepath.Join(dir, "index.db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			util.SafeClose(db, "close db after pragma failure")
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS pages (
		layer_id TEXT    NOT NULL,
		page_idx INTEGER NOT NULL,
		slot     INTEGER NOT NULL UNIQUE,
		credits  INTEGER NOT NULL DEFAULT 1,
		PRIMARY KEY (layer_id, page_idx)
	) WITHOUT ROWID`); err != nil {
		util.SafeClose(db, "close db after table creation failure")
		return nil, err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_pages_credits ON pages (credits)`); err != nil {
		util.SafeClose(db, "close db after index creation failure")
		return nil, err
	}

	s := &sqliteStore{dir: dir, db: db}

	s.stmtLookup, err = db.Prepare(`SELECT slot FROM pages WHERE layer_id=? AND page_idx=?`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}
	s.stmtInsert, err = db.Prepare(`INSERT INTO pages (layer_id, page_idx, slot, credits) VALUES (?,?,?,1)`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}
	s.stmtDelete, err = db.Prepare(`DELETE FROM pages WHERE layer_id=? AND page_idx=? RETURNING slot`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}
	s.stmtBump, err = db.Prepare(`UPDATE pages SET credits=MIN(credits+CAST(? AS INTEGER), 2147483647) WHERE layer_id=? AND page_idx=?`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}

	return s, nil
}

func newDefaultStore(dir string) (cacheStore, error) {
	return newSQLiteStore(dir)
}

// --- Arena ---

func (s *sqliteStore) AllocArena(maxSlots int) error {
	path := filepath.Join(s.dir, "arena")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	size := int64(maxSlots) * PageSize
	fi, err := f.Stat()
	if err != nil {
		util.SafeClose(f, "close arena file after stat failure")
		return err
	}
	currentSize := fi.Size()
	if currentSize < size {
		if err := f.Truncate(size); err != nil {
			util.SafeClose(f, "close arena file after truncate failure")
			return err
		}
		currentSize = size
	}

	actualSlots := int(currentSize / PageSize)
	mapped, err := unix.Mmap(int(f.Fd()), 0, int(currentSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		util.SafeClose(f, "close arena file after mmap failure")
		return err
	}

	s.arenaFile = f
	s.arenaMmap = mapped
	s.slots = actualSlots
	return nil
}

func (s *sqliteStore) ArenaSlots() int {
	return s.slots
}

func (s *sqliteStore) ReadSlot(slot int) ([]byte, error) {
	if s.arenaMmap == nil {
		return nil, fmt.Errorf("arena not allocated")
	}
	if slot < 0 || slot >= s.slots {
		return nil, fmt.Errorf("slot %d out of range [0, %d)", slot, s.slots)
	}
	off := slot * PageSize
	buf := make([]byte, PageSize)
	copy(buf, s.arenaMmap[off:off+PageSize])
	return buf, nil
}

func (s *sqliteStore) WriteSlot(slot int, data []byte) error {
	if s.arenaMmap == nil {
		return fmt.Errorf("arena not allocated")
	}
	if slot < 0 || slot >= s.slots {
		return fmt.Errorf("slot %d out of range [0, %d)", slot, s.slots)
	}
	off := slot * PageSize
	copy(s.arenaMmap[off:off+PageSize], data)
	return nil
}

// --- Index ---

func (s *sqliteStore) LookupPage(key cacheKey) (int, bool, error) {
	var slot int
	err := s.stmtLookup.QueryRow(key.BlobKey, int64(key.PageIdx)).Scan(&slot)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return slot, true, nil
}

func (s *sqliteStore) InsertPage(key cacheKey, slot int) error {
	_, err := s.stmtInsert.Exec(key.BlobKey, int64(key.PageIdx), slot)
	return err
}

func (s *sqliteStore) DeletePage(key cacheKey) (int, error) {
	var slot int
	err := s.stmtDelete.QueryRow(key.BlobKey, int64(key.PageIdx)).Scan(&slot)
	if err != nil {
		return -1, err
	}
	return slot, nil
}

func (s *sqliteStore) BumpCredits(keys []cacheKey) error {
	counts := make(map[cacheKey]int, len(keys))
	for _, k := range keys {
		counts[k]++
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt := tx.Stmt(s.stmtBump)
	defer util.SafeClose(stmt, "close bump credits stmt")
	for k, n := range counts {
		if _, err := stmt.Exec(int64(n), k.BlobKey, int64(k.PageIdx)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) AgeCredits() error {
	_, err := s.db.Exec(`UPDATE pages SET credits=credits-1 WHERE credits>0`)
	return err
}

func (s *sqliteStore) EvictLow(count int) ([]int, error) {
	rows, err := s.db.Query(`DELETE FROM pages WHERE (layer_id, page_idx) IN (
		SELECT layer_id, page_idx FROM pages ORDER BY credits ASC LIMIT ?
	) RETURNING slot`, count)
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(rows, "close evict rows")
	var slots []int
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			return slots, err
		}
		slots = append(slots, slot)
	}
	return slots, rows.Err()
}

func (s *sqliteStore) CountPages() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM pages`).Scan(&n)
	return n, err
}

func (s *sqliteStore) UsedSlots() ([]int, error) {
	rows, err := s.db.Query(`SELECT slot FROM pages`)
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(rows, "close used slots rows")
	var slots []int
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			return slots, err
		}
		slots = append(slots, slot)
	}
	return slots, rows.Err()
}

// --- Disk ---

func (s *sqliteStore) FreeSpace() int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &stat); err != nil {
		return 0
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}

func (s *sqliteStore) MinReserve() int64 {
	return 2 * 1024 * 1024 * 1024 // 2GB
}

func (s *sqliteStore) Close() error {
	var errs []error
	for _, stmt := range []*sql.Stmt{s.stmtLookup, s.stmtInsert, s.stmtDelete, s.stmtBump} {
		if stmt != nil {
			errs = append(errs, stmt.Close())
		}
	}
	if s.db != nil {
		errs = append(errs, s.db.Close())
	}
	if s.arenaMmap != nil {
		errs = append(errs, unix.Munmap(s.arenaMmap))
		s.arenaMmap = nil
	}
	if s.arenaFile != nil {
		errs = append(errs, s.arenaFile.Close())
	}
	return errors.Join(errs...)
}
