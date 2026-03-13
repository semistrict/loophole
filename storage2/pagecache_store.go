package storage2

import (
	"database/sql"
	"encoding/binary"
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
	lockFile  *os.File

	stmtLookup *sql.Stmt
	stmtInsert *sql.Stmt
	stmtDelete *sql.Stmt
	stmtBump   *sql.Stmt
}

const (
	cacheSlotHeaderSize = 8
	cacheSchemaVersion  = 2
)

func newSQLiteStore(dir string) (*sqliteStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	if err := initializeSQLiteStore(dir); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", sqliteStoreDSN(filepath.Join(dir, "index.db")))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	s := &sqliteStore{dir: dir, db: db}
	lockFile, err := os.OpenFile(filepath.Join(dir, "arena.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		util.SafeClose(s, "close store after lock open failure")
		return nil, err
	}
	s.lockFile = lockFile

	s.stmtLookup, err = db.Prepare(`SELECT slot, generation FROM pages WHERE layer_id=? AND page_idx=?`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}
	s.stmtInsert, err = db.Prepare(`INSERT INTO pages (layer_id, page_idx, slot, generation, credits)
		VALUES (?,?,?,?,1)
		ON CONFLICT(layer_id, page_idx) DO UPDATE SET slot=excluded.slot, generation=excluded.generation`)
	if err != nil {
		util.SafeClose(s, "close store after prepare failure")
		return nil, err
	}
	s.stmtDelete, err = db.Prepare(`DELETE FROM pages WHERE layer_id=? AND page_idx=? RETURNING slot, generation`)
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

func initializeSQLiteStore(dir string) error {
	lockPath := filepath.Join(dir, "index.db.init.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open sqlite init lock: %w", err)
	}
	defer util.SafeClose(lockFile, "close sqlite init lock")

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock sqlite init file: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil {
			panic(fmt.Sprintf("unlock sqlite init file: %v", err))
		}
	}()

	dbPath := filepath.Join(dir, "index.db")
	db, err := sql.Open("sqlite", sqliteStoreDSN(dbPath))
	if err != nil {
		return err
	}
	defer util.SafeClose(db, "close sqlite init db")
	db.SetMaxOpenConns(1)

	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		return fmt.Errorf("PRAGMA journal_mode: %w", err)
	}
	if journalMode != "wal" {
		if err := db.QueryRow(`PRAGMA journal_mode=WAL`).Scan(&journalMode); err != nil {
			return fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
		}
		if journalMode != "wal" {
			return fmt.Errorf("PRAGMA journal_mode=WAL: got %q", journalMode)
		}
	}

	var hasPages int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='pages'`).Scan(&hasPages); err != nil {
		return fmt.Errorf("check pages table: %w", err)
	}
	resetArena := false
	if hasPages > 0 {
		rows, err := db.Query(`PRAGMA table_info(pages)`)
		if err != nil {
			return fmt.Errorf("PRAGMA table_info(pages): %w", err)
		}
		defer util.SafeClose(rows, "close pages table info")
		hasGeneration := false
		for rows.Next() {
			var cid int
			var name string
			var typ string
			var notNull int
			var dflt sql.NullString
			var pk int
			if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
				return fmt.Errorf("scan pages column info: %w", err)
			}
			if name == "generation" {
				hasGeneration = true
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate pages column info: %w", err)
		}
		if !hasGeneration {
			if _, err := db.Exec(`DROP TABLE IF EXISTS pages`); err != nil {
				return fmt.Errorf("drop old pages table: %w", err)
			}
			resetArena = true
		}
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS pages (
		layer_id TEXT    NOT NULL,
		page_idx INTEGER NOT NULL,
		slot     INTEGER NOT NULL UNIQUE,
		generation INTEGER NOT NULL,
		credits  INTEGER NOT NULL DEFAULT 1,
		PRIMARY KEY (layer_id, page_idx)
	) WITHOUT ROWID`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_pages_credits ON pages (credits)`); err != nil {
		return err
	}
	if resetArena {
		if err := os.Remove(filepath.Join(dir, "arena")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old arena: %w", err)
		}
	}
	return nil
}

func sqliteStoreDSN(dbPath string) string {
	return fmt.Sprintf("file:%s?_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)", dbPath)
}

// --- Arena ---

func (s *sqliteStore) AllocArena(maxSlots int) error {
	path := filepath.Join(s.dir, "arena")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	size := int64(maxSlots) * cacheSlotSize()
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

	actualSlots := int(currentSize / cacheSlotSize())
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

func cacheSlotSize() int64 {
	return PageSize + cacheSlotHeaderSize
}

func (s *sqliteStore) slotOffset(slot int) int {
	return slot * int(cacheSlotSize())
}

func (s *sqliteStore) readGeneration(slot int) (uint64, error) {
	if s.arenaMmap == nil {
		return 0, fmt.Errorf("arena not allocated")
	}
	if slot < 0 || slot >= s.slots {
		return 0, fmt.Errorf("slot %d out of range [0, %d)", slot, s.slots)
	}
	off := s.slotOffset(slot)
	return binary.LittleEndian.Uint64(s.arenaMmap[off : off+cacheSlotHeaderSize]), nil
}

func (s *sqliteStore) writeGeneration(slot int, generation uint64) error {
	if s.arenaMmap == nil {
		return fmt.Errorf("arena not allocated")
	}
	if slot < 0 || slot >= s.slots {
		return fmt.Errorf("slot %d out of range [0, %d)", slot, s.slots)
	}
	off := s.slotOffset(slot)
	binary.LittleEndian.PutUint64(s.arenaMmap[off:off+cacheSlotHeaderSize], generation)
	return nil
}

func (s *sqliteStore) ReadSlot(ref cacheSlotRef) ([]byte, error) {
	if s.arenaMmap == nil {
		return nil, fmt.Errorf("arena not allocated")
	}
	if ref.Slot < 0 || ref.Slot >= s.slots {
		return nil, fmt.Errorf("slot %d out of range [0, %d)", ref.Slot, s.slots)
	}
	gen, err := s.readGeneration(ref.Slot)
	if err != nil {
		return nil, err
	}
	if gen != ref.Generation {
		return nil, fmt.Errorf("stale slot generation: slot=%d have=%d want=%d", ref.Slot, gen, ref.Generation)
	}
	off := s.slotOffset(ref.Slot) + cacheSlotHeaderSize
	buf := make([]byte, PageSize)
	copy(buf, s.arenaMmap[off:off+PageSize])
	return buf, nil
}

func (s *sqliteStore) PrepareSlot(slot int, data []byte) (cacheSlotRef, error) {
	if s.arenaMmap == nil {
		return cacheSlotRef{}, fmt.Errorf("arena not allocated")
	}
	if slot < 0 || slot >= s.slots {
		return cacheSlotRef{}, fmt.Errorf("slot %d out of range [0, %d)", slot, s.slots)
	}
	gen, err := s.readGeneration(slot)
	if err != nil {
		return cacheSlotRef{}, err
	}
	gen++
	if gen == 0 {
		gen = 1
	}
	if err := s.writeGeneration(slot, gen); err != nil {
		return cacheSlotRef{}, err
	}
	off := s.slotOffset(slot) + cacheSlotHeaderSize
	copy(s.arenaMmap[off:off+PageSize], data)
	return cacheSlotRef{Slot: slot, Generation: gen}, nil
}

// --- Index ---

func (s *sqliteStore) LookupPage(key cacheKey) (cacheSlotRef, bool, error) {
	var ref cacheSlotRef
	err := s.stmtLookup.QueryRow(key.LayerID, int64(key.PageIdx)).Scan(&ref.Slot, &ref.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return cacheSlotRef{}, false, nil
	}
	if err != nil {
		return cacheSlotRef{}, false, err
	}
	return ref, true, nil
}

func (s *sqliteStore) SetPage(key cacheKey, ref cacheSlotRef) error {
	_, err := s.stmtInsert.Exec(key.LayerID, int64(key.PageIdx), ref.Slot, ref.Generation)
	return err
}

func (s *sqliteStore) DeletePage(key cacheKey) (cacheSlotRef, error) {
	var ref cacheSlotRef
	err := s.stmtDelete.QueryRow(key.LayerID, int64(key.PageIdx)).Scan(&ref.Slot, &ref.Generation)
	if err != nil {
		return cacheSlotRef{}, err
	}
	return ref, nil
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
		if _, err := stmt.Exec(int64(n), k.LayerID, int64(k.PageIdx)); err != nil {
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

func (s *sqliteStore) LockMutation() error {
	if s.lockFile == nil {
		return fmt.Errorf("cache mutation lock file not open")
	}
	return syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX)
}

func (s *sqliteStore) UnlockMutation() error {
	if s.lockFile == nil {
		return fmt.Errorf("cache mutation lock file not open")
	}
	return syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
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
	if s.lockFile != nil {
		errs = append(errs, s.lockFile.Close())
	}
	return errors.Join(errs...)
}
