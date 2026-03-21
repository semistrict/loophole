package cachedserver

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/internal/pagegeom"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
	"golang.org/x/sys/unix"

	_ "modernc.org/sqlite"
)

const (
	pageSize       = pagegeom.PageSize
	maxBudget      = 2 << 30 // 2 GiB
	fallbackBudget = 256 << 20
	budgetInterval = 30 * time.Second
	minReserve     = 2 << 30 // 2 GiB
)

type cacheKey struct {
	layerID string
	pageIdx uint64
}

// Package-level state — one daemon per process.
var (
	mu         sync.Mutex
	dir        string
	arena      []byte
	arenaFile  *os.File
	arenaSlots int
	index      map[cacheKey]int // key → slot
	slotOwner  map[int]cacheKey // slot → key (reverse)
	freeSlots  []int
	atime      map[cacheKey]int64 // last access unix nanos
	conns      map[*conn]struct{}
	db         *sql.DB

	budget    int64
	usedBytes int64

	lockFile *os.File
	stopCh   chan struct{}
	wg       sync.WaitGroup
)

// InitInMemory sets up the daemon with a pure in-memory arena and index.
// No files, no SQLite, no mmap. For use in tests and synctest bubbles.
func InitInMemory(slots int) {
	arena = make([]byte, slots*pageSize)
	arenaSlots = slots
	index = make(map[cacheKey]int)
	slotOwner = make(map[int]cacheKey)
	atime = make(map[cacheKey]int64)
	conns = make(map[*conn]struct{})
	stopCh = make(chan struct{})
	budget = int64(slots) * pageSize
	freeSlots = make([]int, slots)
	for i := range slots {
		freeSlots[i] = slots - 1 - i
	}
}

func initDaemon(d string) error {
	dir = d
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	// Acquire an exclusive non-blocking flock to guarantee only one
	// daemon owns this cache directory.
	lockPath := filepath.Join(dir, "cached.lock")
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		util.SafeClose(lf, "close lock file after flock failure")
		return fmt.Errorf("cache directory %s is locked by another daemon", dir)
	}
	lockFile = lf

	// If the dirty marker exists, the previous daemon crashed mid-write.
	// Wipe the potentially inconsistent index and arena; the cache will
	// repopulate from the backing store (performance hit, not data loss).
	dirtyPath := filepath.Join(dir, "dirty")
	if _, err := os.Stat(dirtyPath); err == nil {
		slog.Warn("dirty marker found, wiping stale cache state")
		for _, name := range []string{"index.db", "index.db-wal", "index.db-shm", "arena"} {
			p := filepath.Join(dir, name)
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("wipe stale %s: %w", name, err)
			}
		}
	}
	// Write dirty marker before doing any work.
	if err := os.WriteFile(dirtyPath, nil, 0o600); err != nil {
		return fmt.Errorf("write dirty marker: %w", err)
	}

	index = make(map[cacheKey]int)
	slotOwner = make(map[int]cacheKey)
	atime = make(map[cacheKey]int64)
	conns = make(map[*conn]struct{})
	stopCh = make(chan struct{})

	dbPath := filepath.Join(dir, "index.db")
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath)
	d2, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	d2.SetMaxOpenConns(1)
	db = d2

	if err := initDB(); err != nil {
		util.SafeClose(db, "close db after init failure")
		return err
	}

	budget = computeBudget()
	slots := int(budget / pageSize / 2)
	if slots < 1 {
		slots = 1
	}
	if err := allocArena(slots); err != nil {
		util.SafeClose(db, "close db after arena alloc failure")
		return err
	}

	if err := loadIndex(); err != nil {
		closeArena()
		util.SafeClose(db, "close db after index load failure")
		return err
	}

	wg.Add(1)
	go budgetLoop()
	return nil
}

func initDB() error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS pages (
		layer_id TEXT    NOT NULL,
		page_idx INTEGER NOT NULL,
		slot     INTEGER NOT NULL UNIQUE,
		PRIMARY KEY (layer_id, page_idx)
	) WITHOUT ROWID`)
	return err
}

func loadIndex() error {
	rows, err := db.Query(`SELECT layer_id, page_idx, slot FROM pages`)
	if err != nil {
		return err
	}
	defer util.SafeClose(rows, "close load index rows")

	used := make(map[int]struct{})
	now := time.Now().UnixNano()
	for rows.Next() {
		var layerID string
		var pageIdx int64
		var slot int
		if err := rows.Scan(&layerID, &pageIdx, &slot); err != nil {
			return err
		}
		if slot >= arenaSlots {
			continue
		}
		key := cacheKey{layerID, uint64(pageIdx)}
		index[key] = slot
		slotOwner[slot] = key
		atime[key] = now
		used[slot] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	freeSlots = freeSlots[:0]
	for i := arenaSlots - 1; i >= 0; i-- {
		if _, ok := used[i]; !ok {
			freeSlots = append(freeSlots, i)
		}
	}
	usedBytes = int64(len(index)) * pageSize
	return nil
}

func allocArena(maxSlots int) error {
	path := filepath.Join(dir, "arena")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	size := int64(maxSlots) * pageSize
	fi, err := f.Stat()
	if err != nil {
		util.SafeClose(f, "close arena after stat failure")
		return err
	}
	if fi.Size() < size {
		if err := f.Truncate(size); err != nil {
			util.SafeClose(f, "close arena after truncate failure")
			return err
		}
	} else {
		size = fi.Size()
	}

	actualSlots := int(size / pageSize)
	mapped, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		util.SafeClose(f, "close arena after mmap failure")
		return err
	}

	arenaFile = f
	arena = mapped
	arenaSlots = actualSlots
	return nil
}

func closeArena() {
	if arena != nil {
		_ = unix.Munmap(arena)
		arena = nil
	}
	if arenaFile != nil {
		util.SafeClose(arenaFile, "close arena file")
		arenaFile = nil
	}
}

// lookupLocked returns the slot for a cached page. Caller must hold mu.
func lookupLocked(key cacheKey) (int, bool) {
	slot, ok := index[key]
	if ok {
		atime[key] = time.Now().UnixNano()
	}
	return slot, ok
}

// populateLocked inserts or updates a page in the cache. Caller must hold mu.
func populateLocked(key cacheKey, data []byte) (int, error) {
	if len(data) != pageSize {
		return 0, fmt.Errorf("data must be %d bytes, got %d", pageSize, len(data))
	}

	if slot, ok := index[key]; ok {
		off := slot * pageSize
		copy(arena[off:off+pageSize], data)
		atime[key] = time.Now().UnixNano()
		return slot, nil
	}

	if usedBytes+pageSize > budget {
		return 0, fmt.Errorf("over budget")
	}

	slot, ok := allocSlotLocked()
	if !ok {
		return 0, fmt.Errorf("no free slots")
	}

	off := slot * pageSize
	copy(arena[off:off+pageSize], data)

	index[key] = slot
	slotOwner[slot] = key
	atime[key] = time.Now().UnixNano()
	usedBytes += pageSize

	if db != nil {
		_, err := db.Exec(
			`INSERT INTO pages (layer_id, page_idx, slot) VALUES (?, ?, ?)
			 ON CONFLICT(layer_id, page_idx) DO UPDATE SET slot=excluded.slot`,
			key.layerID, int64(key.pageIdx), slot)
		if err != nil {
			delete(index, key)
			delete(slotOwner, slot)
			delete(atime, key)
			freeSlots = append(freeSlots, slot)
			usedBytes -= pageSize
			return 0, err
		}
	}

	return slot, nil
}

// deletePageLocked removes a page from the cache. Caller must hold mu.
func deletePageLocked(key cacheKey) {
	slot, ok := index[key]
	if !ok {
		return
	}
	delete(index, key)
	delete(slotOwner, slot)
	delete(atime, key)
	freeSlots = append(freeSlots, slot)
	usedBytes -= pageSize

	if db != nil {
		_, _ = db.Exec(`DELETE FROM pages WHERE layer_id=? AND page_idx=?`,
			key.layerID, int64(key.pageIdx))
	}
}

func allocSlotLocked() (int, bool) {
	n := len(freeSlots)
	if n > 0 {
		slot := freeSlots[n-1]
		freeSlots = freeSlots[:n-1]
		return slot, true
	}
	return 0, false
}

func evictLRULocked(count int) []int {
	if len(index) == 0 {
		return nil
	}

	type entry struct {
		key   cacheKey
		atime int64
		slot  int
	}
	items := make([]entry, 0, len(index))
	for k, slot := range index {
		items = append(items, entry{key: k, atime: atime[k], slot: slot})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].atime < items[j].atime })
	if count > len(items) {
		count = len(items)
	}

	freed := make([]int, 0, count)
	for i := range count {
		k := items[i].key
		slot := items[i].slot
		delete(index, k)
		delete(slotOwner, slot)
		delete(atime, k)
		freed = append(freed, slot)
		usedBytes -= pageSize
	}
	metrics.CacheDaemonEvictions.Add(float64(count))

	if db != nil {
		tx, err := db.Begin()
		if err == nil {
			for i := range count {
				_, _ = tx.Exec(`DELETE FROM pages WHERE layer_id=? AND page_idx=?`,
					items[i].key.layerID, int64(items[i].key.pageIdx))
			}
			_ = tx.Commit()
		}
	}

	return freed
}

func evictUntilWithinBudgetLocked() {
	for usedBytes > budget {
		count := int((usedBytes - budget) / pageSize)
		if count < 1 {
			count = 1
		}
		freed := evictLRULocked(count)
		if len(freed) == 0 {
			return
		}
		freeSlots = append(freeSlots, freed...)
	}
}

// --- Budget ---

func computeBudget() int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return fallbackBudget
	}
	freeSpace := int64(stat.Bavail) * int64(stat.Bsize)
	if freeSpace <= 0 {
		return fallbackBudget
	}
	available := freeSpace + usedBytes
	b := available * 3 / 4
	if reserveBudget := available - minReserve; reserveBudget < b {
		b = reserveBudget
	}
	if b < 0 {
		return 0
	}
	if b > maxBudget {
		b = maxBudget
	}
	return b
}

func budgetLoop() {
	defer wg.Done()
	ticker := time.NewTicker(budgetInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mu.Lock()
			budget = computeBudget()
			overBudget := usedBytes > budget
			pages := len(index)
			used := usedBytes
			b := budget
			mu.Unlock()

			if overBudget {
				drainEvictResume()
			}

			metrics.CacheDaemonPages.Set(float64(pages))
			metrics.CacheDaemonUsedBytes.Set(float64(used))
			metrics.CacheDaemonBudgetBytes.Set(float64(b))

			slog.Debug("pagecache: budget check",
				"budget_mb", b>>20,
				"used_mb", used>>20,
				"pages", pages,
			)
		case <-stopCh:
			return
		}
	}
}

// --- Drain protocol ---

func drainEvictResume() {
	n := startDrain()
	if n > 0 {
		t := metrics.NewTimer(metrics.CacheDaemonDrainQuiesceSeconds)
		drainWg.Wait()
		t.ObserveDuration()
	}

	// Evict while all clients are paused.
	mu.Lock()
	evictUntilWithinBudgetLocked()
	mu.Unlock()

	metrics.CacheDaemonDrains.Inc()

	if n > 0 {
		finishDrain()
	}
}

// --- Lifecycle ---

// Arena returns the shared arena byte slice. Only valid after StartServer.
func Arena() []byte { return arena }

func Shutdown() error {
	close(stopCh)

	mu.Lock()
	for c := range conns {
		util.SafeClose(c.netConn, "close client conn on shutdown")
	}
	mu.Unlock()

	wg.Wait()

	if arenaFile != nil {
		closeArena()
	} else {
		arena = nil
	}
	var errs []error
	if db != nil {
		errs = append(errs, db.Close())
	}
	if lockFile != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		errs = append(errs, lockFile.Close())
	}

	if dir != "" {
		_ = os.Remove(cached.SocketPath(dir))
		util.SafeRun(func() error { return os.Remove(filepath.Join(dir, "dirty")) }, "remove dirty marker")
	}

	return errors.Join(errs...)
}
