package bbolt

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
	fl "go.etcd.io/bbolt/internal/freelist"
)

// FreelistType is the type of the freelist backend
type FreelistType string

// TODO(ahrtr): eventually we should (step by step)
//  1. default to `FreelistMapType`;
//  2. remove the `FreelistArrayType`, do not export `FreelistMapType`
//     and remove field `FreelistType' from both `DB` and `Options`;
const (
	// FreelistArrayType indicates backend freelist type is array
	FreelistArrayType = FreelistType("array")
	// FreelistMapType indicates backend freelist type is hashmap
	FreelistMapType = FreelistType("hashmap")
)

// Data abstracts the underlying storage backend for a database.
// Implementations may be backed by a file, block device, or memory.
type Data interface {
	// ReadAt returns exactly n bytes starting at the given offset.
	// It returns an error if off is negative or if off+n exceeds the data size.
	// The returned slice must not be modified by the caller.
	ReadAt(off int64, n int) (buf []byte, release func(), err error)

	// WriteAt writes b at the given offset.
	WriteAt(b []byte, off int64) (n int, err error)

	// Size returns the current size of the data in bytes.
	Size() (int64, error)

	// Grow grows the data to at least the given size in bytes.
	Grow(sz int64) error

	// Sync flushes any buffered writes to stable storage.
	Sync() error
}

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	StrictMode bool

	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	NoSync bool

	// When true, skips syncing freelist to disk. This improves the database
	// write performance under normal operation, but requires a full database
	// re-sync during recovery.
	NoFreelistSync bool

	// FreelistType sets the backend freelist type. There are two options. Array which is simple but endures
	// dramatic performance degradation if database is large and fragmentation in freelist is common.
	// The alternative one is using hashmap, it is faster in almost all circumstances
	// but it doesn't guarantee that it offers the smallest page id available. In normal case it is safe.
	// The default type is array
	FreelistType FreelistType

	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool

	// When `true`, bbolt will always load the free pages when opening the DB.
	// When opening db in write mode, this flag will always automatically
	// set to `true`.
	PreLoadFreelist bool

	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchDelay time.Duration

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int

	// MaxSize is the maximum size (in bytes) allowed for the data file.
	// If a caller's attempt to add data results in the need to grow
	// the data file, an error will be returned and the data file will not grow.
	// <=0 means no limit.
	MaxSize int

	logger Logger

	data     Data
	pageSize int
	opened   bool
	rwtx     *Tx
	stats    *Stats

	freelist     fl.Interface
	freelistLoad sync.Once

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool
}

// Path returns the file path of the database if backed by a file.
// Returns an empty string for non-file-backed databases.
func (db *DB) Path() string {
	type pather interface {
		Path() string
	}
	if p, ok := db.data.(pather); ok {
		return p.Path()
	}
	return ""
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.Path())
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.Path())
}

// Open opens a database using the given Data backend.
// Passing in nil options will cause Bolt to open the database with the default options.
func Open(data Data, options *Options) (db *DB, err error) {
	db = &DB{
		opened: true,
		data:   data,
	}

	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.NoSync = options.NoSync
	db.NoGrowSync = options.NoGrowSync
	db.NoFreelistSync = options.NoFreelistSync
	db.PreLoadFreelist = options.PreLoadFreelist
	db.FreelistType = options.FreelistType
	db.MaxSize = options.MaxSize

	// Set default values for later DB operations.
	db.MaxBatchSize = common.DefaultMaxBatchSize
	db.MaxBatchDelay = common.DefaultMaxBatchDelay
	db.AllocSize = common.DefaultAllocSize

	if !options.NoStatistics {
		db.stats = new(Stats)
	}

	if options.Logger == nil {
		db.logger = getDiscardLogger()
	} else {
		db.logger = options.Logger
	}

	lg := db.Logger()

	if options.ReadOnly {
		db.readOnly = true
	} else {
		// always load free pages in write mode
		db.PreLoadFreelist = true
	}

	if db.pageSize = options.PageSize; db.pageSize == 0 {
		// Set the default page size to the OS page size.
		db.pageSize = common.DefaultPageSize
	}

	// Initialize the database if it doesn't exist.
	sz, err := data.Size()
	if err != nil {
		_ = db.close()
		return nil, fmt.Errorf("failed to get data size: %w", err)
	}
	if sz == 0 {
		// Initialize new data with meta pages.
		if err = db.init(); err != nil {
			_ = db.close()
			lg.Errorf("failed to initialize db: %v", err)
			return nil, err
		}
	} else {
		// try to get the page size from the metadata pages
		if db.pageSize, err = db.getPageSize(); err != nil {
			_ = db.close()
			lg.Errorf("failed to get page size: %v", err)
			return nil, err
		}
	}

	// Initialize page pool.
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	if err = db.loadMeta(); err != nil {
		_ = db.close()
		lg.Errorf("failed to load meta: %v", err)
		return nil, err
	}

	if db.PreLoadFreelist {
		db.loadFreelist()
	}

	if db.readOnly {
		return db, nil
	}

	// Flush freelist when transitioning from no sync to sync so
	// NoFreelistSync unaware boltdb can open the db later.
	if !db.NoFreelistSync && !db.hasSyncedFreelist() {
		tx, txErr := db.Begin(true)
		if tx != nil {
			txErr = tx.Commit()
		}
		if txErr != nil {
			lg.Errorf("starting readwrite transaction failed: %v", txErr)
			_ = db.close()
			return nil, txErr
		}
	}

	return db, nil
}

// getPageSize reads the pageSize from the meta pages. It tries
// to read the first meta page firstly. If the first page is invalid,
// then it tries to read the second page using the default page size.
func (db *DB) getPageSize() (int, error) {
	var (
		meta0CanRead, meta1CanRead bool
	)

	// Read the first meta page to determine the page size.
	if pgSize, canRead, err := db.getPageSizeFromFirstMeta(); err != nil {
		// We cannot read the page size from page 0, but can read page 0.
		meta0CanRead = canRead
	} else {
		return pgSize, nil
	}

	// Read the second meta page to determine the page size.
	if pgSize, canRead, err := db.getPageSizeFromSecondMeta(); err != nil {
		// We cannot read the page size from page 1, but can read page 1.
		meta1CanRead = canRead
	} else {
		return pgSize, nil
	}

	// If we can't read the page size from both pages, but can read
	// either page, then we assume it's the same as the OS or the one
	// given, since that's how the page size was chosen in the first place.
	//
	// If both pages are invalid, and (this OS uses a different page size
	// from what the database was created with or the given page size is
	// different from what the database was created with), then we are out
	// of luck and cannot access the database.
	if meta0CanRead || meta1CanRead {
		return db.pageSize, nil
	}

	return 0, berrors.ErrInvalid
}

// getPageSizeFromFirstMeta reads the pageSize from the first meta page
func (db *DB) getPageSizeFromFirstMeta() (int, bool, error) {
	b, release, err := db.data.ReadAt(0, 0x1000)
	if err != nil {
		return 0, false, err
	}
	defer release()
	if m := db.pageInBuffer(b[:0x1000], 0).Meta(); m.Validate() == nil {
		return int(m.PageSize()), true, nil
	}
	return 0, true, berrors.ErrInvalid
}

// getPageSizeFromSecondMeta reads the pageSize from the second meta page
func (db *DB) getPageSizeFromSecondMeta() (int, bool, error) {
	dataSz, err := db.data.Size()
	if err != nil {
		return 0, false, err
	}

	// We need to read the second meta page, so we should skip the first page;
	// but we don't know the exact page size yet, it's chicken & egg problem.
	// The solution is to try all the possible page sizes, which starts from 1KB
	// and until 16MB (1024<<14) or the end of the data.
	for i := 0; i <= 14; i++ {
		pos := int64(1024 << uint(i))
		if pos >= dataSz-1024 {
			break
		}
		readSz := int64(0x1000)
		if pos+readSz > dataSz {
			readSz = dataSz - pos
		}
		b, release, err := db.data.ReadAt(pos, int(readSz))
		if err != nil {
			continue
		}
		m := db.pageInBuffer(b[:readSz], 0).Meta()
		valid := m.Validate() == nil
		var pgSize int
		if valid {
			pgSize = int(m.PageSize())
		}
		release()
		if valid {
			return pgSize, true, nil
		}
	}

	return 0, false, berrors.ErrInvalid
}

// loadFreelist reads the freelist if it is synced, or reconstructs it
// by scanning the DB if it is not synced. It assumes there are no
// concurrent accesses being made to the freelist.
func (db *DB) loadFreelist() {
	db.freelistLoad.Do(func() {
		db.freelist = newFreelist(db.FreelistType)
		if !db.hasSyncedFreelist() {
			// Reconstruct free list by scanning the DB.
			db.freelist.Init(db.freepages())
		} else {
			// Read free list from freelist page.
			p, release := db.page(db.meta().Freelist())
			db.freelist.Read(p)
			release()
		}
		if db.stats != nil {
			db.stats.FreePageN = db.freelist.FreeCount()
		}
	})
}

func (db *DB) hasSyncedFreelist() bool {
	return db.meta().Freelist() != common.PgidNoFreelist
}

func (db *DB) dataSize() (int, error) {
	sz, err := db.data.Size()
	if err != nil {
		return 0, fmt.Errorf("data size error: %w", err)
	}
	if int(sz) < db.pageSize*2 {
		return 0, fmt.Errorf("data size too small %d", sz)
	}
	return int(sz), nil
}

// loadMeta reads the meta pages from the data backend and validates them.
func (db *DB) loadMeta() error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	lg := db.Logger()

	// Ensure the file is large enough to hold both meta pages.
	sz, err := db.data.Size()
	if err != nil {
		return err
	}
	if sz < int64(db.pageSize*2) {
		return berrors.ErrFileSizeTooSmall
	}

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	p0, release0 := db.page(0)
	err0 := p0.Meta().Validate()
	release0()
	p1, release1 := db.page(1)
	err1 := p1.Meta().Validate()
	release1()
	if err0 != nil && err1 != nil {
		lg.Errorf("both meta pages are invalid, meta0: %v, meta1: %v", err0, err1)
		return err0
	}

	return nil
}

func (db *DB) invalidate() {
	db.data = nil
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf, common.Pgid(i))
		p.SetId(common.Pgid(i))
		p.SetFlags(common.MetaPageFlag)

		// Initialize the meta page.
		m := p.Meta()
		m.SetMagic(common.Magic)
		m.SetVersion(common.Version)
		m.SetPageSize(uint32(db.pageSize))
		m.SetFreelist(2)
		m.SetRootBucket(common.NewInBucket(3, 0))
		m.SetPgid(4)
		m.SetTxid(common.Txid(i))
		m.SetChecksum(m.Sum64())
	}

	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf, common.Pgid(2))
	p.SetId(2)
	p.SetFlags(common.FreelistPageFlag)
	p.SetCount(0)

	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf, common.Pgid(3))
	p.SetId(3)
	p.SetFlags(common.LeafPageFlag)
	p.SetCount(0)

	// Write the buffer to our data backend.
	if _, err := db.data.WriteAt(buf, 0); err != nil {
		db.Logger().Errorf("writeAt failed: %w", err)
		return err
	}
	if err := db.data.Sync(); err != nil {
		db.Logger().Errorf("sync failed: %w", err)
		return err
	}

	return nil
}

// Close releases all database resources.
// It will block waiting for any open transactions to finish
// before closing the database and returning.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false
	db.freelist = nil
	db.invalidate()
	return nil
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.
func (db *DB) Begin(writable bool) (t *Tx, err error) {
	if lg := db.Logger(); lg != discardLogger {
		lg.Debugf("Starting a new transaction [writable: %t]", writable)
		defer func() {
			if err != nil {
				lg.Errorf("Starting a new transaction [writable: %t] failed: %v", writable, err)
			} else {
				lg.Debugf("Starting a new transaction [writable: %t] successfully", writable)
			}
		}()
	}

	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) Logger() Logger {
	if db == nil || db.logger == nil {
		return getDiscardLogger()
	}
	return db.logger
}

func (db *DB) beginTx() (*Tx, error) {
	// Lock the meta pages while we initialize the transaction. We obtain
	// the meta lock before the mmap lock because that's the order that the
	// write transaction will obtain them.
	db.metalock.Lock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	// Exit if the database is not correctly mapped.
	if db.data == nil {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrInvalidMapping
	}

	// Create a transaction associated with the database.
	t := &Tx{}
	t.init(db)

	if db.freelist != nil {
		db.freelist.AddReadonlyTXID(t.meta.Txid())
	}

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Update the transaction stats.
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.TxN++
		db.stats.OpenTxN++
		db.statlock.Unlock()
	}

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, berrors.ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	// Exit if the database is not correctly mapped.
	if db.data == nil {
		db.rwlock.Unlock()
		return nil, berrors.ErrInvalidMapping
	}

	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t
	db.freelist.ReleasePendingPages()
	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	db.metalock.Lock()

	if db.freelist != nil {
		db.freelist.RemoveReadonlyTXID(tx.meta.Txid())
	}

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Merge statistics.
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.OpenTxN--
		db.stats.TxStats.add(&tx.stats)
		db.statlock.Unlock()
	}
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Rollback()
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync flushes any buffered writes to stable storage.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database to sync against the disk.
func (db *DB) Sync() error {
	return db.data.Sync()
}

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	var s Stats
	if db.stats != nil {
		db.statlock.RLock()
		s = *db.stats
		db.statlock.RUnlock()
	}
	return s
}

// Info returns database metadata for debugging.
func (db *DB) Info() *Info {
	return &Info{PageSize: db.pageSize}
}

// page retrieves a page reference from the data backend based on the current page size.
func (db *DB) page(id common.Pgid) (*common.Page, func()) {
	pos := id * common.Pgid(db.pageSize)
	b, release, err := db.data.ReadAt(int64(pos), db.pageSize)
	if err != nil {
		panic(fmt.Sprintf("page read at offset %d failed: %v", pos, err))
	}
	p := (*common.Page)(unsafe.Pointer(&b[0]))
	// If page has overflow, re-read with full size.
	if overflow := p.Overflow(); overflow > 0 {
		release()
		b, release, err = db.data.ReadAt(int64(pos), (int(overflow)+1)*db.pageSize)
		if err != nil {
			panic(fmt.Sprintf("page read at offset %d (overflow %d) failed: %v", pos, overflow, err))
		}
		p = (*common.Page)(unsafe.Pointer(&b[0]))
	}
	return p, release
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id common.Pgid) *common.Page {
	return (*common.Page)(unsafe.Pointer(&b[id*common.Pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
// It always reads fresh from the data backend to avoid stale pointers
// that could occur after the backing storage is grown/reallocated.
func (db *DB) meta() *common.Meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	p0, release0 := db.page(0)
	metaA := *p0.Meta()
	release0()
	p1, release1 := db.page(1)
	metaB := *p1.Meta()
	release1()
	if metaB.Txid() > metaA.Txid() {
		metaA, metaB = metaB, metaA
	}

	// Use higher meta page if valid. Otherwise, fallback to previous, if valid.
	if err := metaA.Validate(); err == nil {
		return &metaA
	} else if err := metaB.Validate(); err == nil {
		return &metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(txid common.Txid, count int) (*common.Page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*common.Page)(unsafe.Pointer(&buf[0]))
	p.SetOverflow(uint32(count - 1))

	// Use pages from the freelist if they are available.
	p.SetId(db.freelist.Allocate(txid, count))
	if p.Id() != 0 {
		return p, nil
	}

	p.SetId(db.rwtx.meta.Pgid())

	// Move the page id high water mark.
	curPgid := db.rwtx.meta.Pgid()
	db.rwtx.meta.SetPgid(curPgid + common.Pgid(count))

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	lg := db.Logger()

	dataSz, err := db.data.Size()
	if err != nil {
		lg.Errorf("getting data size failed: %v", err)
		return err
	}
	if int64(sz) <= dataSz {
		return nil
	}

	if !db.readOnly && db.MaxSize > 0 && sz > db.MaxSize {
		lg.Errorf("maximum db size reached, size: %d, db.MaxSize: %d", sz, db.MaxSize)
		return berrors.ErrMaxSizeReached
	}

	if err := db.data.Grow(int64(sz)); err != nil {
		lg.Errorf("growing data failed, size: %d, error: %v", sz, err)
		return fmt.Errorf("data grow error: %w", err)
	}

	if !db.NoGrowSync && !db.readOnly {
		if err := db.data.Sync(); err != nil {
			lg.Errorf("syncing data failed, error: %v", err)
			return fmt.Errorf("data sync error: %w", err)
		}
	}

	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

func (db *DB) freepages() []common.Pgid {
	tx, err := db.beginTx()
	defer func() {
		err = tx.Rollback()
		if err != nil {
			panic("freepages: failed to rollback tx")
		}
	}()
	if err != nil {
		panic("freepages: failed to open read only tx")
	}

	reachable := make(map[common.Pgid]*common.Page)
	nofreed := make(map[common.Pgid]bool)
	ech := make(chan error)

	go func() {
		defer close(ech)
		tx.recursivelyCheckBucket(&tx.root, reachable, nofreed, HexKVStringer(), ech)
	}()
	// following for loop will exit once channel is closed in the above goroutine.
	// we don't need to wait explictly with a waitgroup
	for e := range ech {
		panic(fmt.Sprintf("freepages: failed to get all reachable pages (%v)", e))
	}

	// TODO: If check bucket reported any corruptions (ech) we shouldn't proceed to freeing the pages.

	var fids []common.Pgid
	for i := common.Pgid(2); i < db.meta().Pgid(); i++ {
		if _, ok := reachable[i]; !ok {
			fids = append(fids, i)
		}
	}
	return fids
}

func newFreelist(freelistType FreelistType) fl.Interface {
	if freelistType == FreelistMapType {
		return fl.NewHashMapFreelist()
	}
	return fl.NewArrayFreelist()
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// NoGrowSync skips syncing after growing the data.
	NoGrowSync bool

	// Do not sync freelist to disk. This improves the database write performance
	// under normal operation, but requires a full database re-sync during recovery.
	NoFreelistSync bool

	// PreLoadFreelist sets whether to load the free pages when opening
	// the db. Note when opening db in write mode, bbolt will always
	// load the free pages.
	PreLoadFreelist bool

	// FreelistType sets the backend freelist type.
	FreelistType FreelistType

	// Open database in read-only mode.
	ReadOnly bool

	// PageSize overrides the default OS page size.
	PageSize int

	// MaxSize sets the maximum size of the data. <=0 means no maximum.
	MaxSize int

	// NoSync sets the initial value of DB.NoSync.
	NoSync bool

	// Logger is the logger used for bbolt.
	Logger Logger

	// NoStatistics turns off statistics collection.
	NoStatistics bool
}

// DefaultOptions represent the options used if nil options are passed into Open().
var DefaultOptions = &Options{
	FreelistType: FreelistArrayType,
}

// Stats represents statistics about the database.
type Stats struct {
	// Put `TxStats` at the first field to ensure it's 64-bit aligned. Note
	// that the first word in an allocated struct can be relied upon to be
	// 64-bit aligned. Refer to https://pkg.go.dev/sync/atomic#pkg-note-BUG.
	// Also refer to discussion in https://github.com/etcd-io/bbolt/issues/577.
	TxStats TxStats // global, ongoing stats.

	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

type Info struct {
	PageSize int
}
