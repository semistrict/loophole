package sqlitevfs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/semistrict/loophole"
)

const (
	defaultVolumeSize    = 1024 * 1024 * 1024 // 1GB
	defaultFlushInterval = 5 * time.Second
)

type options struct {
	size          uint64
	syncMode      SyncMode
	flushInterval time.Duration
}

func defaults() options {
	return options{
		size:          defaultVolumeSize,
		flushInterval: defaultFlushInterval,
	}
}

// Option configures a DB.
type Option func(*options)

// WithSize sets the volume size for Create (default 1GB).
func WithSize(size uint64) Option {
	return func(o *options) { o.size = size }
}

// WithSyncMode sets the sync mode (default SyncModeSync).
func WithSyncMode(mode SyncMode) Option {
	return func(o *options) { o.syncMode = mode }
}

// WithFlushInterval sets the flush interval for async mode (default 5s).
func WithFlushInterval(d time.Duration) Option {
	return func(o *options) { o.flushInterval = d }
}

// DB is a high-level handle to a SQLite database stored in a loophole volume.
type DB struct {
	vol     loophole.Volume
	vfs     *VolumeVFS
	manager loophole.VolumeManager

	flushStop chan struct{}
	flushDone chan struct{}
	closeOnce sync.Once
}

// Create creates a new volume, formats it with a superblock, and returns a DB.
func Create(ctx context.Context, mgr loophole.VolumeManager, name string, opts ...Option) (*DB, error) {
	o := defaults()
	for _, fn := range opts {
		fn(&o)
	}
	if o.size < MinVolumeSize {
		return nil, fmt.Errorf("volume size %d too small (minimum %d)", o.size, MinVolumeSize)
	}

	vol, err := mgr.NewVolume(ctx, name, o.size, loophole.VolumeTypeSQLite)
	if err != nil {
		return nil, fmt.Errorf("create volume: %w", err)
	}

	err = FormatVolume(ctx, vol)
	if err != nil {
		_ = vol.ReleaseRef(ctx)
		return nil, fmt.Errorf("format volume: %w", err)
	}

	return openDB(ctx, vol, mgr, o)
}

// Open opens an existing volume as a DB.
func Open(ctx context.Context, mgr loophole.VolumeManager, name string, opts ...Option) (*DB, error) {
	o := defaults()
	for _, fn := range opts {
		fn(&o)
	}

	vol, err := mgr.OpenVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("open volume: %w", err)
	}

	return openDB(ctx, vol, mgr, o)
}

func openDB(ctx context.Context, vol loophole.Volume, mgr loophole.VolumeManager, o options) (*DB, error) {
	vfs, err := NewVolumeVFS(ctx, vol, o.syncMode)
	if err != nil {
		_ = vol.ReleaseRef(ctx)
		return nil, err
	}

	db := &DB{
		vol:     vol,
		vfs:     vfs,
		manager: mgr,
	}

	if o.syncMode == SyncModeAsync {
		db.startFlushLoop(ctx, o.flushInterval)
	}

	return db, nil
}

// VFS returns the VFS for SQLite driver registration via [vfs.Register].
func (db *DB) VFS() *VolumeVFS {
	return db.vfs
}

// Flush explicitly flushes all pending data to S3.
func (db *DB) Flush(ctx context.Context) error {
	if err := db.vfs.FlushHeader(); err != nil {
		return err
	}
	return db.vol.Flush(ctx)
}

// Snapshot flushes and creates a read-only snapshot of the database.
func (db *DB) Snapshot(ctx context.Context, name string) error {
	if err := db.Flush(ctx); err != nil {
		return fmt.Errorf("pre-snapshot flush: %w", err)
	}
	return db.vol.Snapshot(ctx, name)
}

// OpenSnapshot opens a snapshot as a read-only DB.
func OpenSnapshot(ctx context.Context, mgr loophole.VolumeManager, name string) (*DB, error) {
	vol, err := mgr.OpenVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("open snapshot: %w", err)
	}

	vfs, err := NewVolumeVFS(ctx, vol, SyncModeSync)
	if err != nil {
		_ = vol.ReleaseRef(ctx)
		return nil, err
	}

	return &DB{
		vol:     vol,
		vfs:     vfs,
		manager: mgr,
	}, nil
}

// Branch flushes, clones the volume, and returns a new writable DB.
func (db *DB) Branch(ctx context.Context, name string) (*DB, error) {
	if err := db.Flush(ctx); err != nil {
		return nil, fmt.Errorf("pre-branch flush: %w", err)
	}
	cloneVol, err := db.vol.Clone(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("clone volume: %w", err)
	}

	// Inherit sync mode from parent.
	syncMode := db.vfs.SyncMode()
	vfs, err := NewVolumeVFS(ctx, cloneVol, syncMode)
	if err != nil {
		_ = cloneVol.ReleaseRef(ctx)
		return nil, err
	}

	branchDB := &DB{
		vol:     cloneVol,
		vfs:     vfs,
		manager: db.manager,
	}

	if syncMode == SyncModeAsync {
		branchDB.startFlushLoop(ctx, defaultFlushInterval)
	}

	return branchDB, nil
}

// OpenFollow opens a volume read-only with periodic refresh of the layer map
// from S3. This allows following a live volume being written to by another process.
// The returned DB re-reads the superblock and layer map on each refresh tick,
// so new queries see the latest flushed state.
func OpenFollow(ctx context.Context, mgr loophole.VolumeManager, name string, interval time.Duration) (*DB, error) {
	vol, err := mgr.OpenVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("open volume for follow: %w", err)
	}

	vfs, err := NewVolumeVFS(ctx, vol, SyncModeSync)
	if err != nil {
		_ = vol.ReleaseRef(ctx)
		return nil, err
	}

	db := &DB{
		vol:     vol,
		vfs:     vfs,
		manager: mgr,
	}
	db.startRefreshLoop(ctx, interval)
	return db, nil
}

func (db *DB) startRefreshLoop(ctx context.Context, interval time.Duration) {
	db.startTickerLoop(interval, func() {
		// Re-read layer map from S3 to see new data.
		_ = db.vol.Refresh(ctx)
		// Re-read header to see updated file sizes.
		if h, err := ReadHeader(ctx, db.vol); err == nil {
			db.vfs.SetHeader(h)
		}
	})
}

func (db *DB) startFlushLoop(ctx context.Context, interval time.Duration) {
	db.startTickerLoop(interval, func() {
		_ = db.Flush(ctx)
	})
}

// startTickerLoop runs fn on a periodic ticker until the DB is closed.
func (db *DB) startTickerLoop(interval time.Duration, fn func()) {
	db.flushStop = make(chan struct{})
	db.flushDone = make(chan struct{})
	go func() {
		defer close(db.flushDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fn()
			case <-db.flushStop:
				return
			}
		}
	}()
}

// Close flushes, stops the background goroutine, and releases the volume.
func (db *DB) Close(ctx context.Context) error {
	var closeErr error
	db.closeOnce.Do(func() {
		if db.flushStop != nil {
			close(db.flushStop)
			<-db.flushDone
		}

		if err := db.Flush(ctx); err != nil {
			closeErr = err
		}

		if err := db.vol.ReleaseRef(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

// Volume returns the underlying volume (for advanced operations).
func (db *DB) Volume() loophole.Volume {
	return db.vol
}
