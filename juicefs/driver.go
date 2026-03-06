package juicefs

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/diskcache"
	"github.com/semistrict/loophole/internal/util"
)

// Config holds the dependencies for creating JuiceFS drivers.
// Zero-value fields disable their corresponding features (disk cache, writeback).
type Config struct {
	ObjStore   loophole.ObjectStore
	DiskCache  *diskcache.DiskCache
	StagingDir string // empty = writeback disabled
}

// NewConfig creates a Config with production defaults derived from cacheDir.
// If cacheDir is empty, disk cache and staging are disabled.
func NewConfig(store loophole.ObjectStore, diskCache *diskcache.DiskCache, cacheDir string) Config {
	var stagingDir string
	if cacheDir != "" {
		stagingDir = filepath.Join(cacheDir, "staging")
	}
	return Config{
		ObjStore:   store,
		DiskCache:  diskCache,
		StagingDir: stagingDir,
	}
}

// InProcessDriver implements fsbackend.Driver using JuiceFS VFS directly
// (no FUSE). Works on macOS, Linux, and WASM.
type InProcessDriver struct {
	cfg Config
}

var _ fsbackend.Driver[*juiceFSMount] = (*InProcessDriver)(nil)

// NewInProcessDriver returns a type-erased driver for in-process JuiceFS.
func NewInProcessDriver(cfg Config) fsbackend.AnyDriver {
	if cfg.ObjStore == nil {
		panic("juicefs: NewInProcessDriver requires a non-nil ObjectStore")
	}
	return fsbackend.EraseDriver[*juiceFSMount](&InProcessDriver{cfg: cfg})
}

// NewInProcess creates a Service backed by in-process JuiceFS.
// The store must not be nil — all mounts from this driver share it.
func NewInProcess(vm loophole.VolumeManager, cfg Config) fsbackend.Service {
	return fsbackend.New(vm, NewInProcessDriver(cfg), loophole.VolumeTypeJuiceFS)
}

func (d *InProcessDriver) Format(ctx context.Context, vol loophole.Volume) error {
	m, vd, err := openMeta(ctx, vol)
	if err != nil {
		return fmt.Errorf("open meta for format: %w", err)
	}
	defer func() {
		util.SafeRun(m.Shutdown, "juicefs: format shutdown")
		util.SafeRun(vd.Sync, "juicefs: format sync volume data")
	}()

	if err := formatMeta(m, vol.Name()); err != nil {
		return err
	}

	slog.Info("juicefs: formatted volume", "volume", vol.Name())
	return nil
}

func (d *InProcessDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (*juiceFSMount, error) {
	slog.Info("juicefs: mounting", "volume", vol.Name(), "mountpoint", mountpoint)

	m, vd, err := openMeta(ctx, vol)
	if err != nil {
		return nil, fmt.Errorf("open meta: %w", err)
	}

	format, err := m.Load(false)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("load format: %w", err)
	}
	chunkConf := chunkConfig(format)

	chs, ok := m.(chunkHashStore)
	if !ok {
		_ = m.Shutdown()
		return nil, fmt.Errorf("meta engine does not support chunk hash store (type %T)", m)
	}
	if err := chs.InitChunkHashTable(); err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("init chunk hash table: %w", err)
	}
	store := newCASStore(d.cfg, chs, *chunkConf)

	jfsVFS, err := createVFS(m, format, chunkConf, store)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("create VFS: %w", err)
	}

	return &juiceFSMount{
		jfsVFS:  jfsVFS,
		metaCl:  m,
		store:   store,
		vol:     vol,
		volData: vd,
	}, nil
}

func (d *InProcessDriver) Freeze(ctx context.Context, h *juiceFSMount) error {
	if err := h.jfsVFS.FlushAll(""); err != nil {
		return fmt.Errorf("flush VFS: %w", err)
	}
	if err := h.store.FlushPending(ctx); err != nil {
		return fmt.Errorf("flush pending uploads: %w", err)
	}
	if h.volData != nil {
		if err := h.volData.Sync(); err != nil {
			return fmt.Errorf("sync volume data: %w", err)
		}
	}
	return h.vol.Flush(ctx)
}

func (d *InProcessDriver) Unmount(ctx context.Context, h *juiceFSMount) error {
	return closeMount(ctx, h)
}

func (d *InProcessDriver) Thaw(_ context.Context, _ *juiceFSMount) error {
	return nil
}

func (d *InProcessDriver) Close(_ context.Context) error {
	return nil
}

func (d *InProcessDriver) FS(h *juiceFSMount) (fsbackend.FS, error) {
	return newJuiceFS(h.jfsVFS), nil
}
