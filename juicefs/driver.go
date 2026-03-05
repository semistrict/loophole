package juicefs

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/util"
	svfs "github.com/semistrict/loophole/sqlitevfs"
)

// InProcessDriver implements fsbackend.Driver using JuiceFS VFS directly
// (no FUSE). Works on macOS and Linux.
type InProcessDriver struct {
	objStore loophole.ObjectStore
}

var _ fsbackend.Driver[*juiceFSMount] = (*InProcessDriver)(nil)

// NewInProcess creates a Service backed by in-process JuiceFS.
// The store must not be nil — all mounts from this driver share it.
func NewInProcess(vm loophole.VolumeManager, store loophole.ObjectStore) fsbackend.Service {
	if store == nil {
		panic("juicefs: NewInProcess requires a non-nil ObjectStore")
	}
	return fsbackend.New[*juiceFSMount](&InProcessDriver{objStore: store}, vm)
}

func (d *InProcessDriver) Format(ctx context.Context, vol loophole.Volume) error {
	// 1. Format the volume with sqlitevfs superblock.
	if _, err := svfs.FormatVolume(ctx, vol); err != nil {
		return fmt.Errorf("format volume: %w", err)
	}

	// 2. Open meta via mattn VFS and initialize JuiceFS.
	vfsName := fmt.Sprintf("jfs-fmt-%s", vol.Name())
	m, cvfs, err := openMeta(ctx, vol, vfsName)
	if err != nil {
		return fmt.Errorf("open meta for format: %w", err)
	}
	defer func() {
		util.SafeRun(m.Shutdown, "juicefs: format shutdown")
		util.SafeRun(cvfs.FlushSuperblock, "juicefs: format flush superblock")
		util.SafeRunCtx(ctx, vol.Flush, "juicefs: format flush volume")
	}()

	// 3. Initialize JuiceFS metadata.
	if err := formatMeta(m, vol.Name()); err != nil {
		return err
	}

	slog.Info("juicefs: formatted volume", "volume", vol.Name())
	return nil
}

func (d *InProcessDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (*juiceFSMount, error) {
	slog.Info("juicefs: mounting", "volume", vol.Name(), "mountpoint", mountpoint)

	vfsName := fmt.Sprintf("jfs-%s", vol.Name())
	m, cvfs, err := openMeta(ctx, vol, vfsName)
	if err != nil {
		return nil, fmt.Errorf("open meta: %w", err)
	}

	cacheDir, err := os.MkdirTemp("", "jfs-cache-*")
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	format, err := m.Load(false)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("load format: %w", err)
	}
	chunkConf := chunkConfig(format, cacheDir)

	chs, ok := m.(chunkHashStore)
	if !ok {
		_ = m.Shutdown()
		return nil, fmt.Errorf("meta engine does not support chunk hash store (type %T)", m)
	}
	if err := chs.InitChunkHashTable(); err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("init chunk hash table: %w", err)
	}
	store := newCASStore(d.objStore, chs, *chunkConf)

	jfsVFS, err := createVFS(m, format, chunkConf, store)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("create VFS: %w", err)
	}

	return &juiceFSMount{
		jfsVFS:   jfsVFS,
		metaCl:   m,
		store:    store,
		vol:      vol,
		cvfs:     cvfs,
		vfsName:  vfsName,
		cacheDir: cacheDir,
	}, nil
}

func (d *InProcessDriver) Unmount(ctx context.Context, h *juiceFSMount) error {
	return closeMount(ctx, h)
}

func (d *InProcessDriver) Freeze(ctx context.Context, h *juiceFSMount) error {
	slog.Info("juicefs: freezing")
	// Flush all JuiceFS buffered data to the chunk store.
	if err := h.jfsVFS.FlushAll(""); err != nil {
		return fmt.Errorf("flush VFS: %w", err)
	}
	// Wait for writeback uploads to reach the blob store.
	if err := h.store.FlushPending(ctx); err != nil {
		return fmt.Errorf("flush pending uploads: %w", err)
	}
	// Flush metadata to volume.
	if h.cvfs != nil {
		if err := h.cvfs.FlushSuperblock(); err != nil {
			return fmt.Errorf("flush superblock: %w", err)
		}
	}
	return h.vol.Flush(ctx)
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
