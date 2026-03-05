//go:build linux

package juicefs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfuse "github.com/juicedata/juicefs/pkg/fuse"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/util"
	svfs "github.com/semistrict/loophole/sqlitevfs"
)

// FUSEDriver implements fsbackend.Driver using JuiceFS's built-in FUSE server.
// File I/O goes through the kernel mount (osFS). Linux only.
type FUSEDriver struct {
	objStore loophole.ObjectStore
}

var _ fsbackend.Driver[*juiceFSMount] = (*FUSEDriver)(nil)

// NewFUSE creates a Service backed by JuiceFS FUSE.
func NewFUSE(vm loophole.VolumeManager, store loophole.ObjectStore) fsbackend.Service {
	if store == nil {
		panic("juicefs: NewFUSE requires a non-nil ObjectStore")
	}
	return fsbackend.New[*juiceFSMount](&FUSEDriver{objStore: store}, vm)
}

func (d *FUSEDriver) Format(ctx context.Context, vol loophole.Volume) error {
	if err := svfs.FormatVolume(ctx, vol); err != nil {
		return fmt.Errorf("format volume: %w", err)
	}

	vfsName := fmt.Sprintf("jfs-fmt-%s", vol.Name())
	m, cvfs, err := openMeta(ctx, vol, vfsName)
	if err != nil {
		return fmt.Errorf("open meta for format: %w", err)
	}
	defer func() {
		util.SafeRun(m.Shutdown, "juicefs: format shutdown")
		util.SafeRun(cvfs.FlushHeader, "juicefs: format flush header")
		util.SafeRunCtx(ctx, vol.Flush, "juicefs: format flush volume")
	}()

	if err := formatMeta(m, vol.Name()); err != nil {
		return err
	}

	slog.Info("juicefs-fuse: formatted volume", "volume", vol.Name())
	return nil
}

func (d *FUSEDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (*juiceFSMount, error) {
	slog.Info("juicefs-fuse: mounting", "volume", vol.Name(), "mountpoint", mountpoint)

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

	jfsVFS, err := createVFSWithMountpoint(m, format, chunkConf, store, mountpoint)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("create VFS: %w", err)
	}

	// Create and start FUSE server.
	srv, err := jfuse.CreateServer(jfsVFS, "", false, false)
	if err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("create fuse server: %w", err)
	}

	fuseErr := make(chan error, 1)
	go func() {
		srv.Serve()
		fuseErr <- nil
	}()

	// Wait for the mount to become available.
	if err := waitForMount(mountpoint, fuseErr); err != nil {
		_ = m.Shutdown()
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	slog.Info("juicefs-fuse: mounted", "mountpoint", mountpoint)
	return &juiceFSMount{
		jfsVFS:     jfsVFS,
		metaCl:     m,
		store:      store,
		vol:        vol,
		cvfs:       cvfs,
		vfsName:    vfsName,
		mountpoint: mountpoint,
		fuseServer: srv,
		cacheDir:   cacheDir,
	}, nil
}

func (d *FUSEDriver) Unmount(ctx context.Context, h *juiceFSMount) error {
	slog.Info("juicefs-fuse: unmounting", "mountpoint", h.mountpoint)

	// Shutdown this specific FUSE server (causes Serve goroutine to return).
	if srv, ok := h.fuseServer.(*gofuse.Server); ok && srv != nil {
		srv.Shutdown()
	}

	// Lazy-unmount to detach the mount from the namespace immediately,
	// even if the FUSE server hasn't fully exited yet.
	// Use fusermount -uz (unprivileged, lazy) instead of syscall.Unmount
	// which requires CAP_SYS_ADMIN.
	if out, err := exec.Command("fusermount", "-uz", h.mountpoint).CombinedOutput(); err != nil {
		slog.Warn("juicefs-fuse: lazy unmount failed", "mountpoint", h.mountpoint, "error", err, "output", string(out))
	}

	return closeMount(ctx, h)
}

func (d *FUSEDriver) Freeze(ctx context.Context, h *juiceFSMount) error {
	slog.Info("juicefs-fuse: freezing")
	if err := h.jfsVFS.FlushAll(""); err != nil {
		return fmt.Errorf("flush VFS: %w", err)
	}
	if err := h.store.FlushPending(ctx); err != nil {
		return fmt.Errorf("flush pending uploads: %w", err)
	}
	if h.cvfs != nil {
		if err := h.cvfs.FlushHeader(); err != nil {
			return fmt.Errorf("flush header: %w", err)
		}
	}
	return h.vol.Flush(ctx)
}

func (d *FUSEDriver) Thaw(_ context.Context, _ *juiceFSMount) error {
	return nil
}

func (d *FUSEDriver) Close(_ context.Context) error {
	return nil
}

func (d *FUSEDriver) FS(h *juiceFSMount) (fsbackend.FS, error) {
	return fsbackend.NewOSFS(h.mountpoint), nil
}

// waitForMount polls until the mountpoint is a FUSE mount or an error occurs.
func waitForMount(mountpoint string, fuseErr <-chan error) error {
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case err := <-fuseErr:
			if err != nil {
				return err
			}
			return fmt.Errorf("fuse.Serve returned unexpectedly")
		case <-deadline:
			return fmt.Errorf("timed out waiting for FUSE mount at %s", mountpoint)
		case <-tick.C:
			// Check if something is mounted by trying to stat a file.
			// A FUSE mount will have a different device than the parent.
			fi, err := os.Stat(mountpoint)
			if err != nil {
				continue
			}
			parentFi, err := os.Stat(mountpoint + "/..")
			if err != nil {
				continue
			}
			// If device IDs differ, something is mounted here.
			if !os.SameFile(fi, parentFi) {
				return nil
			}
		}
	}
}
