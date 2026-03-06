// Package juicefs provides a JuiceFS-based filesystem driver for loophole.
//
// JuiceFS metadata is stored in bbolt, backed by a loophole Volume via
// the VolumeData adapter. File data uses content-addressable storage
// (BLAKE3 hashing) under a blobs/ prefix in the same S3 bucket.
//
// Two modes are supported:
//   - InProcess (LOOPHOLE_MODE=inprocess LOOPHOLE_DEFAULT_FS=juicefs): JuiceFS VFS
//     used directly, no FUSE needed. Works on macOS, Linux, and WASM.
//   - FUSE (LOOPHOLE_MODE=fusefs LOOPHOLE_DEFAULT_FS=juicefs): JuiceFS VFS →
//     fuse.Serve → kernel mount. Linux only.
package juicefs

import (
	"context"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/semistrict/loophole"
)

// juiceFSMount is the per-mount handle shared by all drivers.
type juiceFSMount struct {
	jfsVFS  *vfs.VFS
	metaCl  meta.Meta
	store   *casStore
	vol     loophole.Volume
	volData *VolumeData

	mountpoint string //nolint:unused // only used by FUSE driver (linux)
	fuseServer any    //nolint:unused // *fuse.Server, only used by FUSE driver (linux)
}

// openMeta opens a JuiceFS meta client backed by a loophole Volume via bbolt.
func openMeta(ctx context.Context, vol loophole.Volume) (meta.Meta, *VolumeData, error) {
	vd, err := NewVolumeData(ctx, vol)
	if err != nil {
		return nil, nil, fmt.Errorf("create VolumeData: %w", err)
	}

	addr := vol.Name()
	meta.SetBoltData(addr, vd, vol.ReadOnly())

	dsn := fmt.Sprintf("loophole://%s", addr)
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	conf.ReadOnly = vol.ReadOnly()
	m := meta.NewClient(dsn, conf)
	return m, vd, nil
}

// closeMount tears down a juiceFSMount.
func closeMount(ctx context.Context, h *juiceFSMount) error {
	closeMountShared(ctx, h)

	if h.volData != nil && !h.vol.ReadOnly() {
		if err := h.volData.Sync(); err != nil {
			slog.Warn("juicefs: volume data sync error", "error", err)
		}
	}

	return nil
}

// formatMeta initializes JuiceFS metadata in a fresh volume.
func formatMeta(m meta.Meta, volumeName string) error {
	format := &meta.Format{
		Name:        volumeName,
		UUID:        uuid.New().String(),
		Storage:     "loophole",
		BlockSize:   4096, // 4MB in KiB (JuiceFS convention: KiB)
		Compression: "none",
		TrashDays:   0, // No trash — COW handles versioning.
		MetaVersion: 1,
		DirStats:    true,
	}
	if err := m.Init(format, false); err != nil {
		return fmt.Errorf("juicefs format: %w", err)
	}
	return nil
}

// chunkConfig builds a chunk.Config from the meta.Format.
func chunkConfig(format *meta.Format) *chunk.Config {
	conf := &chunk.Config{
		BlockSize:      format.BlockSize * 1024, // KiB → bytes
		Compress:       format.Compression,
		HashPrefix:     format.HashPrefix,
		GetTimeout:     time.Minute,
		PutTimeout:     time.Minute * 5,
		MaxUpload:      20,
		MaxDownload:    200,
		MaxRetries:     10,
		Writeback:      true,
		Prefetch:       1,
		BufferSize:     300 << 20, // 300 MiB
		CacheDir:       "",
		CacheSize:      0,
		CacheItems:     1000,
		FreeSpace:      0.01,
		CacheMode:      0o600,
		CacheFullBlock: true,
		CacheChecksum:  "extend",
		CacheEviction:  "2-random",
		AutoCreate:     false,
		OSCache:        true,
	}
	conf.SelfCheck(format.UUID)
	return conf
}

// createVFS creates a JuiceFS VFS instance (no FUSE mountpoint).
func createVFS(m meta.Meta, format *meta.Format, chunkConf *chunk.Config, store chunk.ChunkStore) (*vfs.VFS, error) {
	return createVFSWithMountpoint(m, format, chunkConf, store, "")
}

// createVFSWithMountpoint creates a JuiceFS VFS with an optional FUSE mountpoint.
func createVFSWithMountpoint(m meta.Meta, format *meta.Format, chunkConf *chunk.Config, store chunk.ChunkStore, mountpoint string) (*vfs.VFS, error) {
	if err := m.NewSession(false); err != nil {
		return nil, fmt.Errorf("new session: %w", err)
	}

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mountpoint

	conf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Chunk:           chunkConf,
		HideInternal:    true,
		PrefixInternal:  false,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
		Mountpoint:      mountpoint,
		FuseOpts:        &vfs.FuseOptions{},
	}

	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(*chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})

	v := vfs.NewVFS(conf, m, store, nil, nil)
	return v, nil
}

// errnoErr converts a syscall.Errno to an error, or nil if 0.
func errnoErr(errno syscall.Errno) error {
	if errno == 0 {
		return nil
	}
	return errno
}

// closeMountShared tears down the shared parts of a juiceFSMount.
func closeMountShared(ctx context.Context, h *juiceFSMount) {
	slog.Debug("juicefs: closing mount")

	if err := h.jfsVFS.FlushAll(""); err != nil {
		slog.Warn("juicefs: FlushAll error", "error", err)
	}

	if h.store != nil {
		if err := h.store.FlushPending(ctx); err != nil {
			slog.Warn("juicefs: FlushPending error", "error", err)
		}
		h.store.Close()
	}

	_ = h.metaCl.CloseSession()
	_ = h.metaCl.Shutdown()
}
