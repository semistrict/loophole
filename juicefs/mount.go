// Package juicefs provides a JuiceFS-based filesystem driver for loophole.
//
// JuiceFS metadata is stored in SQLite, backed by a loophole Volume via
// the sqlitevfs VolumeVFS adapter. File data uses content-addressable storage
// (BLAKE3 hashing) under a blobs/ prefix in the same S3 bucket.
//
// Two modes are supported:
//   - ModeJuiceFS (in-process): JuiceFS VFS used directly, no FUSE needed.
//     Works on macOS and Linux.
//   - (future) FUSE mode: JuiceFS VFS → fuse.Serve → kernel mount.
package juicefs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/semistrict/loophole"
	svfs "github.com/semistrict/loophole/sqlitevfs"
)

// juiceFSMount is the per-mount handle shared by both drivers.
type juiceFSMount struct {
	jfsVFS *vfs.VFS
	metaCl meta.Meta
	store  *casStore
	vol    loophole.Volume
	cvfs   *svfs.CVFS // C VFS for the metadata volume

	vfsName    string // registered sqlite3vfs name (for cleanup)
	mountpoint string //nolint:unused // only used by FUSE driver (linux)
	cacheDir   string // temp dir for chunk cache
}

// openMeta opens a JuiceFS meta client backed by a loophole Volume.
// It registers a C VFS (iVersion=3, full SHM/WAL support) named vfsName,
// opens the SQLite meta engine through it, and returns the meta client.
func openMeta(ctx context.Context, vol loophole.Volume, vfsName string) (meta.Meta, *svfs.CVFS, error) {
	cvfs, err := svfs.NewCVFS(ctx, vol, vfsName, svfs.SyncModeAsync)
	if err != nil {
		return nil, nil, fmt.Errorf("open CVFS: %w", err)
	}

	dsn := fmt.Sprintf("sqlite3://file:main.db?vfs=%s&cache=private&_busy_timeout=5000", vfsName)
	conf := meta.DefaultConf()
	conf.NoBGJob = true // We handle lifecycle ourselves.
	m := meta.NewClient(dsn, conf)
	return m, cvfs, nil
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

// chunkConfig builds a chunk.Config from the meta.Format and a cache directory.
// This mirrors JuiceFS's getChunkConf (cmd/mount.go) so the VFS writer and
// chunk store share the same config — critically including BufferSize and
// Writeback — avoiding the split-brain that caused permanent backpressure.
func chunkConfig(format *meta.Format, cacheDir string) *chunk.Config {
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
		CacheDir:       cacheDir,
		CacheSize:      1 << 10, // 1 GiB (in MiB units per JuiceFS convention)
		CacheItems:     1000,
		FreeSpace:      0.01,
		CacheMode:      os.FileMode(0600),
		CacheFullBlock: true,
		CacheChecksum:  "extend",
		CacheEviction:  "2-random",
		AutoCreate:     true,
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

	// Register message callbacks for slice compaction and deletion.
	// Without these, writes fail with EIO once the slice count exceeds
	// the compaction threshold (~350 slices per chunk).
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

// closeMount tears down a juiceFSMount.
func closeMount(ctx context.Context, h *juiceFSMount) error {
	slog.Info("juicefs: closing mount")

	// Flush all pending writes.
	if err := h.jfsVFS.FlushAll(""); err != nil {
		slog.Warn("juicefs: FlushAll error", "error", err)
	}

	_ = h.metaCl.CloseSession()
	_ = h.metaCl.Shutdown()

	if h.cvfs != nil {
		if err := h.cvfs.FlushSuperblock(); err != nil {
			slog.Warn("juicefs: flush superblock error", "error", err)
		}
	}

	if err := h.vol.Flush(ctx); err != nil {
		slog.Warn("juicefs: volume flush error", "error", err)
	}

	if h.cacheDir != "" {
		_ = os.RemoveAll(h.cacheDir)
	}

	return nil
}
