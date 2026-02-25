package loophole

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole/metrics"
)

// BlockCache is a shared, file-backed block cache. Blocks from frozen
// (immutable) layers go into an immutable directory that persists across
// restarts. Blocks from mutable layers go into a mutable directory that
// is wiped on startup.
//
// A zero-length cache file is a tombstone — the block was explicitly zeroed.
// Writes go to a .tmp file first and are atomically renamed into place.
//
// Fetches are deduplicated: concurrent requests for the same uncached
// block result in a single S3 download.
type BlockCache struct {
	layers    ObjectStore // scoped to "layers/"
	blockSize uint64

	immutableDir string
	mutableDir   string

	flights     singleflight.Group  // deduplicates concurrent S3 fetches
	downloadSem *semaphore.Weighted // bounds concurrent S3 downloads
}

// NewBlockCache creates a BlockCache rooted at dir. It creates the
// immutable/ and mutable/ subdirectories and wipes mutable/ clean.
func NewBlockCache(dir string, layers ObjectStore, blockSize uint64, maxDownloads int) (*BlockCache, error) {
	immutableDir := filepath.Join(dir, "immutable")
	mutableDir := filepath.Join(dir, "mutable")

	// Wipe mutable cache — may be stale from a previous run.
	if err := os.RemoveAll(mutableDir); err != nil {
		slog.Warn("remove all failed", "dir", mutableDir, "error", err)
	}

	if err := os.MkdirAll(immutableDir, 0o755); err != nil {
		return nil, fmt.Errorf("create immutable cache dir: %w", err)
	}
	if err := os.MkdirAll(mutableDir, 0o755); err != nil {
		return nil, fmt.Errorf("create mutable cache dir: %w", err)
	}

	return &BlockCache{
		layers:       layers,
		blockSize:    blockSize,
		immutableDir: immutableDir,
		mutableDir:   mutableDir,
		downloadSem:  semaphore.NewWeighted(int64(maxDownloads)),
	}, nil
}

// GetOrFetch returns an open file handle for an immutable cached block,
// fetching from S3 on a miss. A nil file with tombstone=true means the
// block was explicitly zeroed. Concurrent callers for the same block
// share a single download.
func (bc *BlockCache) GetOrFetch(ctx context.Context, layerID string, blockIdx BlockIdx) (f *os.File, tombstone bool, err error) {
	// Fast path: already cached.
	if f, tombstone, ok := bc.get(layerID, blockIdx); ok {
		metrics.CacheHits.Inc()
		return f, tombstone, nil
	}

	metrics.CacheMisses.Inc()
	// Slow path: fetch from S3 with dedup.
	key := bc.flightKey(layerID, blockIdx)
	_, err, _ = bc.flights.Do(key, func() (any, error) {
		// Re-check cache — another goroutine may have populated it.
		if _, _, ok := bc.get(layerID, blockIdx); ok {
			return nil, nil
		}
		return nil, bc.fetchToCache(ctx, layerID, blockIdx)
	})
	if err != nil {
		return nil, false, err
	}

	f, tombstone, ok := bc.get(layerID, blockIdx)
	if !ok {
		return nil, false, fmt.Errorf("block %s/%d not in cache after fetch", layerID, blockIdx)
	}
	return f, tombstone, nil
}

// fetchToCache downloads a block from S3 and streams it directly into
// the immutable cache file, zero-padding to blockSize.
func (bc *BlockCache) fetchToCache(ctx context.Context, layerID string, blockIdx BlockIdx) error {
	if err := bc.downloadSem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer bc.downloadSem.Release(1)
	metrics.InflightDownloads.Inc()
	defer metrics.InflightDownloads.Dec()
	t := metrics.NewTimer(metrics.CacheFetchDuration)
	defer t.ObserveDuration()

	store := bc.layers.At(layerID)
	key := blockIdx.String()
	body, _, err := store.Get(ctx, key, 0)
	if err != nil {
		return fmt.Errorf("fetch block %s/%d: %w", layerID, blockIdx, err)
	}
	defer func() {
		if err := body.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	path := bc.immutablePath(layerID, blockIdx)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Warn("close failed", "error", cerr)
		}
		if err != nil {
			if rerr := os.Remove(tmp); rerr != nil {
				slog.Warn("remove failed", "path", tmp, "error", rerr)
			}
		}
	}()

	n, err := io.Copy(f, body)
	if err != nil {
		return fmt.Errorf("stream block %s/%d: %w", layerID, blockIdx, err)
	}

	// Zero-pad to blockSize.
	if uint64(n) < bc.blockSize {
		if err = f.Truncate(int64(bc.blockSize)); err != nil {
			return err
		}
	}

	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// get opens a cached immutable block file. Returns the open file,
// whether it's a tombstone (zero-length), and whether it existed.
func (bc *BlockCache) get(layerID string, blockIdx BlockIdx) (f *os.File, tombstone bool, ok bool) {
	path := bc.immutablePath(layerID, blockIdx)
	info, err := os.Stat(path)
	if err != nil {
		return nil, false, false
	}
	if info.Size() == 0 {
		return nil, true, true
	}
	f, err = os.Open(path)
	if err != nil {
		return nil, false, false
	}
	return f, false, true
}

// OpenMutable opens (or creates) a mutable block cache file. If the file
// does not exist, it is cloned from src (using reflink if the filesystem
// supports it), or zero-filled if src is nil. The returned file supports
// ReadAt/WriteAt (pread/pwrite). Caller retains ownership of src.
func (bc *BlockCache) OpenMutable(layerID string, blockIdx BlockIdx, src *os.File) (*os.File, error) {
	path := bc.mutablePath(layerID, blockIdx)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	// Try to open existing file.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err == nil {
		return f, nil
	}

	tmp := path + ".tmp"
	if src != nil {
		if err := cloneOrCopyFile(src.Name(), tmp, bc.blockSize); err != nil {
			return nil, err
		}
	} else {
		if err := createZeroFile(tmp, bc.blockSize); err != nil {
			return nil, err
		}
	}

	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR, 0o644)
}

// cloneOrCopyFile tries a reflink clone first, falling back to a
// streaming copy. The result is truncated/extended to blockSize.
func cloneOrCopyFile(srcPath, dstPath string, blockSize uint64) error {
	// Try reflink clone — instant, zero-copy on supported filesystems.
	if err := cloneFile(srcPath, dstPath); err == nil {
		// Ensure correct size (extend or truncate to blockSize).
		return os.Truncate(dstPath, int64(blockSize))
	}

	// Fallback: streaming copy.
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := src.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}

	n, err := io.Copy(dst, src)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		if rerr := os.Remove(dstPath); rerr != nil {
			slog.Warn("remove failed", "path", dstPath, "error", rerr)
		}
		return err
	}

	// Zero-pad to blockSize.
	if uint64(n) < blockSize {
		return os.Truncate(dstPath, int64(blockSize))
	}
	return nil
}

func createZeroFile(path string, blockSize uint64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	err = f.Truncate(int64(blockSize))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		if rerr := os.Remove(path); rerr != nil {
			slog.Warn("remove failed", "path", path, "error", rerr)
		}
	}
	return err
}

func (bc *BlockCache) immutablePath(layerID string, blockIdx BlockIdx) string {
	return filepath.Join(bc.immutableDir, layerID, blockIdx.String())
}

func (bc *BlockCache) mutablePath(layerID string, blockIdx BlockIdx) string {
	return filepath.Join(bc.mutableDir, layerID, blockIdx.String())
}

func (bc *BlockCache) flightKey(layerID string, blockIdx BlockIdx) string {
	return layerID + "/" + blockIdx.String()
}
