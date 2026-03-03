package loophole

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole/metrics"
)

// BlockCache is a shared, file-backed cache for immutable (frozen) layer
// blocks. Blocks persist across restarts.
//
// A zero-length cache file is a tombstone — the block was explicitly zeroed.
// Writes go to a .tmp file first and are atomically renamed into place.
//
// Fetches are deduplicated: concurrent requests for the same uncached
// block result in a single S3 download.
type BlockCache struct { // XXX: does this need to be exported?
	layers    ObjectStore // scoped to "layers/"
	downloads *BlockDownloader

	immutableDir string

	flights singleflight.Group // deduplicates concurrent S3 fetches XXX - we should probably move this to BlockDownloader
}

// NewBlockCache creates a BlockCache rooted at dir for immutable layer blocks.
func NewBlockCache(dir string, layers ObjectStore, downloads *BlockDownloader) (*BlockCache, error) {
	immutableDir := filepath.Join(dir, "immutable")
	if err := os.MkdirAll(immutableDir, 0o755); err != nil {
		return nil, fmt.Errorf("create immutable cache dir: %w", err)
	}

	return &BlockCache{
		layers:       layers,
		downloads:    downloads,
		immutableDir: immutableDir,
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

// fetchToCache downloads a block from S3 into the immutable cache.
func (bc *BlockCache) fetchToCache(ctx context.Context, layerID string, blockIdx BlockIdx) error {
	path := bc.immutablePath(layerID, blockIdx)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return bc.downloads.Download(ctx, bc.layers.At(layerID), blockIdx.String(), path)
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

// cloneOrCopyFile tries a reflink clone first, falling back to a
// streaming copy. The result is truncated/extended to blockSize.
func cloneOrCopyFile(srcPath, dstPath string, blockSize uint64) error { // XXX: why is this func in this file?
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

// XXX: some of the functions in this file do not belong here

func createZeroFile(path string, blockSize uint64) error { // XXX: I do not understand the point of doing this
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

// Adopt moves a directory of block files into the immutable cache for
// the given layer. Called when a layer is frozen — its blocks are now
// truly immutable.
func (bc *BlockCache) Adopt(layerID string, srcDir string) error {
	dst := filepath.Join(bc.immutableDir, layerID)
	if err := os.RemoveAll(dst); err != nil { // XXX: we should fail if the destination dir exists, remove this RemoveAll
		return err
	}
	return os.Rename(srcDir, dst)
}

func (bc *BlockCache) immutablePath(layerID string, blockIdx BlockIdx) string {
	return filepath.Join(bc.immutableDir, layerID, blockIdx.String())
}

func (bc *BlockCache) flightKey(layerID string, blockIdx BlockIdx) string {
	return layerID + "/" + blockIdx.String()
}
