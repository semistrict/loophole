package loophole

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"golang.org/x/sync/semaphore"

	"github.com/semistrict/loophole/metrics"
)

// BlockDownloader handles downloading blocks from S3 to local files with
// shared concurrency limits, zero-padding, and atomic writes.
type BlockDownloader struct { // XXX: does this need to be exported?
	blockSize uint64
	sem       *semaphore.Weighted
}

// XXX: we should probably have this implement singleflight logic rather than separately in layer and block_store

// Download fetches a block from store and writes it atomically to destPath
// (via tmp + rename). Zero-pads to blockSize. A zero-length S3 object
// produces a zero-length file (tombstone).
func (dm *BlockDownloader) Download(ctx context.Context, store ObjectStore, key string, destPath string) error {
	if err := dm.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer dm.sem.Release(1)
	metrics.InflightDownloads.Inc()
	defer metrics.InflightDownloads.Dec()
	t := metrics.NewTimer(metrics.CacheFetchDuration)
	defer t.ObserveDuration()

	body, _, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	defer func() {
		if err := body.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	tmp := destPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			if cerr := f.Close(); cerr != nil {
				slog.Warn("close failed", "error", cerr)
			}
		}
		if err != nil {
			if rerr := os.Remove(tmp); rerr != nil {
				slog.Warn("remove failed", "path", tmp, "error", rerr)
			}
		}
	}()

	n, err := io.Copy(f, body)
	if err != nil {
		return fmt.Errorf("stream %s: %w", key, err)
	}

	// Zero-pad to blockSize (tombstones with n==0 stay zero-length).
	if n > 0 && uint64(n) < dm.blockSize {
		if err = f.Truncate(int64(dm.blockSize)); err != nil {
			return err
		}
	}

	err = f.Close()
	f = nil // prevent defer from closing again
	if err != nil {
		return err
	}
	return os.Rename(tmp, destPath)
}
