package loophole

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole/metrics"
)

// flushItem identifies a dirty block and its open file handle.
type flushItem struct {
	blockIdx  BlockIdx
	key       string
	file      *os.File // open mutable cache file
	tombstone bool     // true if file is zero-length (punched hole)
}

// Flush uploads all dirty blocks concurrently. Each block is read into
// a pooled memory buffer via pread (safe with concurrent pwrite) and
// uploaded from that buffer. Max memory: maxUploads * blockSize (e.g.
// 20 * 4MB = 80MB).
//
// On partial failure, blocks that were not successfully uploaded are
// re-marked dirty so they will be retried on the next flush.
func (l *Layer) Flush(ctx context.Context) error {
	t := metrics.NewTimer(metrics.FlushDuration)
	defer t.ObserveDuration()

	// Grab the dirty set atomically.
	l.mu.Lock()
	dirty := l.dirty
	l.dirty = make(map[BlockIdx]struct{})
	l.mu.Unlock()

	if len(dirty) == 0 {
		return nil
	}

	// Build the upload list: just block index + file path, no data read.
	items := make([]flushItem, 0, len(dirty))
	for blockIdx := range dirty {
		l.mu.Lock()
		f := l.openBlocks[blockIdx]
		l.mu.Unlock()

		if f == nil {
			return fmt.Errorf("dirty block %d has no open file handle", blockIdx)
		}

		info, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat open block %d: %w", blockIdx, err)
		}

		items = append(items, flushItem{
			blockIdx:  blockIdx,
			key:       blockIdx.String(),
			file:      f,
			tombstone: info.Size() == 0,
		})
	}

	// Upload all blocks concurrently.
	var failMu sync.Mutex
	var failed []BlockIdx

	var g errgroup.Group
	for _, item := range items {
		g.Go(func() error {
			if err := l.vm.uploadSem.Acquire(ctx, 1); err != nil {
				metrics.FlushErrors.Inc()
				failMu.Lock()
				failed = append(failed, item.blockIdx)
				failMu.Unlock()
				return err
			}
			metrics.InflightUploads.Inc()
			err := l.flushOne(ctx, item)
			metrics.InflightUploads.Dec()
			l.vm.uploadSem.Release(1)
			if err != nil {
				metrics.FlushErrors.Inc()
				failMu.Lock()
				failed = append(failed, item.blockIdx)
				failMu.Unlock()
				return err
			}
			metrics.FlushBlocks.Inc()
			if item.tombstone {
				metrics.FlushTombstones.Inc()
			}
			return nil
		})
	}

	err := g.Wait()

	// Re-dirty any blocks that failed to upload.
	if len(failed) > 0 {
		l.mu.Lock()
		for _, blockIdx := range failed {
			l.dirty[blockIdx] = struct{}{}
		}
		l.mu.Unlock()
	}

	// Update gauges.
	l.mu.Lock()
	metrics.DirtyBlocks.Set(float64(len(l.dirty)))
	metrics.OpenBlocks.Set(float64(len(l.openBlocks)))
	l.mu.Unlock()

	return err
}

func (l *Layer) flushOne(ctx context.Context, item flushItem) error {
	if item.tombstone {
		if l.refBlockIndex[item.blockIdx] != "" {
			// Ancestor owns this block — write an explicit tombstone.
			if err := l.base.PutReader(ctx, item.key, bytes.NewReader(nil)); err != nil {
				return fmt.Errorf("write tombstone %d: %w", item.blockIdx, err)
			}
		} else {
			// No ancestor owns it — just delete our local copy if any.
			l.mu.Lock()
			_, hadLocal := l.localIndex[item.blockIdx]
			l.mu.Unlock()
			if hadLocal {
				if err := l.base.DeleteObject(ctx, item.key); err != nil {
					return fmt.Errorf("delete block %d: %w", item.blockIdx, err)
				}
				l.mu.Lock()
				delete(l.localIndex, item.blockIdx)
				l.mu.Unlock()
			}
		}
		return nil
	}

	// Snapshot the block into a memory buffer using pread (offset-
	// independent, safe with concurrent pwrite from NBD/FUSE).
	bufp := l.vm.flushPool.Get().(*[]byte)
	defer l.vm.flushPool.Put(bufp)
	buf := *bufp

	n, err := readFullAt(item.file, buf)
	if err != nil {
		return fmt.Errorf("snapshot block %d: %w", item.blockIdx, err)
	}

	data := buf[:n]

	// Check if the block is all zeros — treat as tombstone.
	if isAllZero(data) {
		item.tombstone = true
		return l.flushOne(ctx, item)
	}

	// Upload the buffer to S3.
	if err := l.base.PutReader(ctx, item.key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload block %d: %w", item.blockIdx, err)
	}
	l.mu.Lock()
	l.localIndex[item.blockIdx] = struct{}{}
	l.mu.Unlock()
	return nil
}

// readFullAt reads the entire file from offset 0 into buf using pread.
// Returns the number of bytes read. The file's seek position is not affected.
func readFullAt(f *os.File, buf []byte) (int, error) {
	var total int
	for total < len(buf) {
		n, err := f.ReadAt(buf[total:], int64(total))
		total += n
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return total, err
		}
	}
	return total, nil
}

// isAllZero returns true if every byte in data is zero.
func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

const backgroundFlushInterval = 30 * time.Second

func (l *Layer) backgroundFlush(ctx context.Context) {
	defer close(l.flushStopped)
	ticker := time.NewTicker(backgroundFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.Flush(ctx); err != nil {
				slog.Error("background flush failed", "layer", l.id, "error", err)
			}
		}
	}
}
