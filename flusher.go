package loophole

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

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
//
// Durability guarantee: if another flush is already in progress, this
// call waits for it to complete before proceeding. This ensures that
// an fsync caller never gets "success" while data is still in-flight.
func (l *Layer) Flush(ctx context.Context) error {
	t := metrics.NewTimer(metrics.FlushDuration)
	defer t.ObserveDuration()

	// Wait for any in-flight flush to complete first.
	l.flushMu.Lock()
	for l.flushing {
		l.flushCond.Wait()
	}
	l.flushing = true
	l.flushMu.Unlock()

	// Snapshot dirty keys into a local slice. New writes during
	// upload go into dirty and are picked up by the next flush.
	l.mu.Lock()
	items := make([]flushItem, 0, len(l.dirty))
	for blockIdx := range l.dirty {
		f := l.openBlocks[blockIdx]
		if f == nil {
			l.mu.Unlock()
			return fmt.Errorf("dirty block %d has no open file handle", blockIdx)
		}
		items = append(items, flushItem{
			blockIdx: blockIdx,
			key:      blockIdx.String(),
			file:     f,
		})
	}
	l.mu.Unlock()

	var err error
	if len(items) > 0 {
		err = l.flushItems(ctx, items)
	}

	// Release the flush slot and wake waiters.
	l.flushMu.Lock()
	l.flushing = false
	l.flushCond.Broadcast()
	l.flushMu.Unlock()

	return err
}

// flushItems uploads the given blocks concurrently. Each block is removed
// from dirty before upload (releasing backpressure) and re-added on failure.
func (l *Layer) flushItems(ctx context.Context, items []flushItem) error {
	// Stat all files to detect tombstones.
	for i := range items {
		info, err := items[i].file.Stat()
		if err != nil {
			return fmt.Errorf("stat open block %d: %w", items[i].blockIdx, err)
		}
		items[i].tombstone = info.Size() == 0
	}

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, item := range items {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Remove from dirty before upload — frees backpressure capacity.
			l.mu.Lock()
			delete(l.dirty, item.blockIdx)
			l.mu.Unlock()
			l.dirtyCond.Broadcast()

			if err := l.vm.uploadSem.Acquire(ctx, 1); err != nil {
				metrics.FlushErrors.Inc()
				l.mu.Lock()
				l.dirty[item.blockIdx] = struct{}{}
				l.mu.Unlock()
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			metrics.InflightUploads.Inc()
			err := l.flushOne(ctx, item)
			metrics.InflightUploads.Dec()
			l.vm.uploadSem.Release(1)
			if err != nil {
				metrics.FlushErrors.Inc()
				l.mu.Lock()
				l.dirty[item.blockIdx] = struct{}{}
				l.mu.Unlock()
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			metrics.FlushBlocks.Inc()
			if item.tombstone {
				metrics.FlushTombstones.Inc()
			}
		}()
	}
	wg.Wait()

	// Update gauges.
	l.mu.Lock()
	metrics.DirtyBlocks.Set(float64(len(l.dirty)))
	metrics.OpenBlocks.Set(float64(len(l.openBlocks)))
	l.mu.Unlock()

	return firstErr
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backgroundFlushInterval):
		case <-l.flushTrigger:
			metrics.EarlyFlushes.Inc()
		}
		if err := l.Flush(ctx); err != nil {
			slog.Error("background flush failed", "layer", l.id, "error", err)
		}
	}
}
