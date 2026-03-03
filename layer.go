package loophole

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole/internal/util"

	"github.com/semistrict/loophole/metrics"
)

// BlockIdx is a zero-based block index within a volume.
type BlockIdx uint64

func (b BlockIdx) String() string { return fmt.Sprintf("%016x", uint64(b)) }

// ParseBlockIdx parses a 16-char hex string into a BlockIdx.
func ParseBlockIdx(s string) (BlockIdx, error) {
	n, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	return BlockIdx(n), nil
}

// XXX: does Layer need to be exported?

// Layer is a single S3 prefix containing blocks and a state.json.
type Layer struct {
	vm         *legacyVolumeManager
	base       ObjectStore // scoped to this layer (e.g. "layers/<id>/")
	cache      *BlockCache
	mutableDir string // per-layer directory for mutable block files
	id         string

	refLayers []string // referenced layers for child creation

	// refBlockIndex maps block index → referenced layer ID.
	// Immutable after construction (except for CopyFrom which adds entries).
	// Closer ancestors override farther ones; tombstones (size-0 keys) remove entries.
	refBlockIndex map[BlockIdx]string

	// dirty tracks block indices written locally but not yet uploaded.
	dirty map[BlockIdx]struct{}
	// localIndex tracks block indices that exist in S3 for this layer
	// (populated at load time, updated after uploads).
	localIndex map[BlockIdx]struct{}
	// openBlocks holds open file handles for mutable block cache files.
	openBlocks map[BlockIdx]*os.File // XXX: we need to globally limit the number of file handles we hold onto

	frozen    atomic.Bool
	closeOnce sync.Once

	mu           sync.Mutex         // protects dirty, localIndex, openBlocks
	dirtyCond    *sync.Cond         // signaled when dirty count decreases (wakes blocked writers)
	openBlockSF  singleflight.Group // deduplicates concurrent openBlock calls
	stopFlush    context.CancelFunc
	flushStopped chan struct{} // closed when background flush exits
	flushTrigger chan struct{} // buffered(1), poked to trigger early flush

	flushMu   sync.Mutex // serializes flush cycles
	flushCond *sync.Cond // signaled when a flush cycle completes
	flushing  bool       // true while a flush is in progress
}

func (l *Layer) ID() string   { return l.id }
func (l *Layer) Frozen() bool { return l.frozen.Load() }

// checkWritable returns an error if the layer is frozen.
func (l *Layer) checkWritable() error {
	if l.frozen.Load() {
		return fmt.Errorf("layer %q is frozen", l.id)
	}
	return nil
}

// blockSpan describes a byte range within a single block.
type blockSpan struct {
	BlockIdx   BlockIdx
	OffInBlock uint64
	Length     uint64
	Pos        int // byte offset into the caller's buffer // XXX: this is weird
}

// blockSpans yields block-aligned spans covering [offset, offset+length).
func blockSpans(blockSize, offset, length uint64) iter.Seq[blockSpan] {
	return func(yield func(blockSpan) bool) {
		for pos := uint64(0); pos < length; {
			abs := offset + pos
			offInBlock := abs % blockSize
			n := min(blockSize-offInBlock, length-pos)
			if !yield(blockSpan{
				BlockIdx:   BlockIdx(abs / blockSize),
				OffInBlock: offInBlock,
				Length:     n,
				Pos:        int(pos),
			}) {
				return
			}
			pos += n
		}
	}
}

// IsEmpty returns true if this layer has no local blocks and no dirty blocks.
func (l *Layer) IsEmpty() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.localIndex) == 0 && len(l.dirty) == 0 && len(l.openBlocks) == 0
}

// Read reads len(buf) bytes starting at offset into buf.
// Unwritten regions return zeros.
func (l *Layer) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	t := metrics.NewTimer(metrics.ReadDuration)
	defer t.ObserveDuration()
	metrics.ReadBytes.Add(float64(len(buf)))

	for s := range blockSpans(l.vm.blockSize, offset, uint64(len(buf))) {
		if err := l.readBlockAt(ctx, s.BlockIdx, buf[s.Pos:s.Pos+int(s.Length)], s.OffInBlock); err != nil {
			return 0, err
		}
	}
	return len(buf), nil
}

// readBlockAt reads n bytes from blockIdx at offInBlock into buf.
// For mutable blocks with open file handles, this uses pread.
// For frozen/ancestor blocks, it fetches the full block from cache/S3.
func (l *Layer) readBlockAt(ctx context.Context, blockIdx BlockIdx, buf []byte, offInBlock uint64) error {
	// 1. Open mutable block file — use pread.
	l.mu.Lock()
	f := l.openBlocks[blockIdx]
	l.mu.Unlock()

	if f != nil {
		metrics.ReadSource.WithLabelValues("open_block").Inc()
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if info.Size() == 0 {
			// Tombstone — return zeros.
			clear(buf)
			return nil
		}
		_, err = f.ReadAt(buf, int64(offInBlock))
		return err
	}

	// No fallback to mutable cache files on disk: the mutable dir is wiped
	// on startup, so every mutable block has an open file handle above.

	// 2. This layer's S3 blocks.
	l.mu.Lock()
	_, inLocal := l.localIndex[blockIdx]
	l.mu.Unlock()

	if l.frozen.Load() {
		metrics.ReadSource.WithLabelValues("local").Inc()
		return l.readCached(ctx, l.id, blockIdx, buf, offInBlock)
	}

	// Mutable layer with a block in S3: bring it into openBlocks
	// (mutable cache) rather than the immutable cache, since the
	// block may have been overwritten by another process.
	if inLocal {
		metrics.ReadSource.WithLabelValues("local").Inc()
		f, err := l.openBlock(ctx, blockIdx)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if info.Size() == 0 {
			clear(buf)
			return nil
		}
		_, err = f.ReadAt(buf, int64(offInBlock))
		return err
	}

	// 3. Referenced layer block.
	ownerID := l.refBlockIndex[blockIdx]
	if ownerID == "" {
		metrics.ReadSource.WithLabelValues("zero").Inc()
		clear(buf)
		return nil
	}

	metrics.ReadSource.WithLabelValues("ref").Inc()
	return l.readCached(ctx, ownerID, blockIdx, buf, offInBlock)
}

// readCached reads from an immutable cached block via pread.
func (l *Layer) readCached(ctx context.Context, layerID string, blockIdx BlockIdx, buf []byte, offInBlock uint64) error {
	f, tombstone, err := l.cache.GetOrFetch(ctx, layerID, blockIdx)
	if err != nil {
		return err
	}
	if tombstone {
		clear(buf)
		return nil
	}
	defer util.SafeClose(f, "cache read failed")

	_, err = f.ReadAt(buf, int64(offInBlock))
	return err
}

// openBlock returns the open file handle for a mutable block,
// creating and populating the cache file if necessary.
// Uses singleflight to deduplicate concurrent fetches for the same block.
func (l *Layer) openBlock(ctx context.Context, blockIdx BlockIdx) (*os.File, error) {
	l.mu.Lock()
	f := l.openBlocks[blockIdx]
	l.mu.Unlock()

	if f != nil {
		return f, nil
	}

	// Use singleflight to ensure only one goroutine fetches from S3
	// and creates the mutable file for a given block index.
	key := blockIdx.String()
	v, err, shared := l.openBlockSF.Do(key, func() (any, error) {
		// Re-check under lock — another call may have completed.
		l.mu.Lock()
		if f := l.openBlocks[blockIdx]; f != nil {
			l.mu.Unlock()
			return f, nil
		}
		_, inLocal := l.localIndex[blockIdx]
		l.mu.Unlock()

		if inLocal {
			// Own mutable block: download directly to the mutable dir,
			// bypassing the immutable cache (which may hold stale data
			// from a previous process that wrote to this layer).
			if err := l.fetchOwnBlock(ctx, blockIdx); err != nil {
				return nil, err
			}
		} else if ownerID := l.refBlockIndex[blockIdx]; ownerID != "" {
			// Referenced (frozen ancestor) block: use immutable cache,
			// then clone/copy into the mutable dir.
			src, tombstone, err := l.cache.GetOrFetch(ctx, ownerID, blockIdx)
			if err != nil {
				return nil, err
			}
			if err := l.populateMutableFile(blockIdx, src, tombstone); err != nil {
				if src != nil {
					util.SafeClose(src, "close cached block")
				}
				return nil, err
			}
			if src != nil {
				util.SafeClose(src, "close cached block")
			}
		} else {
			// Unwritten block: create a zero-filled mutable file.
			if err := l.populateMutableFile(blockIdx, nil, false); err != nil {
				return nil, err
			}
		}

		path := filepath.Join(l.mutableDir, blockIdx.String())
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}

		l.mu.Lock()
		l.openBlocks[blockIdx] = f
		l.mu.Unlock()

		return f, nil
	})
	if err != nil {
		return nil, err
	}
	if shared {
		metrics.OpenBlockDedup.Inc()
	}
	return v.(*os.File), nil
}

// populateMutableFile creates a mutable block file from an immutable cache
// source. If src is non-nil, clones/copies it; if tombstone, creates a
// zero-length file; otherwise creates a zero-filled file.
// XXX - when would this create a zero-filled file and why????
func (l *Layer) populateMutableFile(blockIdx BlockIdx, src *os.File, tombstone bool) error {
	path := filepath.Join(l.mutableDir, blockIdx.String())

	// Already exists (e.g. race with singleflight re-check).
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	tmp := path + ".tmp"
	if tombstone || src == nil {
		if err := createZeroFile(tmp, l.vm.blockSize); err != nil {
			return err
		}
	} else {
		if err := cloneOrCopyFile(src.Name(), tmp, l.vm.blockSize); err != nil {
			return err
		}
	}
	return os.Rename(tmp, path)
}

// fetchOwnBlock downloads a block from this layer's S3 prefix directly
// into the mutable dir, bypassing the immutable cache.
func (l *Layer) fetchOwnBlock(ctx context.Context, blockIdx BlockIdx) error {
	path := filepath.Join(l.mutableDir, blockIdx.String())
	return l.vm.Download(ctx, l.base, blockIdx.String(), path)
}

// doWriteBlock waits if the dirty block count is at capacity (backpressure),
// calls fn to perform the actual write, then marks the block dirty.
// Only the flusher reduces the dirty count by removing uploaded blocks.
func (l *Layer) doWriteBlock(ctx context.Context, blockIdx BlockIdx, fn func() error) error {
	// Wait while at capacity, unless this block is already dirty.
	l.mu.Lock()
	start := time.Time{}
	for len(l.dirty) >= l.vm.MaxDirtyBlocks {
		if _, alreadyDirty := l.dirty[blockIdx]; alreadyDirty {
			break
		}
		if start.IsZero() {
			l.mu.Unlock() // XXX: I don't understand the point of unlocking and re-locking this mutex
			metrics.BackpressureWaits.Inc()
			l.pokeFlush()
			start = time.Now()
			// Wake us if context is cancelled while we're in Wait.
			stop := context.AfterFunc(ctx, func() { l.dirtyCond.Broadcast() })
			defer stop()
			l.mu.Lock()
		}
		if ctx.Err() != nil {
			l.mu.Unlock()
			return ctx.Err()
		}
		l.dirtyCond.Wait()
	}
	l.mu.Unlock()
	if !start.IsZero() {
		metrics.BackpressureWaitDuration.Observe(time.Since(start).Seconds())
	}

	// Perform the actual write.
	if err := fn(); err != nil {
		return err
	}

	// Mark dirty (increment generation). // XXX: what is generation???
	l.mu.Lock()
	l.dirty[blockIdx] = struct{}{}
	n := len(l.dirty)
	metrics.DirtyBlocks.Set(float64(n))
	metrics.OpenBlocks.Set(float64(len(l.openBlocks))) // XXX: is this really the right place to update this metric?
	l.mu.Unlock()

	// Poke early flush at 50% capacity.
	if n >= l.vm.MaxDirtyBlocks/2 {
		l.pokeFlush()
	}
	return nil
}

// pokeFlush sends a non-blocking signal to trigger an early flush.
func (l *Layer) pokeFlush() {
	select {
	case l.flushTrigger <- struct{}{}:
	default:
	}
}

// Write writes data starting at offset. Rejects writes to frozen or
// closed layers.
func (l *Layer) Write(ctx context.Context, data []byte, offset uint64) error {
	if err := l.checkWritable(); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	t := metrics.NewTimer(metrics.WriteDuration)
	defer t.ObserveDuration()
	metrics.WriteBytes.Add(float64(len(data))) // XXX: we should probably only do this on each block write

	for s := range blockSpans(l.vm.blockSize, offset, uint64(len(data))) {
		if err := l.doWriteBlock(ctx, s.BlockIdx, func() error {
			f, err := l.openBlock(ctx, s.BlockIdx)
			if err != nil {
				return fmt.Errorf("open block %d: %w", s.BlockIdx, err)
			}
			if _, err := f.WriteAt(data[s.Pos:s.Pos+int(s.Length)], int64(s.OffInBlock)); err != nil {
				return fmt.Errorf("pwrite block %d: %w", s.BlockIdx, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// PunchHole zeroes the range [offset, offset+length) without writing
// actual zero bytes. Only block-aligned, full-block ranges are punched;
// partial blocks at the edges are zero-filled via a normal write.
// The observable effect is identical to writing zeros.
func (l *Layer) PunchHole(ctx context.Context, offset, length uint64) error {
	if err := l.checkWritable(); err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	metrics.PunchHoleOps.Inc()
	metrics.PunchHoleBytes.Add(float64(length))
	end := offset + length

	// Leading partial block: zero-fill via Write.
	if head := offset % l.vm.blockSize; head != 0 {
		n := l.vm.blockSize - head
		if offset+n > end {
			n = end - offset
		}
		if err := l.Write(ctx, make([]byte, n), offset); err != nil {
			return err
		}
		offset += n
	}

	// Full aligned blocks: punch directly as tombstones.
	for offset+l.vm.blockSize <= end {
		blockIdx := BlockIdx(offset / l.vm.blockSize)
		metrics.PunchHoleBlocks.Inc()

		if err := l.doWriteBlock(ctx, blockIdx, func() error {
			f, err := l.openBlock(ctx, blockIdx)
			if err != nil {
				return fmt.Errorf("open block for punch %d: %w", blockIdx, err)
			}
			if err := f.Truncate(0); err != nil {
				return fmt.Errorf("truncate block %d: %w", blockIdx, err)
			}
			return nil
		}); err != nil {
			return err
		}
		offset += l.vm.blockSize
	}

	// Trailing partial block: zero-fill via Write.
	if offset < end {
		if err := l.Write(ctx, make([]byte, end-offset), offset); err != nil {
			return err
		}
	}

	return nil
}

// CopyFrom performs a copy-on-write transfer of data from src into this layer.
// For block-aligned, full-block regions where the owning layer is frozen,
// this is metadata-only: we add a reference in refBlockIndex. Blocks owned
// by an unfrozen layer (dirty/in-flight) must be data-copied. Partial blocks
// at the edges are always data-copied.
func (dst *Layer) CopyFrom(ctx context.Context, src *Layer, srcOff, dstOff, length uint64) (uint64, error) {
	if err := dst.checkWritable(); err != nil {
		return 0, err
	}

	copied := uint64(0)
	for copied < length {
		srcPos := srcOff + copied
		dstPos := dstOff + copied

		srcBlockIdx := BlockIdx(srcPos / dst.vm.blockSize)
		srcOffInBlock := srcPos % dst.vm.blockSize

		dstBlockIdx := BlockIdx(dstPos / dst.vm.blockSize)
		dstOffInBlock := dstPos % dst.vm.blockSize

		// Chunk size is limited by both block boundaries.
		n := min(dst.vm.blockSize-srcOffInBlock, dst.vm.blockSize-dstOffInBlock)
		n = min(n, length-copied)

		// CoW fast path: full block, aligned, same block index, owner is frozen.
		// refBlockIndex maps blockIdx → layerID so block indices must match.
		// We only reference blocks from frozen layers — refs from refBlockIndex
		// are frozen by construction; blocks from src itself require src.frozen.
		if srcOffInBlock == 0 && dstOffInBlock == 0 && n == dst.vm.blockSize && srcBlockIdx == dstBlockIdx {
			ownerID := src.FindBlockOwner(srcBlockIdx)
			ownerFrozen := ownerID != "" && (ownerID != src.id || src.frozen.Load())
			if ownerFrozen {
				dst.mu.Lock()
				if f := dst.openBlocks[dstBlockIdx]; f != nil {
					if err := f.Close(); err != nil {
						slog.Warn("close cached block file", "block", dstBlockIdx, "error", err)
					}
					delete(dst.openBlocks, dstBlockIdx)
				}
				delete(dst.dirty, dstBlockIdx)
				dst.refBlockIndex[dstBlockIdx] = ownerID
				dst.mu.Unlock()
				dst.dirtyCond.Broadcast()
				copied += n
				continue
			}
			if ownerID == "" {
				// Unwritten block — zero out the destination.
				if err := dst.PunchHole(ctx, dstOff+copied, n); err != nil {
					return copied, fmt.Errorf("punch hole for unwritten block %d: %w", dstBlockIdx, err)
				}
				copied += n
				continue
			}
			// Owner is src itself and src is not frozen — fall through to data copy.
		}

		// Slow path: partial block or unfrozen source block — actual data copy.
		buf := make([]byte, n)
		if err := src.readBlockAt(ctx, srcBlockIdx, buf, srcOffInBlock); err != nil {
			return copied, fmt.Errorf("read block %d from source: %w", srcBlockIdx, err)
		}

		if err := dst.doWriteBlock(ctx, dstBlockIdx, func() error {
			f, err := dst.openBlock(ctx, dstBlockIdx)
			if err != nil {
				return fmt.Errorf("open block %d in dest: %w", dstBlockIdx, err)
			}
			if _, err := f.WriteAt(buf, int64(dstOffInBlock)); err != nil {
				return fmt.Errorf("write block %d in dest: %w", dstBlockIdx, err)
			}
			return nil
		}); err != nil {
			return copied, err
		}

		copied += n
	}
	return copied, nil
}

// FindBlockOwner returns the layer ID that owns the given block, or "" if absent.
// Checks open/dirty blocks, local S3 blocks, then the ref block index.
func (l *Layer) FindBlockOwner(blockIdx BlockIdx) string {
	l.mu.Lock()
	_, hasOpen := l.openBlocks[blockIdx]
	_, local := l.localIndex[blockIdx]
	l.mu.Unlock()
	if hasOpen || local {
		return l.id
	}
	return l.refBlockIndex[blockIdx]
}
