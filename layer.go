package loophole

import (
	"context"
	"fmt"
	"iter"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

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

// Layer is a single S3 prefix containing blocks and a state.json.
type Layer struct {
	vm    *VolumeManager
	base  ObjectStore // scoped to this layer (e.g. "layers/<id>/")
	cache *BlockCache
	id    string

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
	openBlocks map[BlockIdx]*os.File

	frozen atomic.Bool
	closed atomic.Bool

	mu           sync.Mutex         // protects dirty, localIndex, openBlocks
	openBlockSF  singleflight.Group // deduplicates concurrent openBlock calls
	stopFlush    context.CancelFunc
	flushStopped chan struct{} // closed when background flush exits
}

func (l *Layer) ID() string   { return l.id }
func (l *Layer) Frozen() bool { return l.frozen.Load() }

// checkWritable returns an error if the layer is frozen, and panics if
// the layer is closed (a programming error — like Volume.mustBeOpen).
func (l *Layer) checkWritable() error {
	if l.closed.Load() {
		panic(fmt.Sprintf("layer %q used after Close()", l.id))
	}
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
	Pos        int // byte offset into the caller's buffer
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
func (l *Layer) Read(ctx context.Context, offset uint64, buf []byte) (int, error) {
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

	// 2. This layer's S3 blocks — fetch into immutable cache.
	l.mu.Lock()
	_, inLocal := l.localIndex[blockIdx]
	l.mu.Unlock()

	if inLocal || l.frozen.Load() {
		metrics.ReadSource.WithLabelValues("local").Inc()
		return l.readCached(ctx, l.id, blockIdx, buf, offInBlock)
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
	defer f.Close()
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

		ownerID := ""
		if inLocal {
			ownerID = l.id
		} else {
			ownerID = l.refBlockIndex[blockIdx]
		}

		var src *os.File
		if ownerID != "" {
			var tombstone bool
			var err error
			src, tombstone, err = l.cache.GetOrFetch(ctx, ownerID, blockIdx)
			if err != nil {
				return nil, err
			}
			if tombstone {
				src = nil // zeros
			} else {
				defer src.Close()
			}
		}

		f, err := l.cache.OpenMutable(l.id, blockIdx, src)
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

// Write writes data starting at offset. Rejects writes to frozen or
// closed layers.
func (l *Layer) Write(ctx context.Context, offset uint64, data []byte) error {
	if err := l.checkWritable(); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	t := metrics.NewTimer(metrics.WriteDuration)
	defer t.ObserveDuration()
	metrics.WriteBytes.Add(float64(len(data)))

	for s := range blockSpans(l.vm.blockSize, offset, uint64(len(data))) {
		f, err := l.openBlock(ctx, s.BlockIdx)
		if err != nil {
			return fmt.Errorf("open block %d: %w", s.BlockIdx, err)
		}
		if _, err := f.WriteAt(data[s.Pos:s.Pos+int(s.Length)], int64(s.OffInBlock)); err != nil {
			return fmt.Errorf("pwrite block %d: %w", s.BlockIdx, err)
		}

		l.mu.Lock()
		l.dirty[s.BlockIdx] = struct{}{}
		metrics.DirtyBlocks.Set(float64(len(l.dirty)))
		metrics.OpenBlocks.Set(float64(len(l.openBlocks)))
		l.mu.Unlock()
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
		if err := l.Write(ctx, offset, make([]byte, n)); err != nil {
			return err
		}
		offset += n
	}

	// Full aligned blocks: punch directly as tombstones.
	for offset+l.vm.blockSize <= end {
		blockIdx := BlockIdx(offset / l.vm.blockSize)
		metrics.PunchHoleBlocks.Inc()

		// Open the block (creating if needed) so flush always has a handle.
		f, err := l.openBlock(ctx, blockIdx)
		if err != nil {
			return fmt.Errorf("open block for punch %d: %w", blockIdx, err)
		}
		if err := f.Truncate(0); err != nil {
			return fmt.Errorf("truncate block %d: %w", blockIdx, err)
		}

		l.mu.Lock()
		l.dirty[blockIdx] = struct{}{}
		l.mu.Unlock()
		offset += l.vm.blockSize
	}

	// Trailing partial block: zero-fill via Write.
	if offset < end {
		if err := l.Write(ctx, offset, make([]byte, end-offset)); err != nil {
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
					f.Close()
					delete(dst.openBlocks, dstBlockIdx)
				}
				delete(dst.dirty, dstBlockIdx)
				dst.refBlockIndex[dstBlockIdx] = ownerID
				dst.mu.Unlock()
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

		f, err := dst.openBlock(ctx, dstBlockIdx)
		if err != nil {
			return copied, fmt.Errorf("open block %d in dest: %w", dstBlockIdx, err)
		}
		if _, err := f.WriteAt(buf, int64(dstOffInBlock)); err != nil {
			return copied, fmt.Errorf("write block %d in dest: %w", dstBlockIdx, err)
		}

		dst.mu.Lock()
		dst.dirty[dstBlockIdx] = struct{}{}
		dst.mu.Unlock()

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
