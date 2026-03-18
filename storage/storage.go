// Package storage implements the tiered storage layer for loophole volumes.
//
// Data is organized into two levels:
//   - L1: sparse 4MB blocks (only changed pages within a 4MB region)
//   - L2: dense 4MB blocks (full snapshot of a 4MB region)
//
// Frozen memtable pages are flushed directly to L1/L2 blocks.
// Snapshots freeze the current layer and move the volume to a new child.
// Each layer's index.json is self-contained — it references all data files
// it needs, including inherited ones from ancestors.
package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	PageSize = 4096 // 4KB

	// BlockPages is the number of 4KB pages in one 4MB block (L1/L2).
	BlockPages = 1024

	// BlockSize is the uncompressed size of a full block.
	BlockSize = BlockPages * PageSize // 4MB

	DefaultFlushThreshold = 512 * 1024 * 1024      // 512MB
	DefaultVolumeSize     = 8 * 1024 * 1024 * 1024 // 8GB
	DefaultFlushInterval  = 30 * time.Second

	// L1PromoteThreshold is the fraction of pages in an L1 block that
	// triggers promotion to L2. 25% = 256 out of 1024 pages.
	L1PromoteThreshold = BlockPages / 4

	// maxMemtableSlots caps the number of unique page slots in a memtable.
	maxMemtableSlots = 131072 // 512MB at 4KB/slot
)

// PageIdx is a page index into the virtual disk.
// Page 0 starts at byte offset 0, page 1 at byte offset 4096, etc.
type PageIdx uint64

func (p PageIdx) String() string { return fmt.Sprintf("page#%d", uint64(p)) }

// PageIdxOf returns the page index and intra-page byte offset for a byte offset.
func PageIdxOf(byteOffset uint64) (PageIdx, uint64) {
	return PageIdx(byteOffset / PageSize), byteOffset % PageSize
}

// ByteOffset returns the byte offset of this page's first byte.
func (p PageIdx) ByteOffset() uint64 { return uint64(p) * PageSize }

// Block returns the block index containing this page.
func (p PageIdx) Block() BlockIdx {
	return BlockIdx(uint64(p) / BlockPages)
}

// BlockIdx is a block index into the virtual disk.
// Block 0 covers pages 0..1023, block 1 covers pages 1024..2047, etc.
type BlockIdx uint64

func (b BlockIdx) String() string { return fmt.Sprintf("block#%d", uint64(b)) }

// PageIdx returns the absolute page index for a page at the given
// block-relative offset within this block.
func (b BlockIdx) PageIdx(pageOffset uint16) PageIdx {
	return PageIdx(uint64(b)*BlockPages + uint64(pageOffset))
}

// zeroPage is a shared read-only zero page returned for tombstones and
// never-written pages. Callers must not modify the returned slice.
var zeroPage [PageSize]byte

// zstd encoder/decoder pools.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic("zstd.NewReader: " + err.Error())
		}
		return d
	},
}

func getZstdDecoder() *zstd.Decoder {
	return zstdDecoderPool.Get().(*zstd.Decoder)
}

func putZstdDecoder(d *zstd.Decoder) {
	_ = d.Reset(nil)
	zstdDecoderPool.Put(d)
}

var zstdEncoderPool = sync.Pool{
	New: func() any {
		e, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			panic("zstd.NewWriter: " + err.Error())
		}
		return e
	},
}

func getZstdEncoder() *zstd.Encoder {
	return zstdEncoderPool.Get().(*zstd.Encoder)
}

func putZstdEncoder(e *zstd.Encoder) {
	e.Reset(nil)
	zstdEncoderPool.Put(e)
}

// Config controls the storage engine's resource limits and behavior.
type Config struct {
	// FlushThreshold is the memtable size in bytes that triggers a freeze+flush.
	FlushThreshold int64

	// FlushInterval is how often the background goroutine flushes dirty
	// memtables to S3. 0 = default (30s). Negative = disabled.
	FlushInterval time.Duration

	// MaxCacheEntries caps the number of in-memory parsed block entries.
	// 0 = default (256).
	MaxCacheEntries int

	// MaxMemtableSlots caps the number of unique page slots in a memtable.
	// 0 = default (65536). Only useful for tests.
	MaxMemtableSlots int

	// DisableCompression stores pages uncompressed in blocks. This is used
	// in tests to avoid zstd's internal goroutine channels which are
	// incompatible with synctest.
	DisableCompression bool
}

// testOverrides is set by test code to apply defaults for all Configs.
var testOverrides func(*Config)

func (c *Config) setDefaults() {
	if c.FlushThreshold == 0 {
		c.FlushThreshold = DefaultFlushThreshold
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = DefaultFlushInterval
	}
	if testOverrides != nil {
		testOverrides(c)
	}
}

// maxMemtablePages returns the number of page slots for a new memtable.
func (c *Config) maxMemtablePages() int {
	cap := maxMemtableSlots
	if c.MaxMemtableSlots > 0 {
		cap = c.MaxMemtableSlots
	}
	n := int(c.FlushThreshold / PageSize)
	if n < 1 {
		n = 1
	}
	if n > cap {
		n = cap
	}
	return n
}
