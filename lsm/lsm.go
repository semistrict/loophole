// Package lsm implements an LSM-inspired storage layer for loophole volumes.
//
// The block device is treated as a flat array of 4KB pages. Writes are
// appended to an in-memory layer and periodically flushed as batch objects
// to S3. Branching and snapshots are instant metadata operations.
//
// See docs/lsm-storage.md for the full design.
package lsm

import (
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	PageSize = 65536 // 64KB

	DefaultFlushThreshold  = 256 * 1024 * 1024 // 256MB
	DefaultMaxFrozenLayers = 2
	DefaultPageCacheSize   = 512 * 1024 * 1024      // 512MB
	DefaultVolumeSize      = 8 * 1024 * 1024 * 1024 // 8GB
	DefaultMaxLayerCache   = 64
	DefaultFlushInterval   = 30 * time.Second
)

// zeroPage is a shared read-only zero page returned for tombstones and
// never-written pages. Callers must not modify the returned slice.
var zeroPage [PageSize]byte

// zstdDecoderPool reuses zstd decoders across page reads to avoid the
// significant allocation cost of zstd.NewReader per decompression.
// WithDecoderConcurrency(1) makes DecodeAll run synchronously — no internal
// goroutine pool. This avoids synctest "receive on channel from outside bubble"
// panics and is actually faster for small (4KB page) decompressions.
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

// Config controls the LSM engine's resource limits and behavior.
type Config struct {
	// FlushThreshold is the memLayer size in bytes that triggers a freeze+flush.
	FlushThreshold int64

	// MaxFrozenLayers caps the number of frozen memLayers awaiting upload.
	// When full, writes block until a flush completes (write backpressure).
	MaxFrozenLayers int

	// PageCacheBytes is the size of the LRU page cache on local disk.
	PageCacheBytes int64

	// MaxLayerCacheEntries caps the number of parsed delta/image layers kept
	// in memory. When exceeded, random entries are evicted. 0 = default.
	MaxLayerCacheEntries int

	// FlushInterval is how often the background goroutine flushes dirty
	// memLayers to S3. 0 = default (30s). Negative = disabled.
	FlushInterval time.Duration
}

func (c *Config) setDefaults() {
	if c.FlushThreshold == 0 {
		c.FlushThreshold = DefaultFlushThreshold
	}
	if c.MaxFrozenLayers == 0 {
		c.MaxFrozenLayers = DefaultMaxFrozenLayers
	}
	if c.PageCacheBytes == 0 {
		c.PageCacheBytes = DefaultPageCacheSize
	}
	if c.MaxLayerCacheEntries == 0 {
		c.MaxLayerCacheEntries = DefaultMaxLayerCache
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = DefaultFlushInterval
	}
}
