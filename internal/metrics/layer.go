package metrics

import "github.com/prometheus/client_golang/prometheus"

// I/O size buckets: 512B, 1K, 2K, 4K, 8K, 16K, 32K, 64K, 128K, 256K, 512K, 1M, 4M.
var ioSizeBuckets = []float64{
	512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
	128 << 10, 256 << 10, 512 << 10, 1 << 20, 4 << 20,
}

var (
	ReadBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_bytes_total",
		Help:      "Total bytes read from layers.",
	}))

	OpenBlockDedup = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "open_block_dedup_total",
		Help:      "openBlock calls that shared a singleflight result (avoided duplicate S3 fetch).",
	}))

	ReadSize = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "storage",
		Name:      "read_size_bytes",
		Help:      "Distribution of read request sizes in bytes.",
		Buckets:   ioSizeBuckets,
	}))

	WriteSize = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "storage",
		Name:      "write_size_bytes",
		Help:      "Distribution of write request sizes in bytes.",
		Buckets:   ioSizeBuckets,
	}))

	PunchHoleSize = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "storage",
		Name:      "punch_hole_size_bytes",
		Help:      "Distribution of punch hole (discard/trim) request sizes in bytes.",
		Buckets:   ioSizeBuckets,
	}))
)
