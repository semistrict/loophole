package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	FlushDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "duration_seconds",
		Help:      "Total flush cycle duration.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	FlushBlocks = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "blocks_total",
		Help:      "Total blocks flushed (uploaded or tombstoned).",
	}))

	FlushBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "bytes_total",
		Help:      "Total bytes uploaded during flushes.",
	}))

	FlushErrors = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "errors_total",
		Help:      "Blocks that failed to upload and were re-dirtied.",
	}))

	BackpressureWaits = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "backpressure_waits_total",
		Help:      "Times a write blocked waiting for dirty block capacity.",
	}))

	BackpressureWaitDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "backpressure_wait_seconds",
		Help:      "How long writes were delayed by backpressure.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14),
	}))

	EarlyFlushes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "early_flushes_total",
		Help:      "Flush cycles triggered by dirty count crossing soft threshold.",
	}))

	FlushUploadDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "upload_duration_seconds",
		Help:      "Time to upload a single block to S3 (excludes index save).",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	FrozenTableCount = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "frozen_tables",
		Help:      "Current number of pending dirty batches awaiting flush.",
	}))

	DirtyPageBytes = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "dirty_page_bytes",
		Help:      "Current active dirty pages size in bytes.",
	}))

	FlushPages = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "pages_total",
		Help:      "Total pages flushed (data + tombstones).",
	}))

	FlushDirectBlocksWritten = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "direct_blocks_written_total",
		Help:      "Total L1/L2 blocks written by flush.",
	}))

	FlushDirectL2Promotions = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "direct_l2_promotions_total",
		Help:      "L2 promotions during flush.",
	}))
)
