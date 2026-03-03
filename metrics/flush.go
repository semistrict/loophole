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

	FlushTombstones = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "tombstones_total",
		Help:      "Zero-blocks flushed as tombstones (or deleted).",
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
)
