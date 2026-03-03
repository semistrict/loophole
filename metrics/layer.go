package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ReadBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_bytes_total",
		Help:      "Total bytes read from layers.",
	}))

	ReadDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_duration_seconds",
		Help:      "Per-call layer read latency.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 4, 12), // 10us .. ~170s
	}))

	ReadSource = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_blocks_total",
		Help:      "Block reads by source (open_block, local, ref, zero).",
	}, []string{"source"}))

	WriteBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "write_bytes_total",
		Help:      "Total bytes written to layers.",
	}))

	WriteDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "write_duration_seconds",
		Help:      "Per-call layer write latency.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 4, 12),
	}))

	DirtyBlocks = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "dirty_blocks",
		Help:      "Current number of dirty (unuploaded) blocks.",
	}))

	OpenBlocks = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "open_blocks",
		Help:      "Current number of open mutable block file handles.",
	}))

	LayerLoadDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "load_duration_seconds",
		Help:      "Time to load a layer (state.json read + S3 block list).",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	PunchHoleOps = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_ops_total",
		Help:      "Total punch hole (discard/fallocate) operations.",
	}))

	PunchHoleBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_bytes_total",
		Help:      "Total bytes zeroed via punch hole.",
	}))

	PunchHoleBlocks = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_blocks_total",
		Help:      "Full blocks punched as tombstones (not counting partial edge writes).",
	}))

	OpenBlockDedup = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "open_block_dedup_total",
		Help:      "openBlock calls that shared a singleflight result (avoided duplicate S3 fetch).",
	}))
)
