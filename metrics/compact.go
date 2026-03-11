package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CompactDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "duration_seconds",
		Help:      "Total L0→L1 compaction cycle duration.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 14), // 100ms .. ~819s
	}))

	CompactL0InputPages = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "l0_input_pages_total",
		Help:      "Total L0 pages fed into compaction.",
	}))

	CompactBlocksWritten = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "blocks_written_total",
		Help:      "L1+L2 blocks written by compaction.",
	}))

	CompactBytesWritten = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "bytes_written_total",
		Help:      "Bytes uploaded to S3 by compaction (L1+L2 blocks).",
	}))

	CompactL2Promotions = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "l2_promotions_total",
		Help:      "L1 blocks promoted to L2 during compaction.",
	}))

	CompactBlockDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "block_duration_seconds",
		Help:      "Time to build+upload a single L1 block during compaction.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~20s
	}))

	CompactRunning = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "running",
		Help:      "1 if compaction is currently in progress, 0 otherwise.",
	}))

	CompactBlocksTotal = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "blocks_total_current",
		Help:      "Total blocks to process in the current compaction run.",
	}))

	CompactBlocksProcessed = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "compact",
		Name:      "blocks_processed_current",
		Help:      "Blocks processed so far in the current compaction run.",
	}))
)
