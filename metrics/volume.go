package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	SnapshotDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "volume",
		Name:      "snapshot_duration_seconds",
		Help:      "Snapshot operation duration.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	CloneDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "volume",
		Name:      "clone_duration_seconds",
		Help:      "Clone operation duration.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	OpenVolumes = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "volume",
		Name:      "open_volumes",
		Help:      "Current number of open volumes.",
	}))
)
