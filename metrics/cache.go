package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CacheHits = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "Immutable cache hits.",
	}))

	CacheMisses = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cache",
		Name:      "misses_total",
		Help:      "Immutable cache misses (triggered S3 fetch).",
	}))

	CacheFetchDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "cache",
		Name:      "fetch_duration_seconds",
		Help:      "Time to fetch a block from S3 into cache.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~40s
	}))
)
