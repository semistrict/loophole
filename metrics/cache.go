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

	// PageReadSource counts which layer satisfied a page read.
	PageReadSource = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cache",
		Name:      "page_read_source_total",
		Help:      "Pages read by source: memtable, frozen, cache, l0, l1, l2, zero.",
	}, []string{"source"}))
)
