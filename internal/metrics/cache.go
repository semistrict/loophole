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
		Help:      "Pages read by source: dirty_pages, pending_dirty_batch, cache, l0, l1, l2, zero.",
	}, []string{"source"}))

	// --- Page cache daemon metrics ---

	// Client-side: local lookup cache hits (zero IPC).
	CacheLocalHits = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "local_hits_total",
		Help:      "Lookups served from the client-local slot cache (zero IPC).",
	}))

	// Client-side: lookups that required IPC to the daemon.
	CacheIPCLookups = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "ipc_lookups_total",
		Help:      "Lookups that required IPC to the cache daemon.",
	}))

	// Client-side: lookups skipped because the client was draining.
	CacheDrainSkips = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "drain_skips_total",
		Help:      "Lookups skipped because the client was draining.",
	}))

	// Daemon-side: total lookup requests received.
	CacheDaemonLookups = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_lookups_total",
		Help:      "Lookup requests received by the cache daemon.",
	}))

	// Daemon-side: lookup hits (key found in index).
	CacheDaemonHits = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_hits_total",
		Help:      "Lookup requests that found the key in the daemon index.",
	}))

	// Daemon-side: populate requests (page insertions).
	CacheDaemonPopulates = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_populates_total",
		Help:      "Populate (insert/update) requests received by the cache daemon.",
	}))

	// Daemon-side: populate failures (over budget, no slots).
	CacheDaemonPopulateErrors = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_populate_errors_total",
		Help:      "Populate requests that failed (over budget or no free slots).",
	}))

	// Daemon-side: delete requests.
	CacheDaemonDeletes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_deletes_total",
		Help:      "Delete requests received by the cache daemon.",
	}))

	// Daemon-side: pages evicted by LRU.
	CacheDaemonEvictions = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_evictions_total",
		Help:      "Pages evicted by the daemon's LRU eviction.",
	}))

	// Daemon-side: drain cycles completed.
	CacheDaemonDrains = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_drains_total",
		Help:      "Drain/evict/resume cycles completed by the daemon.",
	}))

	// Daemon-side: time to quiesce all clients during drain.
	CacheDaemonDrainQuiesceSeconds = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_drain_quiesce_seconds",
		Help:      "Time from drain start to all clients drained.",
		Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
	}))

	// Daemon-side: current number of pages in the cache.
	CacheDaemonPages = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_pages",
		Help:      "Current number of pages in the cache daemon index.",
	}))

	// Daemon-side: current used bytes in the arena.
	CacheDaemonUsedBytes = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_used_bytes",
		Help:      "Current bytes used in the cache daemon arena.",
	}))

	// Daemon-side: current budget in bytes.
	CacheDaemonBudgetBytes = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_budget_bytes",
		Help:      "Current cache budget in bytes.",
	}))

	// Daemon-side: connected clients.
	CacheDaemonClients = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "cached",
		Name:      "daemon_clients",
		Help:      "Current number of connected clients.",
	}))
)
