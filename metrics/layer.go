package metrics

import "github.com/prometheus/client_golang/prometheus"

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
)
