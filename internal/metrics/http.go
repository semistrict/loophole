package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequests = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total HTTP API requests by method, path, and status code.",
	}, []string{"method", "path", "code"}))

	HTTPDuration = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "HTTP API request latency by method and path.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
	}, []string{"method", "path"}))
)
