package metrics

import (
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	FuseOps = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "fuse",
		Name:      "ops_total",
		Help:      "Total FUSE operations by type and status.",
	}, []string{"op", "status"}))

	FuseDuration = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "fuse",
		Name:      "op_duration_seconds",
		Help:      "FUSE operation latency by type.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 4, 12), // 10us .. ~170s
	}, []string{"op"}))

	FuseBytes = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "fuse",
		Name:      "bytes_total",
		Help:      "Total bytes transferred by FUSE read/write operations.",
	}, []string{"op"}))

	FuseInflight = reg(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "fuse",
		Name:      "inflight",
		Help:      "Currently in-flight FUSE operations by type.",
	}, []string{"op"}))
)

// FuseOp records a FUSE operation. Call the returned function when done.
func FuseOp(op string) func(syscall.Errno) {
	FuseInflight.WithLabelValues(op).Inc()
	t := NewTimer(FuseDuration.WithLabelValues(op))
	return func(errno syscall.Errno) {
		FuseInflight.WithLabelValues(op).Dec()
		t.ObserveDuration()
		status := "ok"
		if errno != 0 {
			status = "error"
		}
		FuseOps.WithLabelValues(op, status).Inc()
	}
}
