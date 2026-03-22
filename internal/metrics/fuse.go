package metrics

import (
	"github.com/hanwen/go-fuse/v2/fuse"
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

// FuseOpRecorder holds pre-resolved metric references for a single FUSE
// operation type, avoiding WithLabelValues map lookups on every call.
type FuseOpRecorder struct {
	inflight prometheus.Gauge
	duration prometheus.Observer
	ok       prometheus.Counter
	err      prometheus.Counter
	bytes    prometheus.Counter
}

// NewFuseOpRecorder creates a recorder with all metric references pre-resolved.
func NewFuseOpRecorder(op string) FuseOpRecorder {
	return FuseOpRecorder{
		inflight: FuseInflight.WithLabelValues(op),
		duration: FuseDuration.WithLabelValues(op),
		ok:       FuseOps.WithLabelValues(op, "ok"),
		err:      FuseOps.WithLabelValues(op, "error"),
		bytes:    FuseBytes.WithLabelValues(op),
	}
}

// Start begins recording a FUSE operation. Call the returned function when done.
func (r *FuseOpRecorder) Start() func(fuse.Status) {
	r.inflight.Inc()
	t := NewTimer(r.duration)
	return func(status fuse.Status) {
		r.inflight.Dec()
		t.ObserveDuration()
		if status != fuse.OK {
			r.err.Inc()
		} else {
			r.ok.Inc()
		}
	}
}

// AddBytes records bytes transferred.
func (r *FuseOpRecorder) AddBytes(n float64) {
	r.bytes.Add(n)
}

// FuseOp records a FUSE operation. Call the returned function when done.
func FuseOp(op string) func(fuse.Status) {
	FuseInflight.WithLabelValues(op).Inc()
	t := NewTimer(FuseDuration.WithLabelValues(op))
	return func(status fuse.Status) {
		FuseInflight.WithLabelValues(op).Dec()
		t.ObserveDuration()
		label := "ok"
		if status != fuse.OK {
			label = "error"
		}
		FuseOps.WithLabelValues(op, label).Inc()
	}
}
