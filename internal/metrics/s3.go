package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	blobRequests = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "requests_total",
		Help:      "Total blob store requests by operation.",
	}, []string{"op"}))

	blobDuration = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "request_duration_seconds",
		Help:      "Blob store request latency by operation.",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms .. ~40s
	}, []string{"op"}))

	blobErrors = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "errors_total",
		Help:      "Failed blob store requests by operation.",
	}, []string{"op"}))

	blobTransferBytes = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "transfer_bytes_total",
		Help:      "Total bytes transferred by operation and direction.",
	}, []string{"op", "direction"})) // direction: "tx" (upload) or "rx" (download)

	blobObjectSize = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "object_size_bytes",
		Help:      "Size of objects transferred by operation.",
		Buckets:   prometheus.ExponentialBuckets(64, 4, 12), // 64B .. ~268MB
	}, []string{"op"}))

	InflightUploads = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "inflight_uploads",
		Help:      "Current number of in-flight upload operations.",
	}))

	InflightDownloads = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "blob",
		Name:      "inflight_downloads",
		Help:      "Current number of in-flight download operations.",
	}))
)

// blobOpMetrics holds pre-curried metrics for a single operation type.
type blobOpMetrics struct {
	requests   prometheus.Counter
	duration   prometheus.Observer
	errors     prometheus.Counter
	transferTx prometheus.Counter
	transferRx prometheus.Counter
	objectSize prometheus.Observer
}

var blobOps map[string]*blobOpMetrics

func init() {
	ops := []string{"get", "put", "put_cas", "put_if_not_exists", "delete", "list", "head", "set_meta"}
	blobOps = make(map[string]*blobOpMetrics, len(ops))
	for _, op := range ops {
		blobOps[op] = &blobOpMetrics{
			requests:   blobRequests.WithLabelValues(op),
			duration:   blobDuration.WithLabelValues(op),
			errors:     blobErrors.WithLabelValues(op),
			transferTx: blobTransferBytes.WithLabelValues(op, "tx"),
			transferRx: blobTransferBytes.WithLabelValues(op, "rx"),
			objectSize: blobObjectSize.WithLabelValues(op),
		}
	}
}

// BlobOp starts timing a blob store operation. Call the returned
// function when done, passing the error (nil on success).
//
//	done := metrics.BlobOp("get")
//	result, err := driver.Get(...)
//	done(err)
func BlobOp(op string) func(error) {
	m := blobOps[op]
	m.requests.Inc()
	t := NewTimer(m.duration)
	return func(err error) {
		t.ObserveDuration()
		if err != nil {
			m.errors.Inc()
		}
	}
}

// BlobTransfer records bytes transferred for a blob store operation.
// direction is "tx" for uploads, "rx" for downloads.
func BlobTransfer(op, direction string, n int64) {
	m := blobOps[op]
	if direction == "tx" {
		m.transferTx.Add(float64(n))
	} else {
		m.transferRx.Add(float64(n))
	}
	m.objectSize.Observe(float64(n))
}
