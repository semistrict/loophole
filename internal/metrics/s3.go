package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	S3Requests = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "requests_total",
		Help:      "Total S3 requests by operation.",
	}, []string{"op"}))

	S3Duration = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "request_duration_seconds",
		Help:      "S3 request latency by operation.",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms .. ~40s
	}, []string{"op"}))

	S3Errors = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "errors_total",
		Help:      "Failed S3 requests by operation.",
	}, []string{"op"}))

	S3TransferBytes = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "transfer_bytes_total",
		Help:      "Total bytes transferred by operation and direction.",
	}, []string{"op", "direction"})) // direction: "tx" (upload) or "rx" (download)

	S3ObjectSize = reg(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "object_size_bytes",
		Help:      "Size of S3 objects transferred by operation.",
		Buckets:   prometheus.ExponentialBuckets(64, 4, 12), // 64B .. ~268MB
	}, []string{"op"}))

	InflightUploads = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "inflight_uploads",
		Help:      "Current number of in-flight S3 upload operations.",
	}))

	InflightDownloads = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "s3",
		Name:      "inflight_downloads",
		Help:      "Current number of in-flight S3 download operations.",
	}))
)

// S3Op starts timing an S3 operation. Call the returned function when
// done, passing the error (nil on success).
//
//	done := metrics.S3Op("get")
//	result, err := client.GetObject(...)
//	done(err)
func S3Op(op string) func(error) {
	S3Requests.WithLabelValues(op).Inc()
	t := NewTimer(S3Duration.WithLabelValues(op))
	return func(err error) {
		t.ObserveDuration()
		if err != nil {
			S3Errors.WithLabelValues(op).Inc()
		}
	}
}

// S3Transfer records bytes transferred for an S3 operation.
// direction is "tx" for uploads, "rx" for downloads.
func S3Transfer(op, direction string, bytes int64) {
	S3TransferBytes.WithLabelValues(op, direction).Add(float64(bytes))
	S3ObjectSize.WithLabelValues(op).Observe(float64(bytes))
}
