// Package metrics provides Prometheus metrics for loophole.
//
// All metrics are registered in a dedicated registry (not the global
// default) so they don't collide with anything else.  Call ListenAndServe
// to expose them on an HTTP /metrics endpoint.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Registry ---

var Registry = prometheus.NewRegistry()

func init() {
	// Include Go runtime metrics alongside ours.
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// ListenAndServe starts an HTTP server on addr serving /metrics.
// It logs a warning and returns if the listener fails.
// Handler returns an http.Handler that serves Prometheus metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

// --- Helpers ---

// Timer records the duration of an operation into a histogram when
// stopped. Create one with ObserveFunc or use the histogram's Timer
// method directly.
type Timer struct {
	start time.Time
	hist  prometheus.Observer
}

// NewTimer starts a timer that will record into hist.
func NewTimer(hist prometheus.Observer) Timer {
	return Timer{start: time.Now(), hist: hist}
}

// ObserveDuration records the elapsed time in seconds.
func (t Timer) ObserveDuration() {
	t.hist.Observe(time.Since(t.start).Seconds())
}

// reg is a shortcut for MustRegister + return.
func reg[T prometheus.Collector](c T) T {
	Registry.MustRegister(c)
	return c
}

// --- S3 metrics ---

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

// --- Store / Layer read-write metrics ---

var (
	ReadBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_bytes_total",
		Help:      "Total bytes read from layers.",
	}))

	ReadDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_duration_seconds",
		Help:      "Per-call layer read latency.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 4, 12), // 10us .. ~170s
	}))

	ReadSource = reg(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "read_blocks_total",
		Help:      "Block reads by source (open_block, local, ref, zero).",
	}, []string{"source"}))

	WriteBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "write_bytes_total",
		Help:      "Total bytes written to layers.",
	}))

	WriteDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "write_duration_seconds",
		Help:      "Per-call layer write latency.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 4, 12),
	}))

	DirtyBlocks = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "dirty_blocks",
		Help:      "Current number of dirty (unuploaded) blocks.",
	}))

	OpenBlocks = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "open_blocks",
		Help:      "Current number of open mutable block file handles.",
	}))
)

// --- Cache metrics ---

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

// --- Flush / Upload metrics ---

var (
	FlushDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "duration_seconds",
		Help:      "Total flush cycle duration.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))

	FlushBlocks = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "blocks_total",
		Help:      "Total blocks flushed (uploaded or tombstoned).",
	}))

	FlushBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "bytes_total",
		Help:      "Total bytes uploaded during flushes.",
	}))

	FlushErrors = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "errors_total",
		Help:      "Blocks that failed to upload and were re-dirtied.",
	}))

	FlushTombstones = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "flush",
		Name:      "tombstones_total",
		Help:      "Zero-blocks flushed as tombstones (or deleted).",
	}))
)

// --- Snapshot / Clone metrics ---

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
)

// --- Concurrency metrics ---

var (
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

// --- Layer load metrics ---

var (
	LayerLoadDuration = reg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "load_duration_seconds",
		Help:      "Time to load a layer (state.json read + S3 block list).",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}))
)

// --- CAS metrics ---

var (
	CASAttempts = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "attempts_total",
		Help:      "Total CAS (ModifyJSON) attempts including retries.",
	}))

	CASRetries = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "retries_total",
		Help:      "CAS retries due to ETag conflict.",
	}))

	CASFailures = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "failures_total",
		Help:      "CAS operations that exhausted all retry attempts.",
	}))
)

// --- Punch hole metrics ---

var (
	PunchHoleOps = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_ops_total",
		Help:      "Total punch hole (discard/fallocate) operations.",
	}))

	PunchHoleBytes = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_bytes_total",
		Help:      "Total bytes zeroed via punch hole.",
	}))

	PunchHoleBlocks = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "punch_hole_blocks_total",
		Help:      "Full blocks punched as tombstones (not counting partial edge writes).",
	}))
)

// --- Volume manager metrics ---

var (
	OpenVolumes = reg(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "loophole",
		Subsystem: "volume",
		Name:      "open_volumes",
		Help:      "Current number of open volumes.",
	}))
)

// --- Singleflight dedup metrics ---

var (
	OpenBlockDedup = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "layer",
		Name:      "open_block_dedup_total",
		Help:      "openBlock calls that shared a singleflight result (avoided duplicate S3 fetch).",
	}))
)

// --- Lease metrics ---

var (
	LeaseRenewals = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "lease",
		Name:      "renewals_total",
		Help:      "Lease renewal attempts.",
	}))

	LeaseErrors = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "lease",
		Name:      "errors_total",
		Help:      "Failed lease renewals.",
	}))
)
