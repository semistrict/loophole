//go:build linux

package e2e

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/semistrict/loophole/metrics"
)

func TestMain(m *testing.M) {
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		var level slog.Level
		switch strings.ToLower(lvl) {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go http.ListenAndServe(":9090", mux)
	fmt.Println("metrics available on :9090/metrics")
	os.Exit(m.Run())
}

// metricsSnapshot captures counter and gauge values keyed by "name{labels}".
type metricsSnapshot map[string]float64

func captureMetrics() metricsSnapshot {
	snap := make(metricsSnapshot)
	families, _ := metrics.Registry.Gather()
	for _, fam := range families {
		name := fam.GetName()
		// Skip go_* and process_* runtime metrics.
		if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := labelString(m.GetLabel())
			key := name + labels
			switch fam.GetType() {
			case dto.MetricType_COUNTER:
				snap[key] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				snap[key] = m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				snap[key+"_count"] = float64(m.GetHistogram().GetSampleCount())
				snap[key+"_sum"] = m.GetHistogram().GetSampleSum()
			}
		}
	}
	return snap
}

func labelString(labels []*dto.LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, len(labels))
	for i, lp := range labels {
		parts[i] = lp.GetName() + "=" + lp.GetValue()
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func dumpMetricsDelta(t *testing.T, before metricsSnapshot) {
	after := captureMetrics()

	type entry struct {
		key   string
		delta float64
	}
	var entries []entry
	for k, v := range after {
		delta := v - before[k]
		if delta != 0 {
			entries = append(entries, entry{k, delta})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	if len(entries) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "--- metrics for %s ---\n", t.Name())
	for _, e := range entries {
		if e.delta == float64(int64(e.delta)) {
			fmt.Fprintf(&b, "  %-60s %d\n", e.key, int64(e.delta))
		} else {
			fmt.Fprintf(&b, "  %-60s %.3f\n", e.key, e.delta)
		}
	}
	fmt.Print(b.String())
}

// trackMetrics snapshots counters at call time and registers a Cleanup
// that prints the delta when the test finishes. Safe to call multiple times
// per test (only the first call installs the hook).
var trackMetricsOnce sync.Map // t.Name() → bool

func trackMetrics(t *testing.T) {
	t.Helper()
	if _, loaded := trackMetricsOnce.LoadOrStore(t.Name(), true); loaded {
		return
	}
	before := captureMetrics()
	t.Cleanup(func() { dumpMetricsDelta(t, before) })
}
