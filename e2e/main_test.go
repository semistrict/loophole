package e2e

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
	"github.com/semistrict/loophole/metrics"
)

var (
	testDaemon *daemon.Daemon
	testClient *client.Client
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

	// Start a shared daemon for all e2e tests.
	tmpDir, err := os.MkdirTemp("", "loophole-e2e-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir := loophole.Dir(tmpDir)
	inst := loophole.Instance{
		ProfileName:   "test",
		Bucket:        envOrDefault("BUCKET", "testbucket"),
		Prefix:        fmt.Sprintf("test-%s", uuid.NewString()[:8]),
		Endpoint:      defaultEndpoint(),
		AccessKey:     envOrDefault("AWS_ACCESS_KEY_ID", "rustfsadmin"),
		SecretKey:     envOrDefault("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
		Region:        envOrDefault("AWS_REGION", ""),
		Mode:          loophole.DefaultMode(),
		DefaultFSType: loophole.DefaultFSType(),
		LogLevel:      os.Getenv("LOG_LEVEL"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	d, err := daemon.Start(ctx, inst, dir, daemon.Options{Foreground: true})
	if err != nil {
		log.Fatal(err)
	}
	testDaemon = d
	go d.Serve(ctx)

	testClient = client.NewFromSocket(dir.Socket("test"))

	code := m.Run()
	cancel()
	os.Exit(code)
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
