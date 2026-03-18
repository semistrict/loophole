package e2e

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/metrics"
)

var (
	testDir  env.Dir
	testInst env.ResolvedProfile
	testBin  string
)

func debugCountersEnabled() bool {
	return os.Getenv("LOOPHOLE_DEBUG_COUNTERS") != ""
}

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

	tmpDir, err := os.MkdirTemp("", "loophole-e2e-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	testDir = env.Dir(tmpDir)
	testInst = env.ResolvedProfile{
		ProfileName: "test",
		Bucket:      envOrDefault("BUCKET", "testbucket"),
		Prefix:      fmt.Sprintf("test-%s", uuid.NewString()[:8]),
		Endpoint:    defaultEndpoint(),
		AccessKey:   envOrDefault("AWS_ACCESS_KEY_ID", "rustfsadmin"),
		SecretKey:   envOrDefault("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
		Region:      envOrDefault("AWS_REGION", "us-east-1"),
		LogLevel:    os.Getenv("LOG_LEVEL"),
	}

	if err := os.WriteFile(filepath.Join(string(testDir), "config.toml"), []byte(fmt.Sprintf(`default_profile = "test"

[profiles.test]
endpoint = %q
bucket = %q
prefix = %q
access_key = %q
secret_key = %q
region = %q
log_level = %q
`, testInst.Endpoint, testInst.Bucket, testInst.Prefix, testInst.AccessKey, testInst.SecretKey, testInst.Region, testInst.LogLevel)), 0o644); err != nil {
		log.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	testBin, err = resolveTestBinary(cwd)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	os.Exit(code)
}

func resolveTestBinary(cwd string) (string, error) {
	if bin := os.Getenv("LOOPHOLE_TEST_BIN"); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin, nil
		}
	}

	repoRoot := filepath.Clean(filepath.Join(cwd, ".."))
	if _, err := os.Stat(filepath.Join(repoRoot, "Makefile")); err == nil {
		if _, err := exec.LookPath("make"); err == nil {
			cmd := exec.Command("make", "loophole")
			cmd.Dir = repoRoot
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return "", fmt.Errorf("build loophole binary: %w", err)
			}
			return filepath.Join(repoRoot, "bin", fmt.Sprintf("loophole-%s-%s", runtime.GOOS, runtime.GOARCH)), nil
		}
	}

	loopholeBin, err := exec.LookPath("loophole")
	if err != nil {
		return "", fmt.Errorf("resolve loophole test binary: %w", err)
	}
	return loopholeBin, nil
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
	if !debugCountersEnabled() {
		return
	}
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
	if !debugCountersEnabled() {
		return
	}
	if _, loaded := trackMetricsOnce.LoadOrStore(t.Name(), true); loaded {
		return
	}
	before := captureMetrics()
	t.Cleanup(func() { dumpMetricsDelta(t, before) })
}
