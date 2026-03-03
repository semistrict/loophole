package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/fatih/color"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/spf13/cobra"
)

func statsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show daemon metrics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			raw, err := c.Metrics(cmd.Context())
			if err != nil {
				return err
			}
			families, err := parseMetrics(raw)
			if err != nil {
				return err
			}
			printStats(families)
			return nil
		},
	}
}

func parseMetrics(raw string) (map[string]*dto.MetricFamily, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	return parser.TextToMetricFamilies(strings.NewReader(raw))
}

// --- rendering ---

var (
	header = color.New(color.Bold, color.Underline)
	label  = color.New(color.FgCyan)
	warn   = color.New(color.FgYellow)
)

func printStats(fams map[string]*dto.MetricFamily) {
	printVolumes(fams)
	printFuse(fams)
	printLayer(fams)
	printCache(fams)
	printFlush(fams)
	printS3(fams)
}

// --- sections ---

func printVolumes(fams map[string]*dto.MetricFamily) {
	_, _ = header.Println("Volumes")
	printGauge(fams, "loophole_volume_open_volumes", "open")
	printHistSummary(fams, "loophole_volume_snapshot_duration_seconds", "snapshot")
	printHistSummary(fams, "loophole_volume_clone_duration_seconds", "clone")
	fmt.Println()
}

func printFuse(fams map[string]*dto.MetricFamily) {
	_, _ = header.Println("FUSE")

	// ops by type
	if fam := fams["loophole_fuse_ops_total"]; fam != nil {
		ops := aggregateByLabel(fam, "op")
		if len(ops) > 0 {
			_, _ = label.Print("  ops        ")
			parts := sortedEntries(ops)
			fmt.Println(strings.Join(parts, "  "))
		}
	}

	// bytes
	if fam := fams["loophole_fuse_bytes_total"]; fam != nil {
		for _, m := range fam.GetMetric() {
			op := labelVal(m, "op")
			v := m.GetCounter().GetValue()
			if v > 0 {
				_, _ = label.Printf("  %-10s ", op)
				fmt.Println(humanBytes(v))
			}
		}
	}

	// inflight
	if fam := fams["loophole_fuse_inflight"]; fam != nil {
		inflight := make(map[string]float64)
		for _, m := range fam.GetMetric() {
			v := m.GetGauge().GetValue()
			if v > 0 {
				inflight[labelVal(m, "op")] = v
			}
		}
		if len(inflight) > 0 {
			_, _ = label.Print("  inflight   ")
			parts := sortedEntries(inflight)
			fmt.Println(strings.Join(parts, "  "))
		}
	}

	fmt.Println()
}

func printLayer(fams map[string]*dto.MetricFamily) {
	_, _ = header.Println("Layer I/O")
	printCounterBytes(fams, "loophole_layer_read_bytes_total", "read")
	printCounterBytes(fams, "loophole_layer_write_bytes_total", "write")
	printGauge(fams, "loophole_layer_dirty_blocks", "dirty blocks")
	printGauge(fams, "loophole_layer_open_blocks", "open blocks")

	// read sources
	if fam := fams["loophole_layer_read_blocks_total"]; fam != nil {
		sources := make(map[string]float64)
		for _, m := range fam.GetMetric() {
			v := m.GetCounter().GetValue()
			if v > 0 {
				sources[labelVal(m, "source")] = v
			}
		}
		if len(sources) > 0 {
			_, _ = label.Print("  sources    ")
			parts := sortedEntries(sources)
			fmt.Println(strings.Join(parts, "  "))
		}
	}

	printCounter(fams, "loophole_layer_punch_hole_ops_total", "punch ops")
	printCounterBytes(fams, "loophole_layer_punch_hole_bytes_total", "punch bytes")
	printCounter(fams, "loophole_layer_open_block_dedup_total", "dedup hits")
	fmt.Println()
}

func printCache(fams map[string]*dto.MetricFamily) {
	hits := counterVal(fams, "loophole_cache_hits_total")
	misses := counterVal(fams, "loophole_cache_misses_total")
	if hits == 0 && misses == 0 {
		return
	}
	_, _ = header.Println("Block Cache")
	total := hits + misses
	rate := float64(0)
	if total > 0 {
		rate = hits / total * 100
	}
	_, _ = label.Print("  hit rate   ")
	fmt.Printf("%.1f%%  (%s hits, %s misses)\n", rate, humanCount(hits), humanCount(misses))
	printHistSummary(fams, "loophole_cache_fetch_duration_seconds", "fetch")
	fmt.Println()
}

func printFlush(fams map[string]*dto.MetricFamily) {
	blocks := counterVal(fams, "loophole_flush_blocks_total")
	if blocks == 0 {
		return
	}
	_, _ = header.Println("Flush")
	printCounter(fams, "loophole_flush_blocks_total", "blocks")
	printCounterBytes(fams, "loophole_flush_bytes_total", "bytes")
	printCounter(fams, "loophole_flush_errors_total", "errors")
	printCounter(fams, "loophole_flush_tombstones_total", "tombstones")
	printCounter(fams, "loophole_flush_backpressure_waits_total", "backpressure")
	printCounter(fams, "loophole_flush_early_flushes_total", "early flushes")
	printHistSummary(fams, "loophole_flush_duration_seconds", "duration")
	fmt.Println()
}

func printS3(fams map[string]*dto.MetricFamily) {
	total := counterVal(fams, "loophole_s3_requests_total")
	if total == 0 {
		return
	}
	_, _ = header.Println("S3")

	// requests by op
	if fam := fams["loophole_s3_requests_total"]; fam != nil {
		ops := make(map[string]float64)
		for _, m := range fam.GetMetric() {
			v := m.GetCounter().GetValue()
			if v > 0 {
				ops[labelVal(m, "op")] = v
			}
		}
		if len(ops) > 0 {
			_, _ = label.Print("  requests   ")
			parts := sortedEntries(ops)
			fmt.Println(strings.Join(parts, "  "))
		}
	}

	// errors by op
	if fam := fams["loophole_s3_errors_total"]; fam != nil {
		errs := make(map[string]float64)
		for _, m := range fam.GetMetric() {
			v := m.GetCounter().GetValue()
			if v > 0 {
				errs[labelVal(m, "op")] = v
			}
		}
		if len(errs) > 0 {
			_, _ = warn.Print("  errors     ")
			parts := sortedEntries(errs)
			fmt.Println(strings.Join(parts, "  "))
		}
	}

	// transfer bytes
	if fam := fams["loophole_s3_transfer_bytes_total"]; fam != nil {
		var tx, rx float64
		for _, m := range fam.GetMetric() {
			switch labelVal(m, "direction") {
			case "tx":
				tx += m.GetCounter().GetValue()
			case "rx":
				rx += m.GetCounter().GetValue()
			}
		}
		if tx > 0 || rx > 0 {
			_, _ = label.Print("  transfer   ")
			fmt.Printf("tx %s  rx %s\n", humanBytes(tx), humanBytes(rx))
		}
	}

	printGauge(fams, "loophole_s3_inflight_uploads", "uploads inflight")
	printGauge(fams, "loophole_s3_inflight_downloads", "downloads inflight")
	fmt.Println()
}

// --- helpers ---

func counterVal(fams map[string]*dto.MetricFamily, name string) float64 {
	fam := fams[name]
	if fam == nil {
		return 0
	}
	var total float64
	for _, m := range fam.GetMetric() {
		total += m.GetCounter().GetValue()
	}
	return total
}

func printCounter(fams map[string]*dto.MetricFamily, name, lbl string) {
	v := counterVal(fams, name)
	if v == 0 {
		return
	}
	_, _ = label.Printf("  %-10s ", lbl)
	fmt.Println(humanCount(v))
}

func printCounterBytes(fams map[string]*dto.MetricFamily, name, lbl string) {
	v := counterVal(fams, name)
	if v == 0 {
		return
	}
	_, _ = label.Printf("  %-10s ", lbl)
	fmt.Println(humanBytes(v))
}

func printGauge(fams map[string]*dto.MetricFamily, name, lbl string) {
	fam := fams[name]
	if fam == nil {
		return
	}
	for _, m := range fam.GetMetric() {
		v := m.GetGauge().GetValue()
		_, _ = label.Printf("  %-10s ", lbl)
		fmt.Println(humanCount(v))
	}
}

func printHistSummary(fams map[string]*dto.MetricFamily, name, lbl string) {
	fam := fams[name]
	if fam == nil {
		return
	}
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		count := h.GetSampleCount()
		if count == 0 {
			continue
		}
		sum := h.GetSampleSum()
		avg := sum / float64(count)
		p50 := histPercentile(h, 0.5)
		p99 := histPercentile(h, 0.99)
		_, _ = label.Printf("  %-10s ", lbl)
		fmt.Printf("%s calls  avg %s  p50 %s  p99 %s\n",
			humanCount(float64(count)),
			humanDuration(avg), humanDuration(p50), humanDuration(p99))
	}
}

func histPercentile(h *dto.Histogram, q float64) float64 {
	count := float64(h.GetSampleCount())
	target := q * count
	buckets := h.GetBucket()
	for _, b := range buckets {
		if float64(b.GetCumulativeCount()) >= target {
			return b.GetUpperBound()
		}
	}
	if len(buckets) > 0 {
		return buckets[len(buckets)-1].GetUpperBound()
	}
	return 0
}

func labelVal(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

func aggregateByLabel(fam *dto.MetricFamily, lbl string) map[string]float64 {
	m := make(map[string]float64)
	for _, met := range fam.GetMetric() {
		v := met.GetCounter().GetValue()
		if v > 0 {
			key := labelVal(met, lbl)
			m[key] += v
		}
	}
	return m
}

func sortedEntries(m map[string]float64) []string {
	type kv struct {
		k string
		v float64
	}
	var pairs []kv
	for k, v := range m {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].v > pairs[j].v })
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = fmt.Sprintf("%s:%s", p.k, humanCount(p.v))
	}
	return parts
}

func humanBytes(b float64) string {
	switch {
	case b >= 1<<30:
		return strconv.FormatFloat(b/(1<<30), 'f', 1, 64) + " GB"
	case b >= 1<<20:
		return strconv.FormatFloat(b/(1<<20), 'f', 1, 64) + " MB"
	case b >= 1<<10:
		return strconv.FormatFloat(b/(1<<10), 'f', 1, 64) + " KB"
	default:
		return strconv.FormatFloat(b, 'f', 0, 64) + " B"
	}
}

func humanCount(v float64) string {
	if v >= 1e9 {
		return strconv.FormatFloat(v/1e9, 'f', 1, 64) + "B"
	}
	if v >= 1e6 {
		return strconv.FormatFloat(v/1e6, 'f', 1, 64) + "M"
	}
	if v >= 1e4 {
		return strconv.FormatFloat(v/1e3, 'f', 1, 64) + "K"
	}
	if v == math.Trunc(v) {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func humanDuration(s float64) string {
	switch {
	case s >= 60:
		return strconv.FormatFloat(s/60, 'f', 1, 64) + "m"
	case s >= 1:
		return strconv.FormatFloat(s, 'f', 2, 64) + "s"
	case s >= 0.001:
		return strconv.FormatFloat(s*1000, 'f', 1, 64) + "ms"
	default:
		return strconv.FormatFloat(s*1e6, 'f', 0, 64) + "us"
	}
}
