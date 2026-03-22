package fsserver

import (
	"net/http"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/semistrict/loophole/internal/metrics"
	"github.com/semistrict/loophole/internal/volserver"
)

func (d *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	state := "running"
	if d.shuttingDown() {
		select {
		case <-d.doneCh:
			state = "stopped"
		default:
			state = "shutting_down"
		}
	}
	status := map[string]any{
		"state":     state,
		"store_url": d.inst.URL(),
		"volset_id": d.inst.VolsetID,
		"mode":      d.backend.Mode(),
		"socket":    d.socket,
		"cache":     d.dir.Cache(d.inst.VolsetID),
		"log":       d.logPath,
	}
	if d.backend != nil {
		status["volumes"] = d.backend.VM().Volumes()
		status["mounts"] = d.backend.Mounts()
	}
	if name := d.volumeName(); name != "" {
		status["volume"] = name
	}
	if mountpoint := d.currentMountpoint(); mountpoint != "" {
		status["mountpoint"] = mountpoint
	}
	if d.devicePath != "" {
		status["device"] = d.devicePath
	}
	status["metrics"] = collectMetrics()
	volserver.WriteJSON(w, status)
}

func (d *server) currentMountpoint() string {
	if d.backend != nil {
		if name := d.volumeName(); name != "" {
			return d.backend.MountpointForVolume(name)
		}
	}
	return ""
}

func counterVal(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func gaugeVal(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		return 0
	}
	return m.GetGauge().GetValue()
}

func histCount(h prometheus.Histogram) uint64 {
	m := &dto.Metric{}
	if err := h.(prometheus.Metric).Write(m); err != nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

func histSum(h prometheus.Histogram) float64 {
	m := &dto.Metric{}
	if err := h.(prometheus.Metric).Write(m); err != nil {
		return 0
	}
	return m.GetHistogram().GetSampleSum()
}

func collectMetrics() map[string]any {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return map[string]any{
		"goroutines":  runtime.NumGoroutine(),
		"heap_alloc":  mem.HeapAlloc,
		"heap_sys":    mem.HeapSys,
		"gc_pause_ns": mem.PauseNs[(mem.NumGC+255)%256],
		"num_gc":      mem.NumGC,
		"flush": map[string]any{
			"blocks_total":            counterVal(metrics.FlushBlocks),
			"bytes_total":             counterVal(metrics.FlushBytes),
			"pages_total":             counterVal(metrics.FlushPages),
			"errors_total":            counterVal(metrics.FlushErrors),
			"cycles":                  histCount(metrics.FlushDuration),
			"duration_sum_s":          histSum(metrics.FlushDuration),
			"upload_cycles":           histCount(metrics.FlushUploadDuration),
			"upload_duration_sum_s":   histSum(metrics.FlushUploadDuration),
			"backpressure_waits":      counterVal(metrics.BackpressureWaits),
			"backpressure_wait_sum_s": histSum(metrics.BackpressureWaitDuration),
			"early_flushes":           counterVal(metrics.EarlyFlushes),
			"frozen_tables":           gaugeVal(metrics.FrozenTableCount),
			"dirty_page_bytes":        gaugeVal(metrics.DirtyPageBytes),
			"superseded_deletes":      counterVal(metrics.FlushSupersededDeletes),
		},
		"s3": map[string]any{
			"inflight_uploads":   gaugeVal(metrics.InflightUploads),
			"inflight_downloads": gaugeVal(metrics.InflightDownloads),
		},
		"cache": map[string]any{
			"hits":   counterVal(metrics.CacheHits),
			"misses": counterVal(metrics.CacheMisses),
		},
		"layer": map[string]any{
			"read_bytes": counterVal(metrics.ReadBytes),
		},
	}
}
