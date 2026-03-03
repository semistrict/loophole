// Package metrics provides Prometheus metrics for loophole.
//
// All metrics are registered in a dedicated registry (not the global
// default) so they don't collide with anything else.  Call Handler
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

// Handler returns an http.Handler that serves Prometheus metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

// --- Helpers ---

// Timer records the duration of an operation into a histogram when
// stopped. Create one with NewTimer.
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
