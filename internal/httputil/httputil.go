// Package httputil provides shared HTTP helpers for loophole servers.
package httputil

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"

	"github.com/semistrict/loophole/internal/metrics"
)

// RegisterObservabilityRoutes registers /metrics and pprof endpoints.
func RegisterObservabilityRoutes(mux *http.ServeMux) {
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}

func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("writeJSON encode error", "error", err)
	}
}
