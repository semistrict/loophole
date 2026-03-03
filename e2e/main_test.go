//go:build linux

package e2e

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

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
	go http.ListenAndServe(":9090", mux)
	fmt.Println("metrics available on :9090/metrics")
	os.Exit(m.Run())
}
