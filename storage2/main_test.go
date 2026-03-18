package storage2

import (
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/semistrict/loophole"
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
	os.Exit(m.Run())
}

func debugCountersEnabled() bool {
	return os.Getenv("LOOPHOLE_DEBUG_COUNTERS") != ""
}

type cloneCapableVolume interface {
	Clone(string) error
}

func snapshotVolume(t testing.TB, v loophole.Volume, name string) error {
	t.Helper()
	cv, ok := v.(cloneCapableVolume)
	if !ok {
		t.Fatalf("volume %T does not implement Clone", v)
	}
	return cv.Clone(name)
}

func cloneOpen(t testing.TB, v loophole.Volume, name string) loophole.Volume {
	t.Helper()
	switch vv := v.(type) {
	case *volume:
		if err := vv.Clone(name); err != nil {
			t.Fatalf("clone %q: %v", name, err)
		}
		clone, err := vv.manager.OpenVolume(name)
		if err != nil {
			t.Fatalf("open clone %q: %v", name, err)
		}
		return clone
	case *frozenVolume:
		if err := vv.Clone(name); err != nil {
			t.Fatalf("clone %q: %v", name, err)
		}
		clone, err := vv.manager.OpenVolume(name)
		if err != nil {
			t.Fatalf("open clone %q: %v", name, err)
		}
		return clone
	default:
		t.Fatalf("volume %T does not support cloneOpen", v)
		return nil
	}
}
