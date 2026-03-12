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

type snapshotCapableVolume interface {
	Snapshot(string) error
}

func snapshotVolume(t testing.TB, v loophole.Volume, name string) error {
	t.Helper()
	sv, ok := v.(snapshotCapableVolume)
	if !ok {
		t.Fatalf("volume %T does not implement Snapshot", v)
	}
	return sv.Snapshot(name)
}
