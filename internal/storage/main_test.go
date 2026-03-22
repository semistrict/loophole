package storage

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func init() {
	// Disable zstd compression in tests to avoid channel conflicts with synctest.
	testOverrides = func(c *Config) { c.DisableCompression = true }
}

func TestMain(m *testing.M) {
	level := slog.Level(100)
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
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
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	os.Exit(m.Run())
}

func debugCountersEnabled() bool {
	return os.Getenv("LOOPHOLE_DEBUG_COUNTERS") != ""
}

func checkpointAndClone(t testing.TB, v *Volume, name string) error {
	t.Helper()
	cpID, err := v.Checkpoint()
	if err != nil {
		return fmt.Errorf("checkpoint before clone: %w", err)
	}
	return Clone(t.Context(), v.manager.Store(), v.Name(), cpID, name)
}

// cloneOpen clones a volume and opens the clone on a separate manager
// (sharing the same store), mirroring production where each volume is
// owned by a different process.
func cloneOpen(t testing.TB, v *Volume, name string) *Volume {
	t.Helper()
	if err := checkpointAndClone(t, v, name); err != nil {
		t.Fatalf("clone %q: %v", name, err)
	}
	m := v.manager
	m2 := &Manager{
		BlobStore: m.Store(),
		CacheDir:  m.CacheDir,
		config:    m.config,
		fs:        m.fs,
	}
	t.Cleanup(func() { _ = m2.Close() })
	clone, err := m2.OpenVolume(name)
	if err != nil {
		t.Fatalf("open clone %q: %v", name, err)
	}
	return clone
}
