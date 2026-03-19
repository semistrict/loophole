package storage

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

	// Build loophole-cached so page cache tests can spawn it.
	if os.Getenv("LOOPHOLE_CACHED_BIN") == "" {
		repoRoot := filepath.Clean(filepath.Join(must(os.Getwd()), ".."))
		binName := "loophole-cached-" + runtime.GOOS + "-" + runtime.GOARCH
		binPath := filepath.Join(repoRoot, "bin", binName)
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/loophole-cached")
		cmd.Dir = repoRoot
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			slog.Error("build loophole-cached for tests", "error", err)
			os.Exit(1)
		}
		os.Setenv("LOOPHOLE_CACHED_BIN", binPath)
	}

	os.Exit(m.Run())
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func debugCountersEnabled() bool {
	return os.Getenv("LOOPHOLE_DEBUG_COUNTERS") != ""
}

func snapshotVolume(t testing.TB, v *Volume, name string) error {
	t.Helper()
	return v.Clone(name)
}

// cloneOpen clones a volume and opens the clone on a separate manager
// (sharing the same store), mirroring production where each volume is
// owned by a different process.
func cloneOpen(t testing.TB, v *Volume, name string) *Volume {
	t.Helper()
	if err := v.Clone(name); err != nil {
		t.Fatalf("clone %q: %v", name, err)
	}
	m := v.manager
	m2 := NewManager(m.store, m.cacheDir, m.config, m.fs, m.diskCache)
	t.Cleanup(func() { _ = m2.Close() })
	clone, err := m2.OpenVolume(name)
	if err != nil {
		t.Fatalf("open clone %q: %v", name, err)
	}
	return clone
}
