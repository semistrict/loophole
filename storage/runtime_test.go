package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "8192")
	cfg := ConfigFromEnv()
	require.Equal(t, int64(8192), cfg.FlushThreshold)

	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "invalid")
	cfg = ConfigFromEnv()
	require.Zero(t, cfg.FlushThreshold)

	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "-1")
	cfg = ConfigFromEnv()
	require.Zero(t, cfg.FlushThreshold)
}

func TestProfileRuntimeHelpers(t *testing.T) {
	dir := env.Dir(t.TempDir())
	inst := env.ResolvedProfile{
		ProfileName: "test-profile",
		LocalDir:    filepath.Join(t.TempDir(), "store"),
	}

	cacheDir := ProfileCacheDir(dir, inst)
	require.Equal(t, filepath.Join(string(dir), "cache", inst.ProfileName), cacheDir)

	store := objstore.NewMemStore()
	m := NewManagerForProfile(inst, dir, store)
	require.Equal(t, store, m.ObjectStore)
	require.Equal(t, cacheDir, m.CacheDir)
	require.Equal(t, ConfigFromEnv(), m.config)
}

func TestOpenManagerForProfile(t *testing.T) {
	dir := env.Dir(t.TempDir())
	inst := env.ResolvedProfile{
		ProfileName: "local",
		LocalDir:    filepath.Join(t.TempDir(), "store"),
	}

	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "16384")

	m, err := OpenManagerForProfile(context.Background(), inst, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Close())
	})

	require.NotNil(t, m.Store())
	require.Equal(t, filepath.Join(string(dir), "cache", inst.ProfileName), m.CacheDir)
	require.Equal(t, int64(16384), m.config.FlushThreshold)
}
