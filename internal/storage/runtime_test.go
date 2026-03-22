package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole/internal/env"
	"github.com/semistrict/loophole/internal/objstore"
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

func TestStoreRuntimeHelpers(t *testing.T) {
	dir := env.Dir(t.TempDir())
	inst := env.ResolvedStore{
		StoreURL: "file:///tmp/store",
		LocalDir: filepath.Join(t.TempDir(), "store"),
		VolsetID: "volset-123",
	}

	cacheDir := StoreCacheDir(dir, inst)
	require.Equal(t, filepath.Join(string(dir), "cache", inst.VolsetID), cacheDir)

	store := objstore.NewMemStore()
	m := NewManagerForStore(inst, dir, store)
	require.Equal(t, store, m.ObjectStore)
	require.Equal(t, cacheDir, m.CacheDir)
	require.Equal(t, ConfigFromEnv(), m.config)
}

func TestOpenManagerForStore(t *testing.T) {
	dir := env.Dir(t.TempDir())
	inst := env.ResolvedStore{
		StoreURL: "file://" + filepath.Join(t.TempDir(), "store"),
		LocalDir: filepath.Join(t.TempDir(), "store"),
	}

	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "16384")

	store, err := objstore.Open(context.Background(), inst)
	require.NoError(t, err)
	desc, _, err := FormatVolumeSet(context.Background(), store)
	require.NoError(t, err)
	inst.VolsetID = desc.VolsetID

	m, err := OpenManagerForStore(context.Background(), inst, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Close())
	})

	require.NotNil(t, m.Store())
	require.Equal(t, filepath.Join(string(dir), "cache", inst.VolsetID), m.CacheDir)
	require.Equal(t, int64(16384), m.config.FlushThreshold)
}
