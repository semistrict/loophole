package storage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/objstore"
)

// ConfigFromEnv returns the shared storage manager configuration overrides
// used by binaries and tests.
func ConfigFromEnv() Config {
	cfg := Config{}
	if v := os.Getenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.FlushThreshold = n
		}
	}
	return cfg
}

// CacheDir returns the profile-scoped storage cache directory.
func CacheDir(dir env.Dir, inst env.ResolvedProfile) string {
	return dir.Cache(inst.ProfileName)
}

// OpenPageCacheForProfile opens the profile-scoped shared page cache.
func OpenPageCacheForProfile(dir env.Dir, inst env.ResolvedProfile) (*PageCache, error) {
	return NewPageCache(filepath.Join(CacheDir(dir, inst), "diskcache"))
}

// NewManagerForProfile constructs a storage manager using the shared runtime defaults.
func NewManagerForProfile(inst env.ResolvedProfile, dir env.Dir, store objstore.ObjectStore, diskCache *PageCache) *Manager {
	return NewManager(store, CacheDir(dir, inst), ConfigFromEnv(), nil, diskCache)
}

// OpenManagerForProfile resolves the object store and constructs a storage manager.
func OpenManagerForProfile(ctx context.Context, inst env.ResolvedProfile, dir env.Dir, diskCache *PageCache) (*Manager, error) {
	store, err := objstore.OpenForProfile(ctx, inst)
	if err != nil {
		return nil, err
	}
	return NewManagerForProfile(inst, dir, store, diskCache), nil
}
