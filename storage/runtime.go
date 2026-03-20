package storage

import (
	"context"
	"os"
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

// ProfileCacheDir returns the profile-scoped cache directory.
func ProfileCacheDir(dir env.Dir, inst env.ResolvedProfile) string {
	return dir.Cache(inst.ProfileName)
}

// NewManagerForProfile constructs a storage manager using the shared runtime defaults.
func NewManagerForProfile(inst env.ResolvedProfile, dir env.Dir, store objstore.ObjectStore) *Manager {
	return &Manager{
		ObjectStore: store,
		CacheDir:    ProfileCacheDir(dir, inst),
		config:      ConfigFromEnv(),
	}
}

// OpenManagerForProfile resolves the object store and constructs a storage manager.
func OpenManagerForProfile(ctx context.Context, inst env.ResolvedProfile, dir env.Dir) (*Manager, error) {
	store, err := objstore.OpenForProfile(ctx, inst)
	if err != nil {
		return nil, err
	}
	m := NewManagerForProfile(inst, dir, store)
	if err := m.init(); err != nil {
		return nil, err
	}
	return m, nil
}
