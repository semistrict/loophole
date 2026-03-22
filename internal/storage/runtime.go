package storage

import (
	"context"
	"os"
	"strconv"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/env"
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

// StoreCacheDir returns the volset-scoped cache directory.
func StoreCacheDir(dir env.Dir, inst env.ResolvedStore) string {
	return dir.Cache(inst.VolsetID)
}

// NewManagerForStore constructs a storage manager using the shared runtime defaults.
func NewManagerForStore(inst env.ResolvedStore, dir env.Dir, store *blob.Store) *Manager {
	return &Manager{
		BlobStore: store,
		CacheDir:  StoreCacheDir(dir, inst),
		config:    ConfigFromEnv(),
	}
}

// ResolveFormattedStore opens the store and reads the volset descriptor.
func ResolveFormattedStore(ctx context.Context, inst env.ResolvedStore) (env.ResolvedStore, *blob.Store, error) {
	store, err := blob.Open(ctx, inst)
	if err != nil {
		return env.ResolvedStore{}, nil, err
	}
	desc, err := CheckVolumeSet(ctx, store)
	if err != nil {
		return env.ResolvedStore{}, nil, err
	}
	inst.VolsetID = desc.VolsetID
	return inst, store, nil
}

// OpenManagerForStore resolves the object store and constructs a storage manager.
func OpenManagerForStore(ctx context.Context, inst env.ResolvedStore, dir env.Dir) (*Manager, error) {
	if inst.VolsetID == "" {
		var store *blob.Store
		var err error
		inst, store, err = ResolveFormattedStore(ctx, inst)
		if err != nil {
			return nil, err
		}
		m := NewManagerForStore(inst, dir, store)
		if err := m.init(); err != nil {
			return nil, err
		}
		return m, nil
	}
	store, err := blob.Open(ctx, inst)
	if err != nil {
		return nil, err
	}
	m := NewManagerForStore(inst, dir, store)
	if err := m.init(); err != nil {
		return nil, err
	}
	return m, nil
}
