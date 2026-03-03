package loophole

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

const (
	DefaultBlockSize      = 4 * 1024 * 1024
	DefaultVolumeSize     = 8 * 1024 * 1024 * 1024 // 8 GB
	DefaultMaxUploads     = 5
	DefaultMaxDownloads   = 200
	DefaultMaxDirtyBlocks = 100 // 100 * 4MB = 400MB max dirty data
)

// NewObjectStore creates an ObjectStore from the instance configuration.
// If the instance has a LocalDir, a FileStore is used. Otherwise, an
// S3-compatible store is created.
func NewObjectStore(ctx context.Context, inst Instance) (ObjectStore, error) {
	if inst.LocalDir != "" {
		return NewFileStore(inst.LocalDir)
	}
	return NewS3Store(ctx, inst)
}

// SetupVolumeManager creates an object store, formats the system if needed,
// and returns a ready VolumeManager.
func SetupVolumeManager(ctx context.Context, inst Instance, dir Dir, logger *slog.Logger) (*legacyVolumeManager, error) {
	store, err := NewObjectStore(ctx, inst)
	if err != nil {
		return nil, fmt.Errorf("connect to store: %w", err)
	}
	if _, ok := store.(*FileStore); ok {
		if logger != nil {
			logger.Warn("using local file store — data is NOT replicated to S3")
		}
	}

	if err := FormatSystem(ctx, store, DefaultBlockSize); err != nil {
		if strings.Contains(err.Error(), "already formatted") {
			if logger != nil {
				logger.Debug("system already formatted")
			}
		} else {
			return nil, fmt.Errorf("format system: %w", err)
		}
	}

	vm := &legacyVolumeManager{
		Store:    store,
		CacheDir: dir.Cache(inst.ProfileName),
	}
	if err := vm.Connect(ctx); err != nil {
		return nil, fmt.Errorf("init volume manager: %w", err)
	}

	return vm, nil
}
