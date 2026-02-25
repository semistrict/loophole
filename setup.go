package loophole

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

const (
	DefaultBlockSize    = 4 * 1024 * 1024
	DefaultMaxUploads   = 20
	DefaultMaxDownloads = 200
)

// NewObjectStore creates an ObjectStore from the instance configuration.
// If S3_ENDPOINT is "local:/path/to/dir", a local FileStore is used.
// Otherwise, an S3-compatible store is created.
func NewObjectStore(ctx context.Context, inst Instance, s3opts *S3Options) (ObjectStore, error) {
	endpoint := inst.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("S3_ENDPOINT")
	}
	if strings.HasPrefix(endpoint, "local:") {
		dir := strings.TrimPrefix(endpoint, "local:")
		return NewFileStore(dir)
	}
	return NewS3Store(ctx, inst, s3opts)
}

// SetupVolumeManager creates an object store, formats the system if needed,
// and returns a ready VolumeManager.
func SetupVolumeManager(ctx context.Context, inst Instance, dir Dir, s3opts *S3Options, logger *slog.Logger) (*VolumeManager, error) {
	store, err := NewObjectStore(ctx, inst, s3opts)
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

	vm, err := NewVolumeManager(ctx, store, dir.Cache(inst), DefaultMaxUploads, DefaultMaxDownloads)
	if err != nil {
		return nil, fmt.Errorf("init volume manager: %w", err)
	}

	return vm, nil
}
