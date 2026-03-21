package objstore

import (
	"context"
	"strings"

	"github.com/semistrict/loophole/env"
)

// OpenForProfile resolves the configured object store for a profile.
func OpenForProfile(ctx context.Context, inst env.ResolvedProfile) (ObjectStore, error) {
	if inst.LocalDir != "" {
		return NewFileStore(inst.LocalDir)
	}
	if isGCSEndpoint(inst.Endpoint) {
		return NewGCSStore(ctx, inst)
	}
	return NewS3Store(ctx, inst)
}

// isGCSEndpoint returns true if the endpoint is Google Cloud Storage.
func isGCSEndpoint(endpoint string) bool {
	return strings.Contains(endpoint, "storage.googleapis.com") ||
		strings.Contains(endpoint, "storage.cloud.google.com")
}
