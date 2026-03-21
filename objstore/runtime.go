package objstore

import (
	"context"

	"github.com/semistrict/loophole/env"
)

// Open resolves the configured object store for a store URL.
func Open(ctx context.Context, inst env.ResolvedStore) (ObjectStore, error) {
	if inst.IsLocal() {
		return NewFileStore(inst.LocalDir)
	}
	if inst.IsGCS() {
		return NewGCSStore(ctx, inst)
	}
	return NewS3Store(ctx, inst)
}
