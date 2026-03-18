package objstore

import (
	"context"

	"github.com/semistrict/loophole/env"
)

// OpenForProfile resolves the configured object store for a profile.
func OpenForProfile(ctx context.Context, inst env.ResolvedProfile) (ObjectStore, error) {
	if inst.LocalDir != "" {
		return NewFileStore(inst.LocalDir)
	}
	return NewS3Store(ctx, inst)
}
