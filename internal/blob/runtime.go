package blob

import (
	"context"

	"github.com/semistrict/loophole/internal/env"
)

// Open resolves the configured blob store for a store URL.
func Open(ctx context.Context, inst env.ResolvedStore) (*Store, error) {
	var drv Driver
	var err error
	if inst.IsLocal() {
		drv, err = NewFileDriver(inst.LocalDir)
	} else if inst.IsGCS() {
		drv, err = NewGCSDriver(ctx, inst)
	} else {
		drv, err = NewS3Driver(ctx, inst)
	}
	if err != nil {
		return nil, err
	}
	s := New(drv)
	if inst.Prefix != "" {
		s.prefix = inst.Prefix + "/"
	}
	return s, nil
}
