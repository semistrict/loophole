package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/semistrict/loophole/objstore"
)

// BreakLease requests the remote holder to release a volume.
func BreakLease(ctx context.Context, store objstore.ObjectStore, volumeName string, force bool) (bool, error) {
	volRefs := store.At("volumes")
	leases := store.At("leases")

	key, err := volumeIndexKey(volumeName)
	if err != nil {
		return false, err
	}
	ref, etag, err := objstore.ReadJSON[volumeRef](ctx, volRefs, key)
	if err != nil {
		return false, fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}
	if ref.LeaseToken == "" {
		return false, fmt.Errorf("volume %q has no active lease", volumeName)
	}

	oldToken := ref.LeaseToken
	lm := objstore.NewLeaseSession(leases)

	slog.Info("break-lease: requesting release", "volume", volumeName, "token", oldToken)
	_, rpcErr := lm.Call(ctx, oldToken, "release", map[string]string{"volume": volumeName})
	graceful := rpcErr == nil
	if rpcErr != nil {
		if !force {
			return false, fmt.Errorf("holder did not respond for volume %q (use -f to force): %w", volumeName, rpcErr)
		}
		slog.Warn("break-lease: holder did not respond, force-clearing", "volume", volumeName, "err", rpcErr)
	}

	ref.LeaseToken = ""
	data, err := json.Marshal(ref)
	if err != nil {
		return graceful, err
	}
	if _, err := volRefs.PutBytesCAS(ctx, key, data, etag); err != nil {
		return graceful, fmt.Errorf("clear lease token: %w", err)
	}

	if err := leases.DeleteObject(ctx, oldToken+".json"); err != nil {
		slog.Warn("delete stale lease file", "token", oldToken, "error", err)
	}

	return graceful, nil
}
