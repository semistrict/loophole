package loophole

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/semistrict/loophole/metrics"
)

const (
	leaseDuration  = 60 * time.Second
	leaseRenewFreq = 30 * time.Second
)

// leaseFile is the JSON stored at leases/<token>.json — one per process.
type leaseFile struct {
	Expires string `json:"expires"`
}

// LeaseManager manages a single process-wide lease token stored in S3.
// The lease file is created lazily on the first Acquire call and
// renewed periodically by a background goroutine. On Close the file
// is deleted, signaling a clean shutdown. A missing lease file for a
// token means the holder shut down cleanly; an expired file means it
// crashed.
//
// Renewals use PutBytesCAS so we detect if another process deleted or
// overwrote our lease file.
type LeaseManager struct {
	token  string
	leases ObjectStore // base.At("leases")

	mu      sync.Mutex
	started bool
	etag    string // etag of our lease file, updated on each write
	stop    context.CancelFunc
	done    chan struct{}
}

// NewLeaseManager creates a manager with a fresh token. No S3 writes
// happen until EnsureStarted is called.
func NewLeaseManager(leases ObjectStore) *LeaseManager {
	return &LeaseManager{
		token:  uuid.NewString(),
		leases: leases,
		done:   make(chan struct{}),
	}
}

// Token returns the process-wide lease token.
func (lm *LeaseManager) Token() string { return lm.token }

// EnsureStarted creates the lease file and starts the background
// renewer on first call. Subsequent calls are no-ops.
func (lm *LeaseManager) EnsureStarted(ctx context.Context) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.started {
		return nil
	}
	if err := lm.createLease(ctx); err != nil {
		return fmt.Errorf("create lease file: %w", err)
	}
	renewCtx, cancel := context.WithCancel(context.Background())
	lm.stop = cancel
	lm.started = true
	go lm.renewLoop(renewCtx)
	return nil
}

// CheckAvailable returns nil if the given token's lease is absent,
// expired, or belongs to us. Returns an error if it's held by a live
// process.
func (lm *LeaseManager) CheckAvailable(ctx context.Context, existingToken string) error {
	if existingToken == "" || existingToken == lm.token {
		return nil
	}
	if lm.isValid(ctx, existingToken) {
		return fmt.Errorf("lease held by %q", existingToken)
	}
	return nil
}

// Close stops the background renewer and deletes the lease file.
func (lm *LeaseManager) Close(ctx context.Context) error {
	lm.mu.Lock()
	started := lm.started
	if lm.stop != nil {
		lm.stop()
	}
	lm.mu.Unlock()

	if started {
		<-lm.done
		return lm.leases.DeleteObject(ctx, lm.token+".json")
	}
	return nil
}

// --- internal ---

func (lm *LeaseManager) isValid(ctx context.Context, token string) bool {
	lf, _, err := ReadJSON[leaseFile](ctx, lm.leases, token+".json")
	if err != nil {
		return false // missing or unparseable → treat as expired
	}
	t, err := time.Parse(time.RFC3339, lf.Expires)
	if err != nil {
		return false
	}
	return time.Now().Before(t)
}

func (lm *LeaseManager) createLease(ctx context.Context) error {
	lf := leaseFile{Expires: time.Now().Add(leaseDuration).UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(lf)
	created, err := lm.leases.PutIfNotExists(ctx, lm.token+".json", data)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("lease token %s already exists", lm.token)
	}
	// Read back to get the etag for subsequent CAS renewals.
	body, etag, err := lm.leases.Get(ctx, lm.token+".json")
	if err != nil {
		return fmt.Errorf("read back lease etag: %w", err)
	}
	if err := body.Close(); err != nil {
		slog.Warn("close failed", "error", err)
	}
	lm.etag = etag
	return nil
}

func (lm *LeaseManager) renewLease(ctx context.Context) error {
	lf := leaseFile{Expires: time.Now().Add(leaseDuration).UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(lf)
	newEtag, err := lm.leases.PutBytesCAS(ctx, lm.token+".json", data, lm.etag)
	if err != nil {
		return fmt.Errorf("lease CAS renewal failed (etag mismatch?): %w", err)
	}
	lm.etag = newEtag
	return nil
}

func (lm *LeaseManager) renewLoop(ctx context.Context) {
	defer close(lm.done)
	ticker := time.NewTicker(leaseRenewFreq)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.LeaseRenewals.Inc()
			if err := lm.renewLease(ctx); err != nil {
				metrics.LeaseErrors.Inc()
				return
			}
		}
	}
}
