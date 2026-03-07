package loophole

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"
)

// RetryStore wraps an ObjectStore and retries transient read failures.
// Only Get and GetRange are retried — writes use CAS or are idempotent
// at the caller level.
type RetryStore struct {
	inner      ObjectStore
	maxRetries int
	baseDelay  time.Duration
}

// NewRetryStore wraps store with retry logic for Get and GetRange.
// Retries up to 2 times with 100ms/200ms delays.
func NewRetryStore(store ObjectStore) ObjectStore {
	return &RetryStore{
		inner:      store,
		maxRetries: 2,
		baseDelay:  100 * time.Millisecond,
	}
}

func (r *RetryStore) At(path string) ObjectStore {
	return &RetryStore{
		inner:      r.inner.At(path),
		maxRetries: r.maxRetries,
		baseDelay:  r.baseDelay,
	}
}

func (r *RetryStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	var lastErr error
	for attempt := range r.maxRetries + 1 {
		body, etag, err := r.inner.Get(ctx, key)
		if err == nil {
			return body, etag, nil
		}
		if !r.retriable(err) {
			return nil, "", err
		}
		lastErr = err
		slog.Warn("retrying Get", "key", key, "attempt", attempt+1, "error", err)
		if err := r.sleep(ctx, attempt); err != nil {
			return nil, "", lastErr
		}
	}
	return nil, "", lastErr
}

func (r *RetryStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	var lastErr error
	for attempt := range r.maxRetries + 1 {
		body, etag, err := r.inner.GetRange(ctx, key, offset, length)
		if err == nil {
			return body, etag, nil
		}
		if !r.retriable(err) {
			return nil, "", err
		}
		lastErr = err
		slog.Warn("retrying GetRange", "key", key, "attempt", attempt+1, "error", err)
		if err := r.sleep(ctx, attempt); err != nil {
			return nil, "", lastErr
		}
	}
	return nil, "", lastErr
}

// retriable returns true for errors worth retrying. Not-found is permanent.
func (r *RetryStore) retriable(err error) bool {
	return !errors.Is(err, ErrNotFound)
}

func (r *RetryStore) sleep(ctx context.Context, attempt int) error {
	delay := r.baseDelay << attempt // 100ms, 200ms
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Pass-through methods for writes (no retry needed).

func (r *RetryStore) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	return r.inner.PutBytesCAS(ctx, key, data, etag)
}

func (r *RetryStore) PutReader(ctx context.Context, key string, rd io.Reader) error {
	return r.inner.PutReader(ctx, key, rd)
}

func (r *RetryStore) PutIfNotExists(ctx context.Context, key string, data []byte) error {
	return r.inner.PutIfNotExists(ctx, key, data)
}

func (r *RetryStore) DeleteObject(ctx context.Context, key string) error {
	return r.inner.DeleteObject(ctx, key)
}

func (r *RetryStore) ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return r.inner.ListKeys(ctx, prefix)
}
