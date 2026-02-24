package loophole

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/semistrict/loophole/metrics"
)

// ObjectStore abstracts access to an S3-compatible object store.
// It is rooted at a specific bucket and prefix — all keys are relative
// to that root.
type ObjectStore interface {
	// At returns a new ObjectStore rooted at the given sub-path.
	At(path string) ObjectStore

	// Get returns a streaming reader for the object starting at offset,
	// along with the object's ETag.
	Get(ctx context.Context, key string, offset int64) (body io.ReadCloser, etag string, err error)

	// PutBytesCAS writes data with If-Match on the given ETag. Returns the new ETag.
	PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (newEtag string, err error)
	PutReader(ctx context.Context, key string, r io.Reader) error
	// PutIfNotExists writes only if the key doesn't exist. Returns true if created.
	PutIfNotExists(ctx context.Context, key string, data []byte) (bool, error)

	DeleteObject(ctx context.Context, key string) error
	ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// ObjectInfo is a key + size returned by ListKeys.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ReadJSON fetches a JSON object from the store and decodes it.
// Returns the decoded value and the ETag (for CAS operations).
func ReadJSON[T any](ctx context.Context, objects ObjectStore, key string) (T, string, error) {
	var zero T
	body, etag, err := objects.Get(ctx, key, 0)
	if err != nil {
		return zero, "", err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return zero, "", err
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return zero, "", fmt.Errorf("parse %s: %w", key, err)
	}
	return v, etag, nil
}

// ModifyJSON does a read-modify-write on a JSON object using CAS.
// It retries up to 5 times on conflict (ETag mismatch).
func ModifyJSON[T any](ctx context.Context, objects ObjectStore, key string, modify func(*T) error) error {
	const maxAttempts = 5
	for attempt := range maxAttempts {
		metrics.CASAttempts.Inc()
		v, etag, err := ReadJSON[T](ctx, objects, key)
		if err != nil {
			return err
		}
		if err := modify(&v); err != nil {
			return err
		}
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = objects.PutBytesCAS(ctx, key, data, etag)
		if err == nil {
			return nil
		}
		metrics.CASRetries.Inc()
		if attempt == maxAttempts-1 {
			metrics.CASFailures.Inc()
			return fmt.Errorf("CAS conflict after %d attempts on %s: %w", maxAttempts, key, err)
		}
	}
	panic("unreachable")
}
