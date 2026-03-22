package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/semistrict/loophole/internal/metrics"
)

// ErrNotFound is returned by ObjectStore.Get when the object does not exist.
var ErrNotFound = errors.New("not found")
var ErrExists = errors.New("already exists")

// ObjectStore abstracts access to an S3-compatible object store.
// It is rooted at a specific bucket and prefix — all keys are relative
// to that root.
type ObjectStore interface {
	// At returns a new ObjectStore rooted at the given sub-path.
	At(path string) ObjectStore

	// Get returns a streaming reader for the entire object, along with
	// the object's ETag.
	Get(ctx context.Context, key string) (body io.ReadCloser, etag string, err error)

	// GetRange returns a bounded range of bytes [offset, offset+length),
	// along with the object's ETag.
	GetRange(ctx context.Context, key string, offset, length int64) (body io.ReadCloser, etag string, err error)

	// PutBytesCAS writes data with If-Match on the given ETag. Returns the new ETag.
	PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (newEtag string, err error)
	PutReader(ctx context.Context, key string, r io.Reader) error
	// PutIfNotExists writes only if the key doesn't exist. Returns ErrExists if the key already exists.
	// Optional metadata is stored as S3 user-defined metadata (x-amz-meta-*).
	PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error

	DeleteObjects(ctx context.Context, keys []string) error
	ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error)

	// HeadMeta returns the user-defined metadata for an object without
	// downloading the body. Returns ErrNotFound if the object doesn't exist.
	HeadMeta(ctx context.Context, key string) (meta map[string]string, err error)
	// SetMeta replaces the user-defined metadata on an existing object
	// without re-uploading the body (S3 copy-to-self).
	SetMeta(ctx context.Context, key string, meta map[string]string) error
}

// ObjectInfo is a key + size returned by ListKeys.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ReadBytes fetches the raw bytes of an object from the store.
// Returns the data and the ETag (for CAS operations).
func ReadBytes(ctx context.Context, objects ObjectStore, key string) ([]byte, string, error) {
	body, etag, err := objects.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if err := body.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, "", err
	}
	return data, etag, nil
}

// ReadJSON fetches a JSON object from the store and decodes it.
// Returns the decoded value and the ETag (for CAS operations).
func ReadJSON[T any](ctx context.Context, objects ObjectStore, key string) (T, string, error) {
	var zero T
	body, etag, err := objects.Get(ctx, key)
	if err != nil {
		return zero, "", err
	}
	defer func() {
		if err := body.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()
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
