package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
)

// GCSStore implements ObjectStore backed by Google Cloud Storage.
type GCSStore struct {
	client           *storage.Client
	bucket           string
	prefix           string // includes trailing slash if non-empty
	readRetries      int
	readBaseDelay    time.Duration
	sleepWithContext func(context.Context, time.Duration) error
}

// NewGCSStore creates a GCSStore from a resolved profile.
// Uses application default credentials from the environment (GCE metadata, ADC, etc.).
func NewGCSStore(ctx context.Context, inst env.ResolvedProfile) (*GCSStore, error) {
	var opts []option.ClientOption

	// On GCE, ADC is available automatically. No explicit credentials needed.
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}

	prefix := ""
	if inst.Prefix != "" {
		prefix = inst.Prefix + "/"
	}
	return &GCSStore{
		client:           client,
		bucket:           inst.Bucket,
		prefix:           prefix,
		readRetries:      2,
		readBaseDelay:    100 * time.Millisecond,
		sleepWithContext: sleepWithContext,
	}, nil
}

func (s *GCSStore) obj(key string) *storage.ObjectHandle {
	return s.client.Bucket(s.bucket).Object(s.fullKey(key))
}

func (s *GCSStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + key
}

func (s *GCSStore) At(path string) ObjectStore {
	p := path + "/"
	if s.prefix != "" {
		p = s.prefix + p
	}
	return &GCSStore{
		client:           s.client,
		bucket:           s.bucket,
		prefix:           p,
		readRetries:      s.readRetries,
		readBaseDelay:    s.readBaseDelay,
		sleepWithContext: s.sleepWithContext,
	}
}

func (s *GCSStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "Get", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.S3Op("get")
		obj := s.obj(key)
		// Get attrs first to learn the generation and etag, then pin
		// the read to that generation so body and etag are consistent.
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			done(err)
			if errors.Is(err, storage.ErrObjectNotExist) {
				return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), ErrNotFound)
			}
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		r, err := obj.Generation(attrs.Generation).NewReader(ctx)
		if err != nil {
			done(err)
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		done(nil)
		metrics.S3Transfer("get", "rx", r.Attrs.Size)
		return r, attrs.Etag, nil
	})
}

func (s *GCSStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "GetRange", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.S3Op("get")
		obj := s.obj(key)
		// Get attrs first to learn the generation and etag, then pin
		// the range read to that generation so body and etag are consistent.
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			done(err)
			if errors.Is(err, storage.ErrObjectNotExist) {
				return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), ErrNotFound)
			}
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		r, err := obj.Generation(attrs.Generation).NewRangeReader(ctx, offset, length)
		if err != nil {
			done(err)
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		done(nil)
		metrics.S3Transfer("get", "rx", length)
		return r, attrs.Etag, nil
	})
}

func (s *GCSStore) getWithRetry(ctx context.Context, op, key string, get func(context.Context) (io.ReadCloser, string, error)) (io.ReadCloser, string, error) {
	var lastErr error
	for attempt := range s.readRetries + 1 {
		body, etag, err := get(ctx)
		if err == nil {
			return body, etag, nil
		}
		if errors.Is(err, ErrNotFound) {
			return nil, "", err
		}
		lastErr = err
		if attempt == s.readRetries {
			return nil, "", lastErr
		}
		slog.Warn("retrying GCS read", "op", op, "key", s.fullKey(key), "attempt", attempt+1, "error", err)
		if err := s.sleepWithContext(ctx, s.readBaseDelay<<attempt); err != nil {
			return nil, "", lastErr
		}
	}
	panic("unreachable")
}

func (s *GCSStore) PutBytes(ctx context.Context, key string, data []byte) error {
	done := metrics.S3Op("put")
	w := s.obj(key).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if _, err := w.Write(data); err != nil {
		util.SafeClose(w, "close GCS writer")
		done(err)
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	if err := w.Close(); err != nil {
		done(err)
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	done(nil)
	metrics.S3Transfer("put", "tx", int64(len(data)))
	return nil
}

func (s *GCSStore) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	// GCS uses generation numbers for conditional writes, not ETags.
	// Parse the generation from the ETag (GCS ETags contain the generation).
	done := metrics.S3Op("put_cas")
	obj := s.obj(key)

	// Get current generation for the given etag.
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		done(err)
		return "", fmt.Errorf("CAS put %s: attrs: %w", s.fullKey(key), err)
	}
	if attrs.Etag != etag {
		done(fmt.Errorf("etag mismatch"))
		return "", fmt.Errorf("CAS put %s: etag mismatch", s.fullKey(key))
	}

	w := obj.If(storage.Conditions{GenerationMatch: attrs.Generation}).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if _, err := w.Write(data); err != nil {
		util.SafeClose(w, "close GCS CAS writer")
		done(err)
		return "", fmt.Errorf("CAS put %s: %w", s.fullKey(key), err)
	}
	if err := w.Close(); err != nil {
		done(err)
		return "", fmt.Errorf("CAS put %s: %w", s.fullKey(key), err)
	}
	done(nil)
	metrics.S3Transfer("put_cas", "tx", int64(len(data)))

	// Get the new etag.
	newAttrs, err := obj.Attrs(ctx)
	if err != nil {
		return "", fmt.Errorf("CAS put %s: get new etag: %w", s.fullKey(key), err)
	}
	return newAttrs.Etag, nil
}

func (s *GCSStore) PutReader(ctx context.Context, key string, r io.Reader) error {
	done := metrics.S3Op("put")
	w := s.obj(key).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	n, err := io.Copy(w, r)
	if err != nil {
		util.SafeClose(w, "close GCS writer")
		done(err)
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	if err := w.Close(); err != nil {
		done(err)
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	done(nil)
	metrics.S3Transfer("put", "tx", n)
	return nil
}

func (s *GCSStore) PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error {
	done := metrics.S3Op("put_if_not_exists")
	obj := s.obj(key)

	// DoesNotExist condition: only succeeds if object doesn't exist (generation == 0).
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if len(meta) > 0 && meta[0] != nil {
		w.Metadata = meta[0]
	}
	if _, err := w.Write(data); err != nil {
		util.SafeClose(w, "close GCS writer")
		done(err)
		return fmt.Errorf("put-if-not-exists %s: %w", s.fullKey(key), err)
	}
	if err := w.Close(); err != nil {
		done(err)
		if isPreconditionFailed(err) {
			return ErrExists
		}
		return fmt.Errorf("put-if-not-exists %s: %w", s.fullKey(key), err)
	}
	done(nil)
	metrics.S3Transfer("put_if_not_exists", "tx", int64(len(data)))
	return nil
}

func isPreconditionFailed(err error) bool {
	return strings.Contains(err.Error(), "conditionNotMet") ||
		strings.Contains(err.Error(), "412") ||
		strings.Contains(err.Error(), "PreconditionFailed")
}

func (s *GCSStore) DeleteObjects(ctx context.Context, keys []string) error {
	for _, key := range keys {
		done := metrics.S3Op("delete")
		err := s.obj(key).Delete(ctx)
		done(err)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				continue
			}
			return fmt.Errorf("delete %s: %w", s.fullKey(key), err)
		}
	}
	return nil
}

func (s *GCSStore) HeadMeta(ctx context.Context, key string) (map[string]string, error) {
	done := metrics.S3Op("head")
	attrs, err := s.obj(key).Attrs(ctx)
	done(err)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("head %s: %w", s.fullKey(key), ErrNotFound)
		}
		return nil, fmt.Errorf("head %s: %w", s.fullKey(key), err)
	}
	return attrs.Metadata, nil
}

func (s *GCSStore) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	done := metrics.S3Op("copy")
	_, err := s.obj(key).Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: meta,
	})
	done(err)
	if err != nil {
		return fmt.Errorf("set meta %s: %w", s.fullKey(key), err)
	}
	return nil
}

func (s *GCSStore) ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	done := metrics.S3Op("list")
	fullPrefix := s.fullKey(prefix)
	var result []ObjectInfo
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: fullPrefix})
	for {
		attrs, err := it.Next()
		if err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				break
			}
			// iterator.Done
			if err.Error() == "no more items in iterator" {
				break
			}
			done(err)
			return nil, fmt.Errorf("list %s: %w", fullPrefix, err)
		}
		rel := strings.TrimPrefix(attrs.Name, s.prefix)
		if prefix != "" {
			rel = strings.TrimPrefix(rel, prefix)
		}
		result = append(result, ObjectInfo{Key: rel, Size: attrs.Size})
	}
	done(nil)
	return result, nil
}
