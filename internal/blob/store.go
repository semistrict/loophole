package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/semistrict/loophole/internal/metrics"
)

// ErrNotFound is returned by Store.Get when the object does not exist.
var ErrNotFound = errors.New("not found")
var ErrExists = errors.New("already exists")

// ObjectInfo is a key + size returned by ListKeys.
type ObjectInfo struct {
	Key  string
	Size int64
}

// Store wraps a Driver with prefix scoping, metrics, retries,
// and inflight tracking.
type Store struct {
	driver           Driver
	prefix           string // includes trailing slash if non-empty
	readRetries      int
	readBaseDelay    time.Duration
	sleepWithContext func(context.Context, time.Duration) error
}

// Option configures an Store.
type Option func(*Store)

// New creates an Store wrapping the given driver.
func New(driver Driver, opts ...Option) *Store {
	s := &Store{
		driver:           driver,
		readRetries:      2,
		readBaseDelay:    100 * time.Millisecond,
		sleepWithContext: defaultSleepWithContext,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Driver returns the underlying driver.
func (s *Store) Driver() Driver { return s.driver }

// At returns a new Store rooted at the given sub-path.
func (s *Store) At(path string) *Store {
	p := path + "/"
	if s.prefix != "" {
		p = s.prefix + p
	}
	return &Store{
		driver:           s.driver,
		prefix:           p,
		readRetries:      s.readRetries,
		readBaseDelay:    s.readBaseDelay,
		sleepWithContext: s.sleepWithContext,
	}
}

func (s *Store) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + key
}

// --- reads ---

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "Get", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.BlobOp("get")
		metrics.InflightDownloads.Inc()
		defer metrics.InflightDownloads.Dec()
		body, size, etag, err := s.driver.Get(ctx, s.fullKey(key), GetOpts{})
		done(err)
		if err != nil {
			return nil, "", err
		}
		if size > 0 {
			metrics.BlobTransfer("get", "rx", size)
		}
		return body, etag, nil
	})
}

func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "GetRange", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.BlobOp("get")
		metrics.InflightDownloads.Inc()
		defer metrics.InflightDownloads.Dec()
		body, size, etag, err := s.driver.Get(ctx, s.fullKey(key), GetOpts{Offset: offset, Length: length})
		done(err)
		if err != nil {
			return nil, "", err
		}
		transferSize := size
		if transferSize <= 0 {
			transferSize = length
		}
		if transferSize > 0 {
			metrics.BlobTransfer("get", "rx", transferSize)
		}
		return body, etag, nil
	})
}

func (s *Store) getWithRetry(ctx context.Context, op, key string, get func(context.Context) (io.ReadCloser, string, error)) (io.ReadCloser, string, error) {
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
		slog.Warn("retrying read", "op", op, "key", s.fullKey(key), "attempt", attempt+1, "error", err)
		if err := s.sleepWithContext(ctx, s.readBaseDelay<<attempt); err != nil {
			return nil, "", lastErr
		}
	}
	panic("unreachable")
}

// --- writes ---

func (s *Store) PutBytes(ctx context.Context, key string, data []byte) error {
	done := metrics.BlobOp("put")
	metrics.InflightUploads.Inc()
	defer metrics.InflightUploads.Dec()
	_, err := s.driver.Put(ctx, s.fullKey(key), bytes.NewReader(data), PutOpts{Size: int64(len(data))})
	done(err)
	if err != nil {
		return err
	}
	metrics.BlobTransfer("put", "tx", int64(len(data)))
	return nil
}

func (s *Store) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	done := metrics.BlobOp("put_cas")
	metrics.InflightUploads.Inc()
	defer metrics.InflightUploads.Dec()
	newEtag, err := s.driver.Put(ctx, s.fullKey(key), bytes.NewReader(data), PutOpts{
		Size:    int64(len(data)),
		IfMatch: etag,
	})
	done(err)
	if err != nil {
		return "", err
	}
	metrics.BlobTransfer("put_cas", "tx", int64(len(data)))
	return newEtag, nil
}

func (s *Store) PutReader(ctx context.Context, key string, r io.Reader) error {
	done := metrics.BlobOp("put")
	metrics.InflightUploads.Inc()
	defer metrics.InflightUploads.Dec()
	// Wrap to count bytes transferred. Use seekableCountingReader when
	// the underlying reader supports Seek (preserves Content-Length detection).
	cr := &countingReader{r: r}
	var body io.Reader
	if _, ok := r.(io.Seeker); ok {
		body = &seekableCountingReader{countingReader: cr}
	} else {
		body = cr
	}
	_, err := s.driver.Put(ctx, s.fullKey(key), body, PutOpts{Size: -1})
	done(err)
	metrics.BlobTransfer("put", "tx", cr.n)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error {
	done := metrics.BlobOp("put_if_not_exists")
	metrics.InflightUploads.Inc()
	defer metrics.InflightUploads.Dec()
	opts := PutOpts{
		Size:        int64(len(data)),
		IfNotExists: true,
	}
	if len(meta) > 0 && meta[0] != nil {
		opts.Metadata = meta[0]
	}
	_, err := s.driver.Put(ctx, s.fullKey(key), bytes.NewReader(data), opts)
	done(err)
	if err != nil {
		return err
	}
	metrics.BlobTransfer("put_if_not_exists", "tx", int64(len(data)))
	return nil
}

// --- other ---

func (s *Store) DeleteObjects(ctx context.Context, keys []string) error {
	done := metrics.BlobOp("delete")
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fullKeys[i] = s.fullKey(k)
	}
	err := s.driver.Delete(ctx, fullKeys)
	done(err)
	return err
}

func (s *Store) ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	done := metrics.BlobOp("list")
	result, err := s.driver.List(ctx, s.fullKey(prefix))
	done(err)
	if err != nil {
		return nil, err
	}
	// Strip the store prefix from returned keys.
	for i := range result {
		result[i].Key = strings.TrimPrefix(result[i].Key, s.prefix)
		if prefix != "" {
			result[i].Key = strings.TrimPrefix(result[i].Key, prefix)
		}
	}
	return result, nil
}

func (s *Store) HeadMeta(ctx context.Context, key string) (map[string]string, error) {
	done := metrics.BlobOp("head")
	meta, err := s.driver.Head(ctx, s.fullKey(key))
	done(err)
	return meta, err
}

func (s *Store) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	done := metrics.BlobOp("set_meta")
	err := s.driver.SetMeta(ctx, s.fullKey(key), meta)
	done(err)
	return err
}

// --- helpers ---

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type seekableCountingReader struct {
	*countingReader
}

func (s *seekableCountingReader) Seek(offset int64, whence int) (int64, error) {
	return s.r.(io.Seeker).Seek(offset, whence)
}

func defaultSleepWithContext(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// --- package-level helpers ---

// ReadBytes fetches the raw bytes of an object from the store.
// Returns the data and the ETag (for CAS operations).
func ReadBytes(ctx context.Context, objects *Store, key string) ([]byte, string, error) {
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
func ReadJSON[T any](ctx context.Context, objects *Store, key string) (T, string, error) {
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
func ModifyJSON[T any](ctx context.Context, objects *Store, key string, modify func(*T) error) error {
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
