package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/env"
)

func skipWithoutS3(t *testing.T) {
	t.Helper()
	if os.Getenv("S3_ENDPOINT") == "" {
		t.Skip("S3 integration tests require S3_ENDPOINT")
	}
}

func newS3TestStore(t *testing.T) *S3Store {
	t.Helper()
	skipWithoutS3(t)
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}
	store, err := NewS3Store(t.Context(), env.ResolvedStore{
		StoreURL: os.Getenv("S3_ENDPOINT") + "/" + bucket,
		Bucket:   bucket,
		Endpoint: os.Getenv("S3_ENDPOINT"),
	})
	require.NoError(t, err)
	// Scope to a random prefix so tests don't collide.
	return store.At("test-" + uuid.NewString()).(*S3Store)
}

func TestS3PutGetRoundtrip(t *testing.T) {
	store := newS3TestStore(t)

	data := []byte("hello s3")
	require.NoError(t, store.PutBytes(t.Context(), "key1", data))

	body, etag, err := store.Get(t.Context(), "key1")
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, data, got)
	assert.NotEmpty(t, etag)
}

func TestS3GetRange(t *testing.T) {
	store := newS3TestStore(t)

	data := []byte("0123456789")
	require.NoError(t, store.PutBytes(t.Context(), "range", data))

	body, _, err := store.GetRange(t.Context(), "range", 5, 3)
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, []byte("567"), got)
}

func TestS3PutIfNotExists(t *testing.T) {
	store := newS3TestStore(t)

	err := store.PutIfNotExists(t.Context(), "unique", []byte("first"))
	require.NoError(t, err)

	err = store.PutIfNotExists(t.Context(), "unique", []byte("second"))
	assert.ErrorIs(t, err, ErrExists, "should not overwrite existing key")

	// Verify first value is preserved.
	body, _, err := store.Get(t.Context(), "unique")
	require.NoError(t, err)
	defer body.Close()
	got, _ := io.ReadAll(body)
	assert.Equal(t, []byte("first"), got)
}

func TestS3CAS(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, store.PutBytes(t.Context(), "cas", []byte("v1")))

	body, etag, err := store.Get(t.Context(), "cas")
	require.NoError(t, err)
	body.Close()

	// CAS with correct etag should succeed.
	newEtag, err := store.PutBytesCAS(t.Context(), "cas", []byte("v2"), etag)
	require.NoError(t, err)
	assert.NotEmpty(t, newEtag)

	// CAS with stale etag should fail.
	_, err = store.PutBytesCAS(t.Context(), "cas", []byte("v3"), etag)
	assert.Error(t, err, "stale etag should cause CAS conflict")
}

func TestS3Delete(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, store.PutBytes(t.Context(), "del", []byte("gone")))
	require.NoError(t, store.DeleteObjects(t.Context(), []string{"del"}))

	_, _, err := store.Get(t.Context(), "del")
	assert.Error(t, err)
}

func TestS3ListKeys(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, store.PutBytes(t.Context(), "a", []byte("1")))
	require.NoError(t, store.PutBytes(t.Context(), "b", []byte("22")))
	require.NoError(t, store.PutBytes(t.Context(), "c", []byte{})) // zero-length

	infos, err := store.ListKeys(t.Context(), "")
	require.NoError(t, err)

	keys := make(map[string]int64)
	for _, info := range infos {
		keys[info.Key] = info.Size
	}
	assert.Equal(t, int64(1), keys["a"])
	assert.Equal(t, int64(2), keys["b"])
	assert.Equal(t, int64(0), keys["c"])
}

func TestS3At(t *testing.T) {
	store := newS3TestStore(t)

	sub := store.At("sub")
	require.NoError(t, sub.PutReader(t.Context(), "nested", strings.NewReader("deep")))

	body, _, err := sub.Get(t.Context(), "nested")
	require.NoError(t, err)
	defer body.Close()
	got, _ := io.ReadAll(body)
	assert.Equal(t, []byte("deep"), got)

	// Should not be visible at root without prefix.
	_, _, err = store.Get(t.Context(), "nested")
	assert.Error(t, err)
}

func TestS3PutReader(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, store.PutReader(t.Context(), "fromreader", strings.NewReader("reader content")))

	body, _, err := store.Get(t.Context(), "fromreader")
	require.NoError(t, err)
	defer body.Close()
	got, _ := io.ReadAll(body)
	assert.Equal(t, []byte("reader content"), got)
}

type slowChunkReader struct {
	r     *bytes.Reader
	chunk int
	delay time.Duration
}

func (r *slowChunkReader) Read(p []byte) (int, error) {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	n := min(len(p), r.chunk)
	return r.r.Read(p[:n])
}

func (r *slowChunkReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func TestS3OverwriteIsAtomicForConcurrentReaders(t *testing.T) {
	store := newS3TestStore(t)

	const size = 4 << 20
	original := bytes.Repeat([]byte{'A'}, size)
	replacement := bytes.Repeat([]byte{'B'}, size)

	require.NoError(t, store.PutBytes(t.Context(), "atomic-overwrite", original))

	var sawRead atomic.Bool
	readErrCh := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				readErrCh <- nil
				return
			default:
			}

			body, _, err := store.Get(t.Context(), "atomic-overwrite")
			if err != nil {
				readErrCh <- err
				return
			}
			data, err := io.ReadAll(body)
			body.Close()
			if err != nil {
				readErrCh <- err
				return
			}
			sawRead.Store(true)
			if len(data) != size {
				readErrCh <- assert.AnError
				return
			}
			if bytes.Equal(data, original) || bytes.Equal(data, replacement) {
				continue
			}
			readErrCh <- io.ErrUnexpectedEOF
			return
		}
	}()

	// Keep the upload open long enough that concurrent readers exercise the
	// overwrite window instead of only pre/post states.
	err := store.PutReader(t.Context(), "atomic-overwrite", &slowChunkReader{
		r:     bytes.NewReader(replacement),
		chunk: 32 << 10,
		delay: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Eventually(t, sawRead.Load, 2*time.Second, 10*time.Millisecond)

	close(stop)
	require.NoError(t, <-readErrCh)

	body, _, err := store.Get(t.Context(), "atomic-overwrite")
	require.NoError(t, err)
	defer body.Close()
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, replacement, got)
}

func TestS3GetWithRetryRetriesTransientReadErrors(t *testing.T) {
	store := &S3Store{
		bucket:           "bucket",
		prefix:           "root/",
		readRetries:      2,
		readBaseDelay:    time.Millisecond,
		sleepWithContext: func(context.Context, time.Duration) error { return nil },
	}

	var attempts int
	body, etag, err := store.getWithRetry(t.Context(), "Get", "key", func(context.Context) (io.ReadCloser, string, error) {
		attempts++
		if attempts < 3 {
			return nil, "", errors.New("transient read failure")
		}
		return io.NopCloser(strings.NewReader("ok")), "etag-3", nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.Equal(t, "etag-3", etag)
	defer body.Close()
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), got)
}

func TestS3GetWithRetryDoesNotRetryNotFound(t *testing.T) {
	store := &S3Store{
		readRetries:      2,
		readBaseDelay:    time.Millisecond,
		sleepWithContext: func(context.Context, time.Duration) error { return nil },
	}

	var attempts int
	_, _, err := store.getWithRetry(t.Context(), "Get", "missing", func(context.Context) (io.ReadCloser, string, error) {
		attempts++
		return nil, "", fmt.Errorf("get missing: %w", ErrNotFound)
	})
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, 1, attempts)
}

func TestS3GetWithRetryStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := &S3Store{
		readRetries:   2,
		readBaseDelay: time.Millisecond,
		sleepWithContext: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	var attempts int
	_, _, err := store.getWithRetry(ctx, "Get", "key", func(context.Context) (io.ReadCloser, string, error) {
		attempts++
		return nil, "", errors.New("temporary")
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts)
}
