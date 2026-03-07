//go:build !js

package loophole

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	store, err := NewS3Store(t.Context(), Instance{
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
	require.NoError(t, store.DeleteObject(t.Context(), "del"))

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
