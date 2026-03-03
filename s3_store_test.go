package loophole

import (
	"bytes"
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
		t.Fatal("S3 integration tests require BUCKET")
	}
	store, err := NewS3Store(t.Context(), Instance{Bucket: bucket})
	require.NoError(t, err)
	// Scope to a random prefix so tests don't collide.
	return store.At("test-" + uuid.NewString()).(*S3Store)
}

// --- S3Store unit tests ---

func TestS3PutGetRoundtrip(t *testing.T) {
	store := newS3TestStore(t)

	data := []byte("hello s3")
	require.NoError(t, store.PutBytes(t.Context(), "key1", data))

	body, etag, err := store.Get(t.Context(), "key1")
	require.NoError(t, err)
	defer func() {
		if err := body.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	got, err := readAll(body)
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
	defer func() {
		if err := body.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	got, err := readAll(body)
	require.NoError(t, err)
	assert.Equal(t, []byte("567"), got)
}

func TestS3PutIfNotExists(t *testing.T) {
	store := newS3TestStore(t)

	created, err := store.PutIfNotExists(t.Context(), "unique", []byte("first"))
	require.NoError(t, err)
	assert.True(t, created)

	created, err = store.PutIfNotExists(t.Context(), "unique", []byte("second"))
	require.NoError(t, err)
	assert.False(t, created, "should not overwrite existing key")

	// Verify first value is preserved.
	body, _, err := store.Get(t.Context(), "unique")
	require.NoError(t, err)
	defer func() {
		if err := body.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()
	got, _ := readAll(body)
	assert.Equal(t, []byte("first"), got)
}

func TestS3CAS(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, store.PutBytes(t.Context(), "cas", []byte("v1")))

	_, etag, err := store.Get(t.Context(), "cas")
	require.NoError(t, err)

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
	defer func() {
		if err := body.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()
	got, _ := readAll(body)
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
	defer func() {
		if err := body.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()
	got, _ := readAll(body)
	assert.Equal(t, []byte("reader content"), got)
}

// --- Full integration: VolumeManager with real S3 ---

func TestS3VolumeWriteReadFlush(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, FormatSystem(t.Context(), store, 64))
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm.Connect(t.Context()))
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol, err := vm.NewVolume(t.Context(), "testvol", 0)
	require.NoError(t, err)

	data := bytes.Repeat([]byte("S"), 128)
	require.NoError(t, vol.Write(t.Context(), data, 0))
	require.NoError(t, vol.Flush(t.Context()))

	buf := make([]byte, 128)
	_, err = vol.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf)
}

func TestS3SnapshotAndClone(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, FormatSystem(t.Context(), store, 64))
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm.Connect(t.Context()))
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol, err := vm.NewVolume(t.Context(), "original", 0)
	require.NoError(t, err)

	// Write data and snapshot.
	require.NoError(t, vol.Write(t.Context(), bytes.Repeat([]byte("A"), 64), 0))
	require.NoError(t, vol.Snapshot(t.Context(), "snap1"))

	// Write more data after snapshot.
	require.NoError(t, vol.Write(t.Context(), bytes.Repeat([]byte("B"), 64), 64))

	// Clone from the live volume.
	clone, err := vol.Clone(t.Context(), "myclone")
	require.NoError(t, err)

	// Clone should see both blocks.
	buf := make([]byte, 128)
	_, err = clone.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("A"), 64), buf[:64])
	assert.Equal(t, bytes.Repeat([]byte("B"), 64), buf[64:])

	// Write to clone should not affect original.
	require.NoError(t, clone.Write(t.Context(), bytes.Repeat([]byte("C"), 64), 0))
	origBuf := make([]byte, 64)
	_, err = vol.Read(t.Context(), origBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("A"), 64), origBuf)
}

func TestS3CopyFromCoW(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, FormatSystem(t.Context(), store, 64))
	vm := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm.Connect(t.Context()))
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	src, err := vm.NewVolume(t.Context(), "src", 0)
	require.NoError(t, err)

	// Write and freeze.
	require.NoError(t, src.Write(t.Context(), bytes.Repeat([]byte("X"), 192), 0))
	require.NoError(t, src.Freeze(t.Context()))

	dst, err := vm.NewVolume(t.Context(), "dst", 0)
	require.NoError(t, err)

	n, err := dst.CopyFrom(t.Context(), src, 0, 0, 192)
	require.NoError(t, err)
	assert.Equal(t, uint64(192), n)

	// Read back.
	buf := make([]byte, 192)
	_, err = dst.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("X"), 192), buf)
}

func TestS3Reopen(t *testing.T) {
	store := newS3TestStore(t)

	require.NoError(t, FormatSystem(t.Context(), store, 64))

	// Write with first VM.
	vm1 := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm1.Connect(t.Context()))

	vol, err := vm1.NewVolume(t.Context(), "persist", 0)
	require.NoError(t, err)

	require.NoError(t, vol.Write(t.Context(), bytes.Repeat([]byte("P"), 64), 0))
	require.NoError(t, vm1.Close(t.Context()))

	// Read with second VM (different cache dir).
	vm2 := &legacyVolumeManager{Store: store, CacheDir: t.TempDir()}
	require.NoError(t, vm2.Connect(t.Context()))
	defer func() {
		if err := vm2.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol2, err := vm2.OpenVolume(t.Context(), "persist")
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = vol2.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("P"), 64), buf)
}

func readAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}
