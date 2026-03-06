package juicefs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/diskcache"

	_ "github.com/mattn/go-sqlite3"
)

// testChunkHashDB implements chunkHashStore backed by an in-memory SQLite database.
type testChunkHashDB struct {
	db *sql.DB
}

func (m *testChunkHashDB) InitChunkHashTable() error {
	_, err := m.db.Exec(`CREATE TABLE IF NOT EXISTS lh_chunk_hash (
		slice_id INTEGER PRIMARY KEY,
		hashes   BLOB,
		inline   BLOB
	)`)
	return err
}

func (m *testChunkHashDB) GetChunkHash(sliceId uint64) ([]byte, []byte, error) {
	var hashes []byte
	var inline []byte
	err := m.db.QueryRow("SELECT hashes, inline FROM lh_chunk_hash WHERE slice_id = ?", sliceId).Scan(&hashes, &inline)
	if err != nil {
		return nil, nil, err
	}
	return hashes, inline, nil
}

func (m *testChunkHashDB) SetChunkHash(sliceId uint64, hashes []byte, inline []byte) error {
	_, err := m.db.Exec("INSERT OR REPLACE INTO lh_chunk_hash (slice_id, hashes, inline) VALUES (?, ?, ?)", sliceId, hashes, inline)
	return err
}

func (m *testChunkHashDB) DeleteChunkHash(sliceId uint64) error {
	_, err := m.db.Exec("DELETE FROM lh_chunk_hash WHERE slice_id = ?", sliceId)
	return err
}

func newTestChunkHashDB(t *testing.T) *testChunkHashDB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return &testChunkHashDB{db: db}
}

func testCASStore(t *testing.T) *casStore {
	t.Helper()
	db := newTestChunkHashDB(t)
	require.NoError(t, db.InitChunkHashTable())
	return newCASStore(Config{ObjStore: loophole.NewMemStore()}, db, chunk.Config{
		BlockSize: 1 << 20, // 1MB blocks for tests
	})
}

func TestCASWriter_SmallWrite(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(1)

	data := []byte("hello world")
	n, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	require.NoError(t, w.Finish(len(data)))

	r := s.NewReader(1, len(data))
	page := chunk.NewPage(make([]byte, len(data)))
	defer page.Release()
	n, err = r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page.Data)
}

func TestCASWriter_WriteAtOffset(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(2)

	_, err := w.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("world"), 10)
	require.NoError(t, err)

	require.NoError(t, w.Finish(15))

	r := s.NewReader(2, 15)
	page := chunk.NewPage(make([]byte, 15))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, 15, n)

	expected := make([]byte, 15)
	copy(expected[0:], "hello")
	copy(expected[10:], "world")
	require.Equal(t, expected, page.Data)
}

func TestCASWriter_Overwrite(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(3)

	_, err := w.WriteAt([]byte("aaaaaaaaaa"), 0)
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("BBBB"), 3)
	require.NoError(t, err)

	require.NoError(t, w.Finish(10))

	r := s.NewReader(3, 10)
	page := chunk.NewPage(make([]byte, 10))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, []byte("aaaBBBBaaa"), page.Data)
}

func TestCASWriter_MultiBlock(t *testing.T) {
	s := testCASStore(t)
	blockSize := s.conf.BlockSize

	w := s.NewWriter(4)
	data := make([]byte, blockSize+1000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))

	r := s.NewReader(4, len(data))
	page := chunk.NewPage(make([]byte, len(data)))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page.Data)
}

func TestCASWriter_MultiBlockPartialRead(t *testing.T) {
	s := testCASStore(t)
	blockSize := s.conf.BlockSize

	w := s.NewWriter(5)
	data := make([]byte, blockSize+500)
	for i := range data {
		data[i] = byte(i % 199)
	}
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))

	// Read across block boundary.
	r := s.NewReader(5, len(data))
	readOff := blockSize - 100
	page := chunk.NewPage(make([]byte, 200))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, readOff)
	require.NoError(t, err)
	require.Equal(t, 200, n)
	require.Equal(t, data[readOff:readOff+200], page.Data)
}

func TestCASStore_Remove(t *testing.T) {
	s := testCASStore(t)

	w := s.NewWriter(6)
	_, err := w.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(4))

	require.NoError(t, s.Remove(6, 4))

	r := s.NewReader(6, 4)
	page := chunk.NewPage(make([]byte, 4))
	defer page.Release()
	_, err = r.ReadAt(t.Context(), page, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lookup slice 6")
}

func TestCASStore_Dedup(t *testing.T) {
	s := testCASStore(t)
	memStore := s.objStore.(*loophole.MemStore)

	data := bytes.Repeat([]byte("dedup"), inlineMaxBytes/5+1) // >64KB to force S3 upload path

	w1 := s.NewWriter(10)
	_, err := w1.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w1.Finish(len(data)))

	w2 := s.NewWriter(11)
	_, err = w2.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w2.Finish(len(data)))

	for _, id := range []uint64{10, 11} {
		r := s.NewReader(id, len(data))
		page := chunk.NewPage(make([]byte, len(data)))
		n, err := r.ReadAt(t.Context(), page, 0)
		require.NoError(t, err, "slice %d", id)
		require.Equal(t, len(data), n)
		require.Equal(t, data, page.Data)
		page.Release()
	}

	keys, err := memStore.ListKeys(context.Background(), "chunks/")
	require.NoError(t, err)
	require.Equal(t, 1, len(keys), fmt.Sprintf("expected 1 CAS object, got %d", len(keys)))
}

func TestCASReaderCachesBlocksOnDisk(t *testing.T) {
	db := newTestChunkHashDB(t)
	require.NoError(t, db.InitChunkHashTable())
	objStore := loophole.NewMemStore()
	conf := chunk.Config{BlockSize: 1 << 20}

	// Write without disk cache so blocks only live in S3.
	writerStore := newCASStore(Config{ObjStore: objStore}, db, conf)
	data := bytes.Repeat([]byte("block-cache"), inlineMaxBytes/11+1) // >64KB to force S3 upload path
	w := writerStore.NewWriter(12)
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))

	// Read with fresh disk cache — first read hits S3 and populates cache.
	cache, err := diskcache.New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	readerStore := newCASStore(Config{ObjStore: objStore, DiskCache: cache}, db, conf)
	objStore.ResetCounts()

	r := readerStore.NewReader(12, len(data))
	page1 := chunk.NewPage(make([]byte, len(data)))
	defer page1.Release()
	n, err := r.ReadAt(t.Context(), page1, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page1.Data)
	require.EqualValues(t, 1, objStore.Count(loophole.OpGet))

	// Second read hits disk cache — no additional S3 calls.
	page2 := chunk.NewPage(make([]byte, len(data)))
	defer page2.Release()
	n, err = r.ReadAt(t.Context(), page2, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page2.Data)
	require.EqualValues(t, 1, objStore.Count(loophole.OpGet))
}

func TestCASStore_FlushPending(t *testing.T) {
	s := testCASStore(t)
	require.NoError(t, s.FlushPending(t.Context()))
}

func TestCASWriter_LengthMismatch(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(20)
	_, err := w.WriteAt([]byte("abc"), 0)
	require.NoError(t, err)

	err = w.Finish(10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "length mismatch")
}

func TestCASWriter_Abort(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(30)
	_, err := w.WriteAt([]byte("data that will be aborted"), 0)
	require.NoError(t, err)
	w.Abort()
}

func TestCASWriter_SetID(t *testing.T) {
	s := testCASStore(t)
	w := s.NewWriter(0)
	w.SetID(42)

	_, err := w.WriteAt([]byte("test"), 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(4))

	// Should be stored under ID 42.
	r := s.NewReader(42, 4)
	page := chunk.NewPage(make([]byte, 4))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("test"), page.Data)
}

func testCASStoreWithStaging(t *testing.T) (*casStore, *loophole.MemStore) {
	t.Helper()
	db := newTestChunkHashDB(t)
	require.NoError(t, db.InitChunkHashTable())
	objStore := loophole.NewMemStore()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	return newCASStore(Config{ObjStore: objStore, StagingDir: stagingDir}, db, chunk.Config{
		BlockSize: 1 << 20,
	}), objStore
}

func TestCASWriter_Writeback(t *testing.T) {
	s, objStore := testCASStoreWithStaging(t)
	defer s.Close()

	data := bytes.Repeat([]byte("writeback-test"), inlineMaxBytes/14+1) // >64KB

	w := s.NewWriter(100)
	w.SetWriteback(true)
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))

	// Data should be readable immediately from staging (before upload completes).
	r := s.NewReader(100, len(data))
	page := chunk.NewPage(make([]byte, len(data)))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page.Data)

	// After FlushPending, data should be in S3.
	require.NoError(t, s.FlushPending(t.Context()))

	keys, err := objStore.ListKeys(t.Context(), "chunks/")
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
}

func TestCASWriter_WritebackDedup(t *testing.T) {
	s, objStore := testCASStoreWithStaging(t)
	defer s.Close()

	data := bytes.Repeat([]byte("dedup-wb"), inlineMaxBytes/8+1) // >64KB

	for _, id := range []uint64{200, 201} {
		w := s.NewWriter(id)
		w.SetWriteback(true)
		_, err := w.WriteAt(data, 0)
		require.NoError(t, err)
		require.NoError(t, w.Finish(len(data)))
	}

	require.NoError(t, s.FlushPending(t.Context()))

	keys, err := objStore.ListKeys(t.Context(), "chunks/")
	require.NoError(t, err)
	require.Equal(t, 1, len(keys), fmt.Sprintf("expected 1 CAS object, got %d", len(keys)))

	// Both slices should be readable.
	for _, id := range []uint64{200, 201} {
		r := s.NewReader(id, len(data))
		page := chunk.NewPage(make([]byte, len(data)))
		n, err := r.ReadAt(t.Context(), page, 0)
		require.NoError(t, err, "slice %d", id)
		require.Equal(t, len(data), n)
		require.Equal(t, data, page.Data)
		page.Release()
	}
}

func TestCASWriter_WritebackFallback(t *testing.T) {
	db := newTestChunkHashDB(t)
	require.NoError(t, db.InitChunkHashTable())
	objStore := loophole.NewMemStore()
	// Use a path that will fail staging.
	s := newCASStore(Config{ObjStore: objStore, StagingDir: "/dev/null/impossible"}, db, chunk.Config{
		BlockSize: 1 << 20,
	})
	defer s.Close()

	data := bytes.Repeat([]byte("fallback"), inlineMaxBytes/8+1) // >64KB

	w := s.NewWriter(300)
	w.SetWriteback(true)
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	// Staging will fail, should fall back to direct upload.
	require.NoError(t, w.Finish(len(data)))

	r := s.NewReader(300, len(data))
	page := chunk.NewPage(make([]byte, len(data)))
	defer page.Release()
	n, err := r.ReadAt(t.Context(), page, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, page.Data)

	keys, err := objStore.ListKeys(t.Context(), "chunks/")
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
}

func TestCASStore_FlushPendingDrainsAll(t *testing.T) {
	s, objStore := testCASStoreWithStaging(t)
	defer s.Close()

	// Write multiple slices with writeback.
	for id := uint64(400); id < 410; id++ {
		data := bytes.Repeat([]byte(fmt.Sprintf("drain-%d-", id)), inlineMaxBytes/10+1) // >64KB, unique per slice
		w := s.NewWriter(id)
		w.SetWriteback(true)
		_, err := w.WriteAt(data, 0)
		require.NoError(t, err)
		require.NoError(t, w.Finish(len(data)))
	}

	require.NoError(t, s.FlushPending(t.Context()))

	// All 10 slices should have been uploaded (each unique = 10 objects).
	keys, err := objStore.ListKeys(t.Context(), "chunks/")
	require.NoError(t, err)
	require.Equal(t, 10, len(keys))

	// No pending items should remain.
	s.pendingMu.Lock()
	remaining := len(s.pending)
	s.pendingMu.Unlock()
	require.Equal(t, 0, remaining)
}
