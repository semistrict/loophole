package juicefs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/zeebo/blake3"

	loophole "github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/diskcache"
	"github.com/semistrict/loophole/internal/util"
)

const (
	hashSize       = 32       // BLAKE3-256
	chunkSize      = 1 << 26  // 64M (must match JuiceFS constant)
	inlineMaxBytes = 64 << 10 // 64KB — slices at or below this size are stored inline in bbolt
)

// chunkHashStore is the subset of the meta engine needed by the CAS store.
// Satisfied by dbMeta (SQLite/MySQL/Postgres) via type assertion.
type chunkHashStore interface {
	InitChunkHashTable() error
	GetChunkHash(sliceId uint64) (hashes []byte, inline []byte, err error)
	SetChunkHash(sliceId uint64, hashes []byte, inline []byte) error
	DeleteChunkHash(sliceId uint64) error
}

// pendingUpload represents a block staged to disk awaiting S3 upload.
type pendingUpload struct {
	hashHex string
	path    string
	done    chan struct{}
}

// casStore implements chunk.ChunkStore with content-addressable storage.
// Each slice's blocks are hashed with BLAKE3 and stored in S3 under
// chunks/{hash[0:2]}/{hash[2:4]}/{hash}. A SQLite table (lh_chunk_hash)
// maps slice IDs to their concatenated block hashes via the meta engine's
// database connection.
type casStore struct {
	objStore  loophole.ObjectStore
	diskCache *diskcache.DiskCache
	db        chunkHashStore
	conf      chunk.Config
	memUsed   atomic.Int64

	// Writeback staging: blocks are written to stagingDir and uploaded
	// to S3 asynchronously by background workers.
	stagingDir string                    // empty = writeback disabled
	pendingMu  sync.Mutex                //nolint:unused
	pending    map[string]*pendingUpload // hashHex -> item
	uploadCh   chan *pendingUpload
	workerWg   sync.WaitGroup
}

var _ chunk.ChunkStore = (*casStore)(nil)

func newCASStore(cfg Config, db chunkHashStore, conf chunk.Config) *casStore {
	s := &casStore{objStore: cfg.ObjStore, diskCache: cfg.DiskCache, db: db, conf: conf, stagingDir: cfg.StagingDir}
	if cfg.StagingDir != "" {
		s.pending = make(map[string]*pendingUpload)
		s.uploadCh = make(chan *pendingUpload, 2000)
		s.startUploadWorkers(conf.MaxUpload)
	}
	return s
}

func casKey(hash []byte) string {
	h := hex.EncodeToString(hash)
	return fmt.Sprintf("chunks/%s/%s/%s", h[:2], h[2:4], h)
}

func casCacheKey(hash []byte) string {
	return "c:" + hex.EncodeToString(hash)
}

func blake3Hash(data []byte) []byte {
	h := blake3.Sum256(data)
	return h[:]
}

// --- ChunkStore interface ---

func (s *casStore) NewReader(id uint64, length int) chunk.Reader {
	return &casReader{store: s, id: id, length: length}
}

func (s *casStore) NewWriter(id uint64) chunk.Writer {
	return &casWriter{
		store:  s,
		id:     id,
		blocks: make([][]byte, chunkSize/s.conf.BlockSize),
	}
}

func (s *casStore) Remove(id uint64, length int) error {
	return s.db.DeleteChunkHash(id)
}

func (s *casStore) FillCache(id uint64, length uint32) error  { return nil }
func (s *casStore) EvictCache(id uint64, length uint32) error { return nil }
func (s *casStore) CheckCache(id uint64, length uint32, handler func(exists bool, loc string, size int)) error {
	handler(false, "", 0)
	return nil
}

func (s *casStore) UsedMemory() int64      { return s.memUsed.Load() }
func (s *casStore) UpdateLimit(_, _ int64) {}

// FlushPending waits for all pending writeback uploads to complete.
func (s *casStore) FlushPending(ctx context.Context) error {
	if s.stagingDir == "" {
		return nil
	}
	s.pendingMu.Lock()
	items := make([]*pendingUpload, 0, len(s.pending))
	for _, item := range s.pending {
		items = append(items, item)
	}
	s.pendingMu.Unlock()

	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-item.done:
		}
	}
	return nil
}

// Close stops the upload workers. Must be called after FlushPending.
func (s *casStore) Close() {
	if s.uploadCh != nil {
		close(s.uploadCh)
		s.workerWg.Wait()
	}
}

func (s *casStore) lookupHashes(id uint64) (hashes []byte, inline []byte, err error) {
	return s.db.GetChunkHash(id)
}

func (s *casStore) storeMapping(id uint64, hashes []byte, inline []byte) error {
	return s.db.SetChunkHash(id, hashes, inline)
}

// --- Staging (writeback) ---

func (s *casStore) stagingPath(hash []byte) string {
	h := hex.EncodeToString(hash)
	return filepath.Join(s.stagingDir, h[:2], h[2:4], h)
}

// stageBlock writes data to the staging directory. Idempotent by content hash.
func (s *casStore) stageBlock(hash, data []byte) error {
	path := s.stagingPath(hash)
	if _, err := os.Stat(path); err == nil {
		return nil // already staged
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Atomic write: temp file + rename.
	f, err := os.CreateTemp(filepath.Dir(path), ".stage-*")
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), path)
}

// enqueueUpload adds a block to the upload queue if not already pending.
func (s *casStore) enqueueUpload(hash []byte) {
	h := hex.EncodeToString(hash)
	s.pendingMu.Lock()
	if _, ok := s.pending[h]; ok {
		s.pendingMu.Unlock()
		return
	}
	item := &pendingUpload{
		hashHex: h,
		path:    s.stagingPath(hash),
		done:    make(chan struct{}),
	}
	s.pending[h] = item
	s.pendingMu.Unlock()
	s.uploadCh <- item
}

func (s *casStore) startUploadWorkers(n int) {
	if n <= 0 {
		n = 20
	}
	for range n {
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			for item := range s.uploadCh {
				s.doUpload(item)
			}
		}()
	}
}

func (s *casStore) doUpload(item *pendingUpload) {
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, item.hashHex)
		s.pendingMu.Unlock()
		close(item.done)
	}()

	data, err := os.ReadFile(item.path)
	if err != nil {
		slog.Warn("cas: staging read failed", "path", item.path, "error", err)
		return
	}

	hash, _ := hex.DecodeString(item.hashHex)
	key := casKey(hash)

	const maxRetries = 3
	for attempt := range maxRetries {
		_, err = s.objStore.PutIfNotExists(context.Background(), key, data)
		if err == nil {
			break
		}
		slog.Warn("cas: upload retry", "key", key, "attempt", attempt+1, "error", err)
	}
	if err != nil {
		slog.Error("cas: upload failed after retries", "key", key, "error", err)
		return
	}

	if s.diskCache != nil {
		s.diskCache.PutBlob(casCacheKey(hash), data)
	}

	_ = os.Remove(item.path)
}

// readStaged attempts to read a block from the staging directory.
func (s *casStore) readStaged(hash []byte) ([]byte, error) {
	return os.ReadFile(s.stagingPath(hash))
}

// --- Reader ---

type casReader struct {
	store  *casStore
	id     uint64
	length int
}

func (r *casReader) blockSize(indx int) int {
	bsize := r.length - indx*r.store.conf.BlockSize
	if bsize > r.store.conf.BlockSize {
		bsize = r.store.conf.BlockSize
	}
	return bsize
}

func (r *casReader) ReadAt(ctx context.Context, page *chunk.Page, off int) (int, error) {
	p := page.Data
	slog.Debug("cas: ReadAt", "id", r.id, "off", off, "len", len(p), "length", r.length)
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.length {
		return 0, io.EOF
	}

	hashes, inline, err := r.store.lookupHashes(r.id)
	if err != nil {
		return 0, fmt.Errorf("cas read: lookup slice %d: %w", r.id, err)
	}

	if len(inline) > 0 {
		if off >= len(inline) {
			return 0, io.EOF
		}
		n := copy(p, inline[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	blockSize := r.store.conf.BlockSize
	var got int
	for got < len(p) {
		curOff := off + got
		if curOff >= r.length {
			return got, io.EOF
		}

		indx := curOff / blockSize
		boff := curOff % blockSize
		blen := r.blockSize(indx)

		avail := blen - boff
		need := len(p) - got
		toRead := min(avail, need)

		hashStart := indx * hashSize
		hashEnd := hashStart + hashSize
		if hashEnd > len(hashes) {
			return got, fmt.Errorf("cas read: slice %d block %d: hash index out of range", r.id, indx)
		}
		hash := hashes[hashStart:hashEnd]
		cacheKey := casCacheKey(hash)

		var block []byte
		if r.store.diskCache != nil {
			block = r.store.diskCache.GetBlob(cacheKey)
		}
		if block == nil && r.store.stagingDir != "" {
			block, _ = r.store.readStaged(hash)
		}
		if block == nil {
			key := casKey(hash)
			var rc io.ReadCloser
			rc, _, err = r.store.objStore.Get(ctx, key)
			if err != nil {
				return got, fmt.Errorf("cas read: slice %d block %d: %w", r.id, indx, err)
			}
			block, err = io.ReadAll(rc)
			util.SafeClose(rc, "cas: close reader")
			if err != nil {
				return got, err
			}
			if r.store.diskCache != nil {
				r.store.diskCache.PutBlob(cacheKey, block)
			}
		}
		if boff >= len(block) {
			return got, io.EOF
		}
		end := boff + toRead
		if end > len(block) {
			n := copy(p[got:got+toRead], block[boff:])
			got += n
			return got, io.ErrUnexpectedEOF
		}
		copy(p[got:got+toRead], block[boff:end])
		got += toRead
	}

	return got, nil
}

// --- Writer ---

// casWriter uses a flat byte buffer per block. WriteAt writes at the given
// offset (matching cachedStore semantics). Finish stores small slices
// (≤ inlineMaxBytes) inline in bbolt; larger slices are hashed and uploaded
// to S3 under their BLAKE3 content-address.
type casWriter struct {
	store     *casStore
	id        uint64
	length    int
	uploaded  int
	blocks    [][]byte // flat buffer per block (nil = not yet written)
	writeback bool     // set via SetWriteback; enables staging path in Finish
}

func (w *casWriter) ID() uint64      { return w.id }
func (w *casWriter) SetID(id uint64) { w.id = id }
func (w *casWriter) SetWriteback(enabled bool) {
	w.writeback = enabled
}

func (w *casWriter) WriteAt(p []byte, off int64) (int, error) {
	slog.Debug("cas: WriteAt", "id", w.id, "off", off, "len", len(p))
	end := int(off) + len(p)
	if end > chunkSize {
		return 0, fmt.Errorf("write out of chunk boundary: %d > %d", end, chunkSize)
	}
	if off < int64(w.uploaded) {
		return 0, fmt.Errorf("cannot overwrite uploaded block: %d < %d", off, w.uploaded)
	}

	// Fill gap with zeros (same as cachedStore).
	if w.length < int(off) {
		zeros := make([]byte, int(off)-w.length)
		_, _ = w.WriteAt(zeros, int64(w.length))
	}

	blockSize := w.store.conf.BlockSize
	n := 0
	for n < len(p) {
		pos := int(off) + n
		indx := pos / blockSize
		boff := pos % blockSize

		if w.blocks[indx] == nil {
			w.blocks[indx] = make([]byte, 0, blockSize)
		}
		for len(w.blocks[indx]) < boff {
			w.blocks[indx] = append(w.blocks[indx], 0)
		}

		l := min(len(p)-n, blockSize-boff)
		endInBlock := boff + l
		for len(w.blocks[indx]) < endInBlock {
			w.blocks[indx] = append(w.blocks[indx], 0)
		}
		copy(w.blocks[indx][boff:boff+l], p[n:n+l])
		n += l
	}

	if end > w.length {
		w.length = end
	}
	return len(p), nil
}

type blockResult struct {
	indx int
	hash []byte
	err  error
}

func (w *casWriter) FlushTo(offset int) error {
	if offset < w.uploaded {
		panic(fmt.Sprintf("invalid offset: %d < %d", offset, w.uploaded))
	}
	blockSize := w.store.conf.BlockSize
	for i := range w.blocks {
		start := i * blockSize
		end := start + blockSize
		if start >= w.uploaded && end <= offset {
			w.uploaded = end
		}
	}
	return nil
}

func (w *casWriter) Finish(length int) error {
	slog.Debug("cas: Finish", "id", w.id, "length", length, "writerLength", w.length)
	if w.length != length {
		return fmt.Errorf("length mismatch: %v != %v", w.length, length)
	}

	if length <= inlineMaxBytes {
		return w.finishInline(length)
	}
	if w.writeback && w.store.stagingDir != "" {
		if err := w.finishWriteback(length); err != nil {
			slog.Warn("cas: writeback staging failed, falling back to direct upload", "id", w.id, "error", err)
			return w.finishUpload(length)
		}
		return nil
	}
	return w.finishUpload(length)
}

// finishInline concatenates all blocks and stores the data inline in bbolt.
func (w *casWriter) finishInline(length int) error {
	data := make([]byte, length)
	blockSize := w.store.conf.BlockSize
	for i, block := range w.blocks {
		if block == nil {
			continue
		}
		copy(data[i*blockSize:], block)
		w.blocks[i] = nil
	}
	return w.store.storeMapping(w.id, nil, data)
}

// finishWriteback stages blocks to disk and enqueues background uploads.
func (w *casWriter) finishWriteback(length int) error {
	blockSize := w.store.conf.BlockSize
	numBlocks := (length-1)/blockSize + 1

	// Hash and stage all blocks sequentially (blocks stay intact for fallback).
	hashes := make([]byte, numBlocks*hashSize)
	for i := 0; i < numBlocks; i++ {
		block := w.blocks[i]
		if block == nil {
			block = make([]byte, w.blockLen(i))
		}
		hash := blake3Hash(block)
		copy(hashes[i*hashSize:(i+1)*hashSize], hash)
		if err := w.store.stageBlock(hash, block); err != nil {
			return err
		}
	}

	// Store hash mapping in bbolt.
	if err := w.store.storeMapping(w.id, hashes, nil); err != nil {
		return err
	}

	// Enqueue uploads and release block memory.
	for i := 0; i < numBlocks; i++ {
		hashStart := i * hashSize
		w.store.enqueueUpload(hashes[hashStart : hashStart+hashSize])
		w.blocks[i] = nil
	}

	return nil
}

// finishUpload hashes and uploads all blocks to S3 in parallel (used by compaction).
func (w *casWriter) finishUpload(length int) error {
	blockSize := w.store.conf.BlockSize
	numBlocks := (length-1)/blockSize + 1

	results := make(chan blockResult, numBlocks)
	var wg sync.WaitGroup
	for i := 0; i < numBlocks; i++ {
		block := w.blocks[i]
		if block == nil {
			block = make([]byte, w.blockLen(i))
		}
		w.blocks[i] = nil
		wg.Add(1)
		go func(indx int, data []byte) {
			defer wg.Done()
			hash := blake3Hash(data)
			key := casKey(hash)
			_, err := w.store.objStore.PutIfNotExists(context.Background(), key, data)
			if err == nil && w.store.diskCache != nil {
				w.store.diskCache.PutBlob(casCacheKey(hash), data)
			}
			results <- blockResult{indx: indx, hash: hash, err: err}
		}(i, block)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	hashes := make([]byte, numBlocks*hashSize)
	for r := range results {
		if r.err != nil {
			return r.err
		}
		copy(hashes[r.indx*hashSize:(r.indx+1)*hashSize], r.hash)
	}

	return w.store.storeMapping(w.id, hashes, nil)
}

func (w *casWriter) blockLen(indx int) int {
	bsize := w.length - indx*w.store.conf.BlockSize
	if bsize > w.store.conf.BlockSize {
		bsize = w.store.conf.BlockSize
	}
	return bsize
}

func (w *casWriter) Abort() {
	for i := range w.blocks {
		w.blocks[i] = nil
	}
}
