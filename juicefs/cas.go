package juicefs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/zeebo/blake3"

	loophole "github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

const (
	hashSize  = 32      // BLAKE3-256
	chunkSize = 1 << 26 // 64M (must match JuiceFS constant)
)

// chunkHashStore is the subset of the meta engine needed by the CAS store.
// Satisfied by dbMeta (SQLite/MySQL/Postgres) via type assertion.
type chunkHashStore interface {
	InitChunkHashTable() error
	GetChunkHash(sliceId uint64) (hashes []byte, inline []byte, err error)
	SetChunkHash(sliceId uint64, hashes []byte, inline []byte) error
	DeleteChunkHash(sliceId uint64) error
}

// casStore implements chunk.ChunkStore with content-addressable storage.
// Each slice's blocks are hashed with BLAKE3 and stored in S3 under
// chunks/{hash[0:2]}/{hash[2:4]}/{hash}. A SQLite table (lh_chunk_hash)
// maps slice IDs to their concatenated block hashes via the meta engine's
// database connection.
type casStore struct {
	objStore loophole.ObjectStore
	db       chunkHashStore
	conf     chunk.Config
	memUsed  atomic.Int64
}

var _ chunk.ChunkStore = (*casStore)(nil)

func newCASStore(objStore loophole.ObjectStore, db chunkHashStore, conf chunk.Config) *casStore {
	return &casStore{objStore: objStore, db: db, conf: conf}
}

func casKey(hash []byte) string {
	h := hex.EncodeToString(hash)
	return fmt.Sprintf("chunks/%s/%s/%s", h[:2], h[2:4], h)
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

func (s *casStore) UsedMemory() int64                    { return s.memUsed.Load() }
func (s *casStore) UpdateLimit(_, _ int64)               {}
func (s *casStore) FlushPending(_ context.Context) error { return nil }

func (s *casStore) lookupHashes(id uint64) (hashes []byte, inline []byte, err error) {
	return s.db.GetChunkHash(id)
}

func (s *casStore) storeMapping(id uint64, hashes []byte, inline []byte) error {
	return s.db.SetChunkHash(id, hashes, inline)
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
		if off+n >= len(inline) {
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
		key := casKey(hashes[hashStart:hashEnd])

		var rc io.ReadCloser
		if boff == 0 && toRead == blen {
			rc, _, err = r.store.objStore.Get(ctx, key)
		} else {
			rc, _, err = r.store.objStore.GetRange(ctx, key, int64(boff), int64(toRead))
		}
		if err != nil {
			return got, fmt.Errorf("cas read: slice %d block %d: %w", r.id, indx, err)
		}

		n, readErr := io.ReadFull(rc, p[got:got+toRead])
		util.SafeClose(rc, "cas: close reader")
		got += n
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return got, readErr
		}
	}

	return got, nil
}

// --- Writer ---

// casWriter uses a flat byte buffer per block. WriteAt writes at the given
// offset (matching cachedStore semantics). Finish hashes and uploads blocks
// to S3, then records the ID→hashes mapping in SQLite.
type casWriter struct {
	store    *casStore
	id       uint64
	length   int
	uploaded int
	blocks   [][]byte // flat buffer per block (nil = not yet written)
}

func (w *casWriter) ID() uint64                { return w.id }
func (w *casWriter) SetID(id uint64)           { w.id = id }
func (w *casWriter) SetWriteback(enabled bool) {}

func (w *casWriter) WriteAt(p []byte, off int64) (int, error) {
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
	if w.length != length {
		return fmt.Errorf("length mismatch: %v != %v", w.length, length)
	}

	blockSize := w.store.conf.BlockSize
	numBlocks := (length-1)/blockSize + 1

	// Hash and upload all blocks in parallel.
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
