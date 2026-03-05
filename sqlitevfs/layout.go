package sqlitevfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/semistrict/loophole"
)

const (
	headerSize   = 4096       // One LSM page at the start of the volume.
	magic        = "SQVFS002" // Volume format identifier.
	version      = 2
	mainDBOffset = headerSize // MainDB data starts right after the header.

	// MinVolumeSize is the minimum volume size to format.
	MinVolumeSize = 4 * 1024 * 1024 // 4MB
)

// fileKind identifies the type of SQLite file being operated on.
type fileKind int

const (
	fileMainDB  fileKind = iota // backed by the volume
	fileJournal                 // in-memory
	fileWAL                     // in-memory
	fileSHM                     // in-memory
)

// fileNames maps SQLite file names to their kind.
var fileNames = map[string]fileKind{
	"main.db":         fileMainDB,
	"main.db-journal": fileJournal,
	"main.db-wal":     fileWAL,
	"main.db-shm":     fileSHM,
}

// fileKindForName returns the file kind for a SQLite filename.
func fileKindForName(name string) (fileKind, bool) {
	k, ok := fileNames[name]
	return k, ok
}

// Header is the on-disk metadata at the start of a volume.
// The main database lives on the volume starting at mainDBOffset.
// The WAL is kept in memory during operation and persisted to the end
// of the volume on flush (for snapshot/clone correctness).
type Header struct {
	MainDBSize uint64
	WALSize    uint64 // WAL data lives at vol.Size() - WALSize
}

// FormatVolume writes an initial header to a volume.
func FormatVolume(ctx context.Context, vol loophole.Volume) error {
	size := vol.Size()
	if size < MinVolumeSize {
		return fmt.Errorf("volume too small: %d bytes (minimum %d)", size, MinVolumeSize)
	}
	return writeHeader(ctx, vol, &Header{})
}

// ReadHeader reads and validates the header from a volume.
func ReadHeader(ctx context.Context, vol loophole.Volume) (*Header, error) {
	buf := make([]byte, headerSize)
	if _, err := vol.Read(ctx, buf, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	return parseHeader(buf)
}

func parseHeader(buf []byte) (*Header, error) {
	if len(buf) < 32 {
		return nil, errors.New("header too short")
	}
	if string(buf[0:8]) != magic {
		return nil, errors.New("invalid header magic")
	}
	ver := binary.LittleEndian.Uint32(buf[8:12])
	if ver != version {
		return nil, fmt.Errorf("unsupported header version %d", ver)
	}
	return &Header{
		MainDBSize: binary.LittleEndian.Uint64(buf[16:24]),
		WALSize:    binary.LittleEndian.Uint64(buf[24:32]),
	}, nil
}

func writeHeader(ctx context.Context, vol loophole.Volume, h *Header) error {
	buf := make([]byte, headerSize)
	copy(buf[0:8], magic)
	binary.LittleEndian.PutUint32(buf[8:12], version)
	binary.LittleEndian.PutUint64(buf[16:24], h.MainDBSize)
	binary.LittleEndian.PutUint64(buf[24:32], h.WALSize)
	return vol.Write(ctx, buf, 0)
}

// memBuffer is an in-memory file buffer used for WAL, journal, and SHM files.
type memBuffer struct {
	mu     sync.RWMutex
	data   []byte
	exists bool
}

func (b *memBuffer) readAt(p []byte, off int64) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fileSize := int64(len(b.data))
	if off >= fileSize {
		clear(p)
		return len(p), io.EOF
	}

	n := int64(len(p))
	if off+n > fileSize {
		n = fileSize - off
	}
	copy(p[:n], b.data[off:off+n])
	if n < int64(len(p)) {
		clear(p[n:])
		return int(n), io.EOF
	}
	return int(n), nil
}

func (b *memBuffer) writeAt(p []byte, off int64) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	end := off + int64(len(p))
	if end > int64(len(b.data)) {
		grown := make([]byte, end)
		copy(grown, b.data)
		b.data = grown
	}
	copy(b.data[off:], p)
	return len(p)
}

func (b *memBuffer) truncate(size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cur := int64(len(b.data))
	if size < cur {
		clear(b.data[size:])
		b.data = b.data[:size]
	} else if size > cur {
		grown := make([]byte, size)
		copy(grown, b.data)
		b.data = grown
	}
}

func (b *memBuffer) fileSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return int64(len(b.data))
}

func (b *memBuffer) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = nil
	b.exists = false
}

func (b *memBuffer) setExists() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exists = true
}

func (b *memBuffer) isExists() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.exists
}

// snapshot returns a copy of the buffer data (nil if empty).
func (b *memBuffer) snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.data) == 0 {
		return nil
	}
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out
}

// restore replaces the buffer data and marks it as existing.
func (b *memBuffer) restore(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = data
	b.exists = len(data) > 0
}
