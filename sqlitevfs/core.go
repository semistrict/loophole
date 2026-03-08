package sqlitevfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/semistrict/loophole"
)

// errVolumeFull is returned when a write exceeds the volume capacity.
var errVolumeFull = errors.New("write exceeds volume capacity")

type fileCore struct {
	vol      loophole.Volume
	header   *Header
	syncMode SyncMode

	mu           sync.Mutex
	dirty        bool
	mainDBExists bool
	wal          memBuffer
	journal      memBuffer
	shm          memBuffer

	fileMu sync.Mutex
	files  map[int]fileKind
}

func newFileCore(ctx context.Context, vol loophole.Volume, syncMode SyncMode) (*fileCore, error) {
	h, err := ReadHeader(ctx, vol)
	if err != nil {
		return nil, err
	}

	c := &fileCore{
		vol:          vol,
		header:       h,
		syncMode:     syncMode,
		mainDBExists: h.MainDBSize > 0,
		files:        make(map[int]fileKind),
	}

	if h.WALSize > 0 {
		walData := make([]byte, h.WALSize)
		if _, err := vol.Read(ctx, walData, vol.Size()-h.WALSize); err != nil {
			return nil, fmt.Errorf("read WAL from volume: %w", err)
		}
		c.wal.restore(walData)
	}

	return c, nil
}

func (c *fileCore) HeaderSnapshot() Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *c.header
}

func (c *fileCore) ReadOnly() bool {
	return c.vol.ReadOnly()
}

func (c *fileCore) SyncMode() SyncMode {
	return c.syncMode
}

func (c *fileCore) memBuf(k fileKind) *memBuffer {
	switch k {
	case fileWAL:
		return &c.wal
	case fileJournal:
		return &c.journal
	case fileSHM:
		return &c.shm
	default:
		return nil
	}
}

func (c *fileCore) FlushHeader() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	walData := c.wal.snapshot()
	walSize := uint64(len(walData))
	if walSize > 0 {
		if err := c.vol.Write(walData, c.vol.Size()-walSize); err != nil {
			return fmt.Errorf("persist WAL: %w", err)
		}
	}
	if walSize != c.header.WALSize {
		c.header.WALSize = walSize
		c.dirty = true
	}

	if !c.dirty {
		return nil
	}
	if err := writeHeader(c.vol, c.header); err != nil {
		return err
	}
	c.dirty = false
	return nil
}

func (c *fileCore) OpenFile(fileID int, filename string, flags int) (fileKind, int, error) {
	kind, ok := fileKindForName(filename)
	if !ok {
		return 0, 0, fmt.Errorf("unknown file %q", filename)
	}

	if kind == fileMainDB {
		c.mu.Lock()
		if flags&0x04 != 0 { // SQLITE_OPEN_CREATE
			c.mainDBExists = true
		} else if !c.mainDBExists {
			c.mu.Unlock()
			return 0, 0, fmt.Errorf("file %q does not exist", filename)
		}
		c.mu.Unlock()
	} else {
		buf := c.memBuf(kind)
		if flags&0x04 != 0 { // SQLITE_OPEN_CREATE
			buf.setExists()
		} else if !buf.isExists() {
			return 0, 0, fmt.Errorf("file %q does not exist", filename)
		}
	}

	c.fileMu.Lock()
	c.files[fileID] = kind
	c.fileMu.Unlock()

	outFlags := 0x02 // SQLITE_OPEN_READWRITE
	if c.vol.ReadOnly() {
		outFlags = 0x01 // SQLITE_OPEN_READONLY
	}
	return kind, outFlags, nil
}

func (c *fileCore) CloseFile(fileID int) error {
	c.fileMu.Lock()
	delete(c.files, fileID)
	c.fileMu.Unlock()
	return c.FlushHeader()
}

func (c *fileCore) fileKind(fileID int) (fileKind, error) {
	c.fileMu.Lock()
	kind, ok := c.files[fileID]
	c.fileMu.Unlock()
	if !ok {
		return 0, fmt.Errorf("unknown file id %d", fileID)
	}
	return kind, nil
}

func (c *fileCore) ReadFile(fileID int, p []byte, offset int64) (bool, error) {
	kind, err := c.fileKind(fileID)
	if err != nil {
		return false, err
	}

	if kind != fileMainDB {
		_, err := c.memBuf(kind).readAt(p, offset)
		return err != nil, nil
	}

	c.mu.Lock()
	fileSize := int64(c.header.MainDBSize)
	c.mu.Unlock()

	if offset >= fileSize {
		clear(p)
		return true, nil
	}

	toRead := int64(len(p))
	shortRead := false
	if offset+toRead > fileSize {
		toRead = fileSize - offset
		shortRead = true
	}
	if _, err := c.vol.Read(context.Background(), p[:toRead], mainDBOffset+uint64(offset)); err != nil && err != io.EOF {
		return false, fmt.Errorf("read at %d: %w", offset, err)
	}
	if int(toRead) < len(p) {
		clear(p[toRead:])
		shortRead = true
	}
	return shortRead, nil
}

func (c *fileCore) WriteFile(fileID int, p []byte, offset int64) error {
	kind, err := c.fileKind(fileID)
	if err != nil {
		return err
	}

	if kind != fileMainDB {
		c.memBuf(kind).writeAt(p, offset)
		return nil
	}

	volCap := int64(c.vol.Size()) - mainDBOffset
	if offset+int64(len(p)) > volCap {
		return errVolumeFull
	}
	if err := c.vol.Write(p, mainDBOffset+uint64(offset)); err != nil {
		return fmt.Errorf("write at %d: %w", offset, err)
	}

	newEnd := uint64(offset) + uint64(len(p))
	c.mu.Lock()
	if newEnd > c.header.MainDBSize {
		c.header.MainDBSize = newEnd
		c.dirty = true
	}
	c.mu.Unlock()
	return nil
}

func (c *fileCore) FileSize(fileID int) (int64, error) {
	kind, err := c.fileKind(fileID)
	if err != nil {
		return 0, err
	}
	if kind != fileMainDB {
		return c.memBuf(kind).fileSize(), nil
	}

	c.mu.Lock()
	size := int64(c.header.MainDBSize)
	c.mu.Unlock()
	return size, nil
}

func (c *fileCore) TruncateFile(fileID int, size int64) error {
	kind, err := c.fileKind(fileID)
	if err != nil {
		return err
	}
	if kind != fileMainDB {
		c.memBuf(kind).truncate(size)
		return nil
	}

	c.mu.Lock()
	oldSize := c.header.MainDBSize
	c.header.MainDBSize = uint64(size)
	c.dirty = true
	c.mu.Unlock()

	if uint64(size) < oldSize {
		if err := c.vol.PunchHole(mainDBOffset+uint64(size), oldSize-uint64(size)); err != nil {
			return fmt.Errorf("truncate punch hole: %w", err)
		}
	}
	return nil
}

func (c *fileCore) SyncFile() error {
	if c.syncMode == SyncModeAsync {
		return nil
	}
	if err := c.FlushHeader(); err != nil {
		return err
	}
	return c.vol.Flush()
}

func (c *fileCore) DeleteFile(filename string) error {
	kind, ok := fileKindForName(filename)
	if !ok {
		return nil
	}

	if kind == fileMainDB {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.mainDBExists {
			return nil
		}
		if c.header.MainDBSize > 0 {
			if err := c.vol.PunchHole(mainDBOffset, c.header.MainDBSize); err != nil {
				return err
			}
		}
		c.mainDBExists = false
		c.header.MainDBSize = 0
		c.dirty = true
		return nil
	}

	c.memBuf(kind).reset()
	return nil
}

func (c *fileCore) AccessFile(filename string, flags int) bool {
	kind, ok := fileKindForName(filename)
	if !ok {
		return false
	}

	var exists bool
	if kind == fileMainDB {
		c.mu.Lock()
		exists = c.mainDBExists
		c.mu.Unlock()
	} else {
		exists = c.memBuf(kind).isExists()
	}

	if flags == 1 { // SQLITE_ACCESS_READWRITE
		return exists && !c.vol.ReadOnly()
	}
	return exists
}
