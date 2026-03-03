package lsm

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// SimLocalFS is an in-memory filesystem implementing LocalFS.
// Supports crash simulation: Crash() discards ephemeral files (mem/ dirs).
type SimLocalFS struct {
	mu    sync.Mutex
	files map[string][]byte // path → contents
	dirs  map[string]bool   // created directories
}

func NewSimLocalFS() *SimLocalFS {
	return &SimLocalFS{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (s *SimLocalFS) Create(path string) (File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = nil
	return &simFile{fs: s, path: path}, nil
}

func (s *SimLocalFS) Remove(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, path)
	return nil
}

func (s *SimLocalFS) MkdirAll(path string, _ os.FileMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirs[path] = true
	return nil
}

func (s *SimLocalFS) ReadFile(path string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[path]
	if !ok {
		return nil, fmt.Errorf("simfs: not found: %s", path)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *SimLocalFS) WriteFile(path string, data []byte, _ os.FileMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.files[path] = cp
	return nil
}

// Crash discards all files under paths containing "/mem/" (ephemeral memLayer
// files). Delta/image layer caches on disk also get discarded since they're
// just caches of S3 data and will be re-downloaded.
func (s *SimLocalFS) Crash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for path := range s.files {
		if strings.Contains(path, "/mem/") || strings.HasSuffix(path, ".ephemeral") {
			delete(s.files, path)
		}
	}
}

// setFile is used internally to update a file's contents (called by simFile.Close).
func (s *SimLocalFS) setFile(path string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = data
}

func (s *SimLocalFS) getFile(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[path]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, true
}

// simFile implements the File interface backed by SimLocalFS.
type simFile struct {
	fs     *SimLocalFS
	path   string
	mu     sync.Mutex
	buf    []byte
	pos    int64
	closed bool
}

func (f *simFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, fmt.Errorf("simfile: closed")
	}
	// Extend buffer if needed.
	end := f.pos + int64(len(p))
	if end > int64(len(f.buf)) {
		grown := make([]byte, end)
		copy(grown, f.buf)
		f.buf = grown
	}
	copy(f.buf[f.pos:], p)
	f.pos = end
	// Persist immediately (simulating write-through).
	f.fs.setFile(f.path, f.buf)
	return len(p), nil
}

func (f *simFile) ReadAt(p []byte, off int64) (int, error) {
	data, ok := f.fs.getFile(f.path)
	if !ok {
		return 0, fmt.Errorf("simfile: not found: %s", f.path)
	}
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *simFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, fmt.Errorf("simfile: closed")
	}
	data, _ := f.fs.getFile(f.path)
	size := int64(len(data))
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = size + offset
	}
	return f.pos, nil
}

func (f *simFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Verify SimLocalFS implements LocalFS at compile time.
var _ LocalFS = (*SimLocalFS)(nil)
