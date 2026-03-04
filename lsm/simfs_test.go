package lsm

import (
	"fmt"
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

// Verify SimLocalFS implements LocalFS at compile time.
var _ LocalFS = (*SimLocalFS)(nil)
