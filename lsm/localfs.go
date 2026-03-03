package lsm

import (
	"io"
	"os"
	"time"
)

// LocalFS abstracts local filesystem operations for testability.
// Production uses OSLocalFS; simulation tests inject SimLocalFS.
type LocalFS interface {
	Create(path string) (File, error)
	Remove(path string) error
	MkdirAll(path string, perm os.FileMode) error
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
}

// File abstracts file I/O. Only the methods used by MemLayer are included.
type File interface {
	io.Closer
	Write(p []byte) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Seek(offset int64, whence int) (int64, error)
}

// Clock abstracts time for testability. Under synctest, time.Sleep
// advances the fake clock when all goroutines are blocked.
type Clock interface {
	Now() time.Time
}

// --- Production implementations ---

// OSLocalFS delegates to the real os package.
type OSLocalFS struct{}

func (OSLocalFS) Create(path string) (File, error) { return os.Create(path) }
func (OSLocalFS) Remove(path string) error         { return os.Remove(path) }
func (OSLocalFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
func (OSLocalFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (OSLocalFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// RealClock uses the real time package.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
