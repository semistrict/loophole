package lsm

import "os"

// LocalFS abstracts local filesystem operations for testability.
// Production uses OSLocalFS; simulation tests inject SimLocalFS.
type LocalFS interface {
	Remove(path string) error
	MkdirAll(path string, perm os.FileMode) error
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
}

// --- Production implementations ---

// OSLocalFS delegates to the real os package.
type OSLocalFS struct{}

func (OSLocalFS) Remove(path string) error { return os.Remove(path) }
func (OSLocalFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
func (OSLocalFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (OSLocalFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}
