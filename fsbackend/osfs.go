package fsbackend

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// osFS implements FS by delegating to os.* with a mountpoint prefix.
// Used by kernel backends (FUSE, NBD).
type osFS struct {
	root string
}

func newOSFS(mountpoint string) *osFS {
	return &osFS{root: mountpoint}
}

func (f *osFS) path(name string) string {
	return filepath.Join(f.root, name)
}

func (f *osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(f.path(name))
}

func (f *osFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(f.path(name), data, perm)
}

func (f *osFS) MkdirAll(name string, perm fs.FileMode) error {
	return os.MkdirAll(f.path(name), perm)
}

func (f *osFS) Remove(name string) error {
	return os.Remove(f.path(name))
}

func (f *osFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(f.path(name))
}

func (f *osFS) ReadDir(name string) ([]string, error) {
	entries, err := os.ReadDir(f.path(name))
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names, nil
}

func (f *osFS) Open(name string) (File, error) {
	return os.Open(f.path(name))
}

func (f *osFS) Create(name string) (File, error) {
	return os.Create(f.path(name))
}
