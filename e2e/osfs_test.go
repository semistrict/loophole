package e2e

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// RootFS delegates filesystem operations to os.* under a mountpoint root.
type RootFS struct {
	root string
}

func NewRootFS(mountpoint string) *RootFS {
	return &RootFS{root: mountpoint}
}

func (f *RootFS) path(name string) string {
	return filepath.Join(f.root, name)
}

func (f *RootFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(f.path(name))
}

func (f *RootFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(f.path(name), data, perm)
}

func (f *RootFS) MkdirAll(name string, perm fs.FileMode) error {
	return os.MkdirAll(f.path(name), perm)
}

func (f *RootFS) Remove(name string) error {
	return os.Remove(f.path(name))
}

func (f *RootFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(f.path(name))
}

func (f *RootFS) ReadDir(name string) ([]string, error) {
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

func (f *RootFS) Open(name string) (*os.File, error) {
	return os.Open(f.path(name))
}

func (f *RootFS) Create(name string) (*os.File, error) {
	return os.Create(f.path(name))
}

func (f *RootFS) Lstat(name string) (fs.FileInfo, error) {
	return os.Lstat(f.path(name))
}

func (f *RootFS) Symlink(target, name string) error {
	return os.Symlink(target, f.path(name))
}

func (f *RootFS) Readlink(name string) (string, error) {
	return os.Readlink(f.path(name))
}

func (f *RootFS) Chmod(name string, mode fs.FileMode) error {
	return os.Chmod(f.path(name), mode)
}

func (f *RootFS) Lchown(name string, uid, gid int) error {
	return os.Lchown(f.path(name), uid, gid)
}

func (f *RootFS) Chtimes(name string, mtime int64) error {
	t := time.Unix(mtime, 0)
	return os.Chtimes(f.path(name), t, t)
}

func (f *RootFS) Rename(oldName, newName string) error {
	return os.Rename(f.path(oldName), f.path(newName))
}

func (f *RootFS) Link(existingPath, newPath string) error {
	return os.Link(f.path(existingPath), f.path(newPath))
}

func (f *RootFS) RemoveAll(name string) error {
	return os.RemoveAll(f.path(name))
}
