package fsbackend

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/semistrict/loophole/lwext4"
)

// lwext4FSImpl implements FS using an in-process lwext4 instance.
type lwext4FSImpl struct {
	ext4 *lwext4.FS
}

func newLwext4FS(ext4fs *lwext4.FS) *lwext4FSImpl {
	return &lwext4FSImpl{ext4: ext4fs}
}

// splitPath splits a cleaned path into its components.
// "" and "/" both return nil (root directory).
func splitPath(name string) []string {
	name = path.Clean("/" + name)
	if name == "/" {
		return nil
	}
	return strings.Split(name[1:], "/") // strip leading /
}

// resolve walks from root to the named path, returning the inode.
func (f *lwext4FSImpl) resolve(name string) (lwext4.Ino, error) {
	parts := splitPath(name)
	ino := lwext4.RootIno
	for _, part := range parts {
		child, err := f.ext4.Lookup(ino, part)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}
		ino = child
	}
	return ino, nil
}

// resolveParent returns (parentIno, baseName, error).
func (f *lwext4FSImpl) resolveParent(name string) (lwext4.Ino, string, error) {
	parts := splitPath(name)
	if len(parts) == 0 {
		return 0, "", fmt.Errorf("cannot resolve parent of root")
	}
	parentIno := lwext4.RootIno
	for _, part := range parts[:len(parts)-1] {
		child, err := f.ext4.Lookup(parentIno, part)
		if err != nil {
			return 0, "", fmt.Errorf("%s: %w", name, err)
		}
		parentIno = child
	}
	return parentIno, parts[len(parts)-1], nil
}

func (f *lwext4FSImpl) ReadFile(name string) ([]byte, error) {
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	file, err := f.ext4.Open(ino)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func (f *lwext4FSImpl) WriteFile(name string, data []byte, perm fs.FileMode) error {
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}

	// Try to look up existing file; create if not found.
	ino, err := f.ext4.Lookup(parentIno, baseName)
	if err != nil {
		// File doesn't exist — create it.
		ino, err = f.ext4.Mknod(parentIno, baseName, uint32(perm&0o7777)|0o100000)
		if err != nil {
			return err
		}
	}

	file, err := f.ext4.OpenFile(ino, os.O_WRONLY)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(0); err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}

func (f *lwext4FSImpl) MkdirAll(name string, perm fs.FileMode) error {
	parts := splitPath(name)
	ino := lwext4.RootIno
	for _, part := range parts {
		child, err := f.ext4.Lookup(ino, part)
		if err != nil {
			// Doesn't exist — create.
			child, err = f.ext4.Mkdir(ino, part, uint32(perm&0o7777)|0o40000)
			if err != nil {
				return err
			}
		}
		ino = child
	}
	return nil
}

func (f *lwext4FSImpl) Remove(name string) error {
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}

	ino, err := f.ext4.Lookup(parentIno, baseName)
	if err != nil {
		return err
	}

	attr, err := f.ext4.GetAttr(ino)
	if err != nil {
		return err
	}

	if attr.Mode&0o40000 != 0 {
		return f.ext4.Rmdir(parentIno, baseName)
	}
	return f.ext4.Unlink(parentIno, baseName)
}

func (f *lwext4FSImpl) Stat(name string) (fs.FileInfo, error) {
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	attr, err := f.ext4.GetAttr(ino)
	if err != nil {
		return nil, err
	}

	parts := splitPath(name)
	baseName := "/"
	if len(parts) > 0 {
		baseName = parts[len(parts)-1]
	}

	return &lwext4FileInfo{
		name: baseName,
		attr: attr,
	}, nil
}

func (f *lwext4FSImpl) ReadDir(name string) ([]string, error) {
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	entries, err := f.ext4.Readdir(ino)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		names = append(names, e.Name)
	}
	sort.Strings(names)
	return names, nil
}

func (f *lwext4FSImpl) Open(name string) (File, error) {
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	return f.ext4.Open(ino)
}

func (f *lwext4FSImpl) Create(name string) (File, error) {
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return nil, err
	}

	// Try lookup first (truncate existing).
	ino, err := f.ext4.Lookup(parentIno, baseName)
	if err != nil {
		// Create new file.
		ino, err = f.ext4.Mknod(parentIno, baseName, 0o100644)
		if err != nil {
			return nil, err
		}
	}

	file, err := f.ext4.OpenFile(ino, os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(0); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

// lwext4FileInfo implements fs.FileInfo from lwext4 attributes.
type lwext4FileInfo struct {
	name string
	attr *lwext4.Attr
}

func (fi *lwext4FileInfo) Name() string      { return fi.name }
func (fi *lwext4FileInfo) Size() int64        { return int64(fi.attr.Size) }
func (fi *lwext4FileInfo) Mode() fs.FileMode  { return fs.FileMode(fi.attr.Mode) & 0o7777 }
func (fi *lwext4FileInfo) ModTime() time.Time { return time.Unix(int64(fi.attr.Mtime), 0) }
func (fi *lwext4FileInfo) IsDir() bool        { return fi.attr.Mode&0o40000 != 0 }
func (fi *lwext4FileInfo) Sys() any           { return fi.attr }
