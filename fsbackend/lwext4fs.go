package fsbackend

import (
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
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

// NewLwext4FS returns an FS backed by an in-process lwext4 instance.
func NewLwext4FS(ext4fs *lwext4.FS) FS {
	return newLwext4FS(ext4fs)
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
// It follows symlinks at every component (like the kernel).
func (f *lwext4FSImpl) resolve(name string) (lwext4.Ino, error) {
	return f.doResolve(name, true, 0)
}

// resolveLstat is like resolve but does not follow the final symlink.
func (f *lwext4FSImpl) resolveLstat(name string) (lwext4.Ino, error) {
	return f.doResolve(name, false, 0)
}

const maxSymlinks = 40

func (f *lwext4FSImpl) doResolve(name string, followLast bool, depth int) (lwext4.Ino, error) {
	if depth > maxSymlinks {
		return 0, fmt.Errorf("%s: too many symlinks", name)
	}

	parts := splitPath(name)
	ino := lwext4.RootIno

	for i, part := range parts {
		child, err := f.ext4.Lookup(ino, part)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}

		// Check if this component is a symlink.
		isLast := i == len(parts)-1
		if !isLast || followLast {
			attr, err := f.ext4.GetAttr(child)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", name, err)
			}
			if attr.Mode&syscall.S_IFMT == syscall.S_IFLNK {
				target, err := f.ext4.Readlink(child)
				if err != nil {
					return 0, fmt.Errorf("%s: readlink: %w", name, err)
				}
				// Build the full remaining path through the symlink.
				var resolvedPath string
				remaining := strings.Join(parts[i+1:], "/")
				if path.IsAbs(target) {
					resolvedPath = target
				} else {
					parentPath := "/" + strings.Join(parts[:i], "/")
					resolvedPath = path.Join(parentPath, target)
				}
				if remaining != "" {
					resolvedPath = resolvedPath + "/" + remaining
				}
				return f.doResolve(resolvedPath, followLast, depth+1)
			}
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
	defer func() {
		if err := file.Close(); err != nil {
			slog.Warn("lwext4 close error", "error", err)
		}
	}()
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
	defer func() {
		if err := file.Close(); err != nil {
			slog.Warn("lwext4 close error", "error", err)
		}
	}()

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

func (f *lwext4FSImpl) Lstat(name string) (fs.FileInfo, error) {
	ino, err := f.resolveLstat(name)
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

func (f *lwext4FSImpl) Symlink(target, name string) error {
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}
	_, err = f.ext4.Symlink(parentIno, baseName, target)
	return err
}

func (f *lwext4FSImpl) Readlink(name string) (string, error) {
	ino, err := f.resolveLstat(name)
	if err != nil {
		return "", err
	}
	return f.ext4.Readlink(ino)
}

func (f *lwext4FSImpl) Chmod(name string, mode fs.FileMode) error {
	ino, err := f.resolve(name)
	if err != nil {
		return err
	}
	// Read current mode to preserve file type bits (S_IFREG, S_IFDIR, etc.).
	attr, err := f.ext4.GetAttr(ino)
	if err != nil {
		return err
	}
	newMode := (attr.Mode & syscall.S_IFMT) | uint32(mode&0o7777)
	return f.ext4.SetAttr(ino, &lwext4.Attr{Mode: newMode}, lwext4.AttrMode)
}

func (f *lwext4FSImpl) Lchown(name string, uid, gid int) error {
	// Use resolveLstat so we don't follow the final symlink.
	ino, err := f.resolveLstat(name)
	if err != nil {
		return err
	}
	return f.ext4.SetAttr(ino, &lwext4.Attr{Uid: uint32(uid), Gid: uint32(gid)}, lwext4.AttrUid|lwext4.AttrGid)
}

func (f *lwext4FSImpl) Chtimes(name string, mtime int64) error {
	ino, err := f.resolve(name)
	if err != nil {
		return err
	}
	return f.ext4.SetAttr(ino, &lwext4.Attr{Mtime: uint32(mtime)}, lwext4.AttrMtime)
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
		if closeErr := file.Close(); closeErr != nil {
			slog.Warn("lwext4 close error", "error", closeErr)
		}
		return nil, err
	}
	return file, nil
}

// lwext4FileInfo implements fs.FileInfo from lwext4 attributes.
type lwext4FileInfo struct {
	name string
	attr *lwext4.Attr
}

func (fi *lwext4FileInfo) Name() string       { return fi.name }
func (fi *lwext4FileInfo) Size() int64        { return int64(fi.attr.Size) }
func (fi *lwext4FileInfo) ModTime() time.Time { return time.Unix(int64(fi.attr.Mtime), 0) }
func (fi *lwext4FileInfo) IsDir() bool        { return fi.attr.Mode&0o40000 != 0 }
func (fi *lwext4FileInfo) Sys() any           { return fi.attr }

func (fi *lwext4FileInfo) Mode() fs.FileMode {
	perm := fs.FileMode(fi.attr.Mode) & 0o7777
	switch fi.attr.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		perm |= fs.ModeDir
	case syscall.S_IFLNK:
		perm |= fs.ModeSymlink
	case syscall.S_IFCHR:
		perm |= fs.ModeDevice | fs.ModeCharDevice
	case syscall.S_IFBLK:
		perm |= fs.ModeDevice
	case syscall.S_IFIFO:
		perm |= fs.ModeNamedPipe
	case syscall.S_IFSOCK:
		perm |= fs.ModeSocket
	}
	if fi.attr.Mode&syscall.S_ISUID != 0 {
		perm |= fs.ModeSetuid
	}
	if fi.attr.Mode&syscall.S_ISGID != 0 {
		perm |= fs.ModeSetgid
	}
	if fi.attr.Mode&syscall.S_ISVTX != 0 {
		perm |= fs.ModeSticky
	}
	return perm
}
