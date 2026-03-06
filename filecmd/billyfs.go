package filecmd

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"syscall"
	"time"

	billy "github.com/go-git/go-billy/v5"

	"github.com/semistrict/loophole/fsbackend"
)

// wrapPathError wraps an error as *os.PathError if it contains a syscall.Errno.
// This is needed because os.IsNotExist (used by go-git) only recognizes
// *os.PathError, *os.SyscallError, and raw syscall.Errno — not arbitrary
// errors wrapping syscall.Errno via fmt.Errorf %w.
func wrapPathError(op, path string, err error) error {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return &os.PathError{Op: op, Path: path, Err: errno}
	}
	return err
}

// billyFS adapts fsbackend.FS to billy.Filesystem.
type billyFS struct {
	fs   fsbackend.FS
	root string // chroot prefix (always starts with "/" or is "")
	tmp  atomic.Int64
}

var _ billy.Filesystem = (*billyFS)(nil)

func newBillyFS(fsys fsbackend.FS, root string) *billyFS {
	return &billyFS{fs: fsys, root: root}
}

func (b *billyFS) abs(name string) string {
	name = path.Clean("/" + name)
	if b.root == "" || b.root == "/" {
		return name
	}
	return path.Join(b.root, name)
}

func (b *billyFS) Create(filename string) (billy.File, error) {
	p := b.abs(filename)
	if dir := path.Dir(p); dir != "/" {
		_ = b.fs.MkdirAll(dir, 0o755)
	}
	_ = b.fs.Remove(p)
	f, err := b.fs.Create(p)
	if err != nil {
		return nil, err
	}
	return &billyFile{f: f, name: filename}, nil
}

func (b *billyFS) Open(filename string) (billy.File, error) {
	f, err := b.fs.Open(b.abs(filename))
	if err != nil {
		return nil, wrapPathError("open", filename, err)
	}
	return &billyFile{f: f, name: filename}, nil
}

func (b *billyFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	p := b.abs(filename)
	if flag&os.O_CREATE != 0 {
		if dir := path.Dir(p); dir != "/" {
			_ = b.fs.MkdirAll(dir, 0o755)
		}
	}
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
		if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
			_ = b.fs.Remove(p)
			f, err := b.fs.Create(p)
			if err != nil {
				return nil, err
			}
			if perm != 0 && perm != 0o644 {
				_ = b.fs.Chmod(p, perm)
			}
			return &billyFile{f: f, name: filename}, nil
		}
	}
	f, err := b.fs.Open(p)
	if err != nil {
		return nil, err
	}
	return &billyFile{f: f, name: filename}, nil
}

func (b *billyFS) Stat(filename string) (os.FileInfo, error) {
	info, err := b.fs.Stat(b.abs(filename))
	if err != nil {
		return nil, wrapPathError("stat", filename, err)
	}
	return info, nil
}

func (b *billyFS) Rename(oldpath, newpath string) error {
	// Read the file, write to new location, remove old.
	data, err := b.fs.ReadFile(b.abs(oldpath))
	if err != nil {
		return err
	}
	if err := b.fs.WriteFile(b.abs(newpath), data, 0o644); err != nil {
		return err
	}
	return b.fs.Remove(b.abs(oldpath))
}

func (b *billyFS) Remove(filename string) error {
	return b.fs.Remove(b.abs(filename))
}

func (b *billyFS) Join(elem ...string) string {
	return path.Join(elem...)
}

// Dir interface

func (b *billyFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	names, err := b.fs.ReadDir(b.abs(dirname))
	if err != nil {
		return nil, wrapPathError("readdir", dirname, err)
	}
	infos := make([]os.FileInfo, 0, len(names))
	for _, name := range names {
		info, err := b.fs.Lstat(path.Join(b.abs(dirname), name))
		if err != nil {
			continue
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (b *billyFS) MkdirAll(filename string, perm os.FileMode) error {
	return b.fs.MkdirAll(b.abs(filename), perm)
}

// TempFile interface

func (b *billyFS) TempFile(dir, prefix string) (billy.File, error) {
	if dir == "" {
		dir = "/"
	}
	n := b.tmp.Add(1)
	name := path.Join(dir, fmt.Sprintf("%s%d", prefix, n))
	return b.Create(name)
}

// Symlink interface

func (b *billyFS) Lstat(filename string) (os.FileInfo, error) {
	info, err := b.fs.Lstat(b.abs(filename))
	if err != nil {
		return nil, wrapPathError("lstat", filename, err)
	}
	return info, nil
}

func (b *billyFS) Symlink(target, link string) error {
	p := b.abs(link)
	if dir := path.Dir(p); dir != "/" {
		_ = b.fs.MkdirAll(dir, 0o755)
	}
	return b.fs.Symlink(target, p)
}

func (b *billyFS) Readlink(link string) (string, error) {
	return b.fs.Readlink(b.abs(link))
}

// Chroot interface

func (b *billyFS) Chroot(chrootPath string) (billy.Filesystem, error) {
	newRoot := b.abs(chrootPath)
	if err := b.fs.MkdirAll(newRoot, 0o755); err != nil {
		return nil, err
	}
	return newBillyFS(b.fs, newRoot), nil
}

func (b *billyFS) Root() string {
	if b.root == "" {
		return "/"
	}
	return b.root
}

// Capabilities

func (b *billyFS) Capabilities() billy.Capability {
	return billy.DefaultCapabilities
}

// billyFile wraps fsbackend.File to implement billy.File.
type billyFile struct {
	f    fsbackend.File
	name string
}

var _ billy.File = (*billyFile)(nil)

func (f *billyFile) Name() string                                 { return f.name }
func (f *billyFile) Read(p []byte) (int, error)                   { return f.f.Read(p) }
func (f *billyFile) ReadAt(p []byte, off int64) (int, error)      { return f.f.ReadAt(p, off) }
func (f *billyFile) Write(p []byte) (int, error)                  { return f.f.Write(p) }
func (f *billyFile) Seek(offset int64, whence int) (int64, error) { return f.f.Seek(offset, whence) }
func (f *billyFile) Close() error                                 { return f.f.Close() }
func (f *billyFile) Truncate(size int64) error                    { return f.f.Truncate(size) }
func (f *billyFile) Lock() error                                  { return nil }
func (f *billyFile) Unlock() error                                { return nil }

// Change interface — lets go-git set file modes/ownership during checkout.
var _ billy.Change = (*billyFS)(nil)

func (b *billyFS) Chmod(name string, mode os.FileMode) error {
	return b.fs.Chmod(b.abs(name), mode)
}

func (b *billyFS) Lchown(name string, uid, gid int) error {
	return b.fs.Lchown(b.abs(name), uid, gid)
}

func (b *billyFS) Chown(name string, uid, gid int) error {
	// Follow symlinks: resolve then lchown the target.
	p := b.abs(name)
	target, err := b.fs.Readlink(p)
	if err == nil {
		// It's a symlink — chown the target.
		if !path.IsAbs(target) {
			target = path.Join(path.Dir(p), target)
		}
		return b.fs.Lchown(target, uid, gid)
	}
	return b.fs.Lchown(p, uid, gid)
}

func (b *billyFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return b.fs.Chtimes(b.abs(name), mtime.Unix())
}
