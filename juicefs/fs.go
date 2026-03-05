package juicefs

import (
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/semistrict/loophole/fsbackend"
)

const maxSymlinks = 40

// juiceFS implements fsbackend.FS by wrapping JuiceFS VFS calls directly.
type juiceFS struct {
	v *vfs.VFS
}

var _ fsbackend.FS = (*juiceFS)(nil)

func newJuiceFS(v *vfs.VFS) *juiceFS {
	return &juiceFS{v: v}
}

// --- path resolution ---

func splitPath(name string) []string {
	name = path.Clean("/" + name)
	if name == "/" {
		return nil
	}
	return strings.Split(name[1:], "/")
}

// resolve walks from root to the named path, following all symlinks.
func (f *juiceFS) resolve(name string) (meta.Ino, error) {
	return f.doResolve(name, true, 0)
}

// resolveLstat is like resolve but does not follow the final symlink.
func (f *juiceFS) resolveLstat(name string) (meta.Ino, error) {
	return f.doResolve(name, false, 0)
}

func (f *juiceFS) doResolve(name string, followLast bool, depth int) (meta.Ino, error) {
	if depth > maxSymlinks {
		return 0, fmt.Errorf("%s: too many symlinks", name)
	}

	parts := splitPath(name)
	var ino meta.Ino = 1 // root inode
	ctx := vfsCtx()

	for i, part := range parts {
		entry, errno := f.v.Lookup(ctx, ino, part)
		if errno != 0 {
			return 0, &fs.PathError{Op: "lookup", Path: name, Err: errno}
		}

		isLast := i == len(parts)-1
		if !isLast || followLast {
			if entry.Attr.Typ == meta.TypeSymlink {
				pathBytes, errno := f.v.Readlink(ctx, entry.Inode)
				if errno != 0 {
					return 0, fmt.Errorf("%s: readlink: %w", name, errno)
				}
				target := string(pathBytes)
				remaining := strings.Join(parts[i+1:], "/")
				var resolvedPath string
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

		ino = entry.Inode
	}
	return ino, nil
}

func (f *juiceFS) resolveParent(name string) (meta.Ino, string, error) {
	parts := splitPath(name)
	if len(parts) == 0 {
		return 0, "", fmt.Errorf("cannot resolve parent of root")
	}
	parentPath := "/" + strings.Join(parts[:len(parts)-1], "/")
	parentIno, err := f.resolve(parentPath)
	if err != nil {
		return 0, "", err
	}
	return parentIno, parts[len(parts)-1], nil
}

// --- FS interface implementation ---

func (f *juiceFS) ReadFile(name string) ([]byte, error) {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}

	_, fh, errno := f.v.Open(ctx, ino, syscall.O_RDONLY)
	if errno != 0 {
		return nil, errnoErr(errno)
	}
	defer f.v.Release(ctx, ino, fh)

	entry, errno := f.v.GetAttr(ctx, ino, 1)
	if errno != 0 {
		return nil, errnoErr(errno)
	}

	size := entry.Attr.Length
	if size == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, size)
	n, errno := f.v.Read(ctx, ino, buf, 0, fh)
	if errno != 0 {
		return nil, errnoErr(errno)
	}
	return buf[:n], nil
}

func (f *juiceFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	ctx := vfsCtx()
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}

	entry, errno := f.v.Lookup(ctx, parentIno, baseName)
	var ino meta.Ino
	var fh uint64
	if errno != 0 {
		entry, fh, errno = f.v.Create(ctx, parentIno, baseName, uint16(perm&0o7777), 0, syscall.O_WRONLY|syscall.O_TRUNC)
		if errno != 0 {
			return errnoErr(errno)
		}
		ino = entry.Inode
	} else {
		ino = entry.Inode
		_, fh, errno = f.v.Open(ctx, ino, syscall.O_WRONLY)
		if errno != 0 {
			return errnoErr(errno)
		}
		errno = f.v.Truncate(ctx, ino, 0, fh, &meta.Attr{})
		if errno != 0 {
			f.v.Release(ctx, ino, fh)
			return errnoErr(errno)
		}
	}
	defer f.v.Release(ctx, ino, fh)

	if len(data) > 0 {
		errno = f.v.Write(ctx, ino, data, 0, fh)
		if errno != 0 {
			return errnoErr(errno)
		}
	}

	errno = f.v.Flush(ctx, ino, fh, 0)
	return errnoErr(errno)
}

func (f *juiceFS) MkdirAll(name string, perm fs.FileMode) error {
	ctx := vfsCtx()
	parts := splitPath(name)
	var ino meta.Ino = 1

	for _, part := range parts {
		entry, errno := f.v.Lookup(ctx, ino, part)
		if errno != 0 {
			entry, errno = f.v.Mkdir(ctx, ino, part, uint16(perm&0o7777), 0)
			if errno != 0 {
				return errnoErr(errno)
			}
		}
		ino = entry.Inode
	}
	return nil
}

func (f *juiceFS) Remove(name string) error {
	ctx := vfsCtx()
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}

	entry, errno := f.v.Lookup(ctx, parentIno, baseName)
	if errno != 0 {
		return errnoErr(errno)
	}

	if entry.Attr.Typ == meta.TypeDirectory {
		return errnoErr(f.v.Rmdir(ctx, parentIno, baseName))
	}
	return errnoErr(f.v.Unlink(ctx, parentIno, baseName))
}

func (f *juiceFS) Stat(name string) (fs.FileInfo, error) {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	entry, errno := f.v.GetAttr(ctx, ino, 0)
	if errno != 0 {
		return nil, errnoErr(errno)
	}

	parts := splitPath(name)
	baseName := "/"
	if len(parts) > 0 {
		baseName = parts[len(parts)-1]
	}
	return &jfsFileInfo{name: baseName, attr: entry.Attr}, nil
}

func (f *juiceFS) Lstat(name string) (fs.FileInfo, error) {
	ctx := vfsCtx()
	ino, err := f.resolveLstat(name)
	if err != nil {
		return nil, err
	}
	entry, errno := f.v.GetAttr(ctx, ino, 0)
	if errno != 0 {
		return nil, errnoErr(errno)
	}

	parts := splitPath(name)
	baseName := "/"
	if len(parts) > 0 {
		baseName = parts[len(parts)-1]
	}
	return &jfsFileInfo{name: baseName, attr: entry.Attr}, nil
}

func (f *juiceFS) ReadDir(name string) ([]string, error) {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}

	fh, errno := f.v.Opendir(ctx, ino, 0)
	if errno != 0 {
		return nil, errnoErr(errno)
	}
	defer f.v.Releasedir(ctx, ino, fh)

	entries, _, errno := f.v.Readdir(ctx, ino, 0, 0, fh, false)
	if errno != 0 {
		return nil, errnoErr(errno)
	}

	var names []string
	for _, e := range entries {
		n := string(e.Name)
		if n == "." || n == ".." {
			continue
		}
		names = append(names, n)
	}
	return names, nil
}

func (f *juiceFS) Open(name string) (fsbackend.File, error) {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return nil, err
	}
	_, fh, errno := f.v.Open(ctx, ino, syscall.O_RDONLY)
	if errno != 0 {
		return nil, errnoErr(errno)
	}
	return &jfsFile{v: f.v, ino: ino, fh: fh, off: 0}, nil
}

func (f *juiceFS) Create(name string) (fsbackend.File, error) {
	ctx := vfsCtx()
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return nil, err
	}

	entry, errno := f.v.Lookup(ctx, parentIno, baseName)
	var ino meta.Ino
	var fh uint64
	if errno != 0 {
		entry, fh, errno = f.v.Create(ctx, parentIno, baseName, 0o644, 0, syscall.O_RDWR|syscall.O_TRUNC)
		if errno != 0 {
			return nil, errnoErr(errno)
		}
		ino = entry.Inode
	} else {
		ino = entry.Inode
		_, fh, errno = f.v.Open(ctx, ino, syscall.O_RDWR)
		if errno != 0 {
			return nil, errnoErr(errno)
		}
		errno = f.v.Truncate(ctx, ino, 0, fh, &meta.Attr{})
		if errno != 0 {
			f.v.Release(ctx, ino, fh)
			return nil, errnoErr(errno)
		}
	}
	return &jfsFile{v: f.v, ino: ino, fh: fh, off: 0}, nil
}

func (f *juiceFS) Symlink(target, name string) error {
	ctx := vfsCtx()
	parentIno, baseName, err := f.resolveParent(name)
	if err != nil {
		return err
	}
	_, errno := f.v.Symlink(ctx, target, parentIno, baseName)
	return errnoErr(errno)
}

func (f *juiceFS) Readlink(name string) (string, error) {
	ctx := vfsCtx()
	ino, err := f.resolveLstat(name)
	if err != nil {
		return "", err
	}
	pathBytes, errno := f.v.Readlink(ctx, ino)
	if errno != 0 {
		return "", errnoErr(errno)
	}
	return string(pathBytes), nil
}

func (f *juiceFS) Chmod(name string, mode fs.FileMode) error {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return err
	}
	_, errno := f.v.SetAttr(ctx, ino, meta.SetAttrMode, 0, uint32(mode&0o7777), 0, 0, 0, 0, 0, 0, 0)
	return errnoErr(errno)
}

func (f *juiceFS) Lchown(name string, uid, gid int) error {
	ctx := vfsCtx()
	ino, err := f.resolveLstat(name)
	if err != nil {
		return err
	}
	set := 0
	if uid >= 0 {
		set |= meta.SetAttrUID
	}
	if gid >= 0 {
		set |= meta.SetAttrGID
	}
	_, errno := f.v.SetAttr(ctx, ino, set, 0, 0, uint32(uid), uint32(gid), 0, 0, 0, 0, 0)
	return errnoErr(errno)
}

func (f *juiceFS) Chtimes(name string, mtime int64) error {
	ctx := vfsCtx()
	ino, err := f.resolve(name)
	if err != nil {
		return err
	}
	_, errno := f.v.SetAttr(ctx, ino, meta.SetAttrMtime, 0, 0, 0, 0, 0, mtime, 0, 0, 0)
	return errnoErr(errno)
}

// --- jfsFile implements fsbackend.File ---

type jfsFile struct {
	v   *vfs.VFS
	ino meta.Ino
	fh  uint64
	off uint64
}

var _ fsbackend.File = (*jfsFile)(nil)

func (f *jfsFile) Read(p []byte) (int, error) {
	ctx := vfsCtx()
	n, errno := f.v.Read(ctx, f.ino, p, f.off, f.fh)
	if errno != 0 {
		return 0, errnoErr(errno)
	}
	if n == 0 {
		return 0, io.EOF
	}
	f.off += uint64(n)
	return n, nil
}

func (f *jfsFile) Write(p []byte) (int, error) {
	ctx := vfsCtx()
	errno := f.v.Write(ctx, f.ino, p, f.off, f.fh)
	if errno != 0 {
		return 0, errnoErr(errno)
	}
	f.off += uint64(len(p))
	return len(p), nil
}

func (f *jfsFile) ReadAt(p []byte, off int64) (int, error) {
	ctx := vfsCtx()
	n, errno := f.v.Read(ctx, f.ino, p, uint64(off), f.fh)
	if errno != 0 {
		return 0, errnoErr(errno)
	}
	if n == 0 {
		return 0, io.EOF
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *jfsFile) WriteAt(p []byte, off int64) (int, error) {
	ctx := vfsCtx()
	errno := f.v.Write(ctx, f.ino, p, uint64(off), f.fh)
	if errno != 0 {
		return 0, errnoErr(errno)
	}
	return len(p), nil
}

func (f *jfsFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.off = uint64(offset)
	case io.SeekCurrent:
		f.off = uint64(int64(f.off) + offset)
	case io.SeekEnd:
		ctx := vfsCtx()
		entry, errno := f.v.GetAttr(ctx, f.ino, 0)
		if errno != 0 {
			return 0, errnoErr(errno)
		}
		f.off = uint64(int64(entry.Attr.Length) + offset)
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	return int64(f.off), nil
}

func (f *jfsFile) Truncate(size int64) error {
	ctx := vfsCtx()
	errno := f.v.Truncate(ctx, f.ino, size, f.fh, &meta.Attr{})
	return errnoErr(errno)
}

func (f *jfsFile) Sync() error {
	ctx := vfsCtx()
	errno := f.v.Fsync(ctx, f.ino, 1, f.fh)
	return errnoErr(errno)
}

func (f *jfsFile) Close() error {
	ctx := vfsCtx()
	_ = errnoErr(f.v.Flush(ctx, f.ino, f.fh, 0))
	f.v.Release(ctx, f.ino, f.fh)
	return nil
}

// --- jfsFileInfo implements fs.FileInfo ---

type jfsFileInfo struct {
	name string
	attr *meta.Attr
}

func (fi *jfsFileInfo) Name() string { return fi.name }
func (fi *jfsFileInfo) Size() int64  { return int64(fi.attr.Length) }
func (fi *jfsFileInfo) ModTime() time.Time {
	return time.Unix(fi.attr.Mtime, int64(fi.attr.Mtimensec))
}
func (fi *jfsFileInfo) IsDir() bool { return fi.attr.Typ == meta.TypeDirectory }
func (fi *jfsFileInfo) Sys() any    { return fi.attr }

func (fi *jfsFileInfo) Mode() fs.FileMode {
	perm := fs.FileMode(fi.attr.Mode) & 0o7777
	switch fi.attr.Typ {
	case meta.TypeDirectory:
		perm |= fs.ModeDir
	case meta.TypeSymlink:
		perm |= fs.ModeSymlink
	case meta.TypeFIFO:
		perm |= fs.ModeNamedPipe
	case meta.TypeBlockDev:
		perm |= fs.ModeDevice
	case meta.TypeCharDev:
		perm |= fs.ModeDevice | fs.ModeCharDevice
	case meta.TypeSocket:
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

// bgCtx is a reusable root-credentials meta context.
// Creating a fresh context per VFS call is expensive (~30% CPU from
// context.WithCancel + time.Now + prometheus access logging).
var bgCtx = meta.Background()

// vfsCtx returns a VFS context for in-process operations.
func vfsCtx() vfs.Context {
	return vfs.NewLogContext(bgCtx)
}
