package fuselwext4

import (
	"context"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/lwext4"
)

// ext4Node implements go-fuse's InodeEmbedder, delegating to lwext4.
// All lwext4 calls are serialized through mu since the C library is not thread-safe.
type ext4Node struct {
	fs.Inode
	ext4fs *lwext4.FS
	mu     *sync.Mutex // shared across all nodes for this FS
	ino    lwext4.Ino
}

var (
	_ fs.NodeLookuper   = (*ext4Node)(nil)
	_ fs.NodeGetattrer  = (*ext4Node)(nil)
	_ fs.NodeSetattrer  = (*ext4Node)(nil)
	_ fs.NodeMkdirer    = (*ext4Node)(nil)
	_ fs.NodeCreater    = (*ext4Node)(nil)
	_ fs.NodeUnlinker   = (*ext4Node)(nil)
	_ fs.NodeRmdirer    = (*ext4Node)(nil)
	_ fs.NodeRenamer    = (*ext4Node)(nil)
	_ fs.NodeLinker     = (*ext4Node)(nil)
	_ fs.NodeSymlinker  = (*ext4Node)(nil)
	_ fs.NodeReadlinker = (*ext4Node)(nil)
	_ fs.NodeReaddirer  = (*ext4Node)(nil)
	_ fs.NodeOpener     = (*ext4Node)(nil)
	// Note: NodeOpendirHandler with FOPEN_CACHE_DIR is not safe here because
	// we don't send cache invalidation notifications when directory contents change.
	_ fs.NodeFsyncer = (*ext4Node)(nil)
)

func (n *ext4Node) child(ino lwext4.Ino) *ext4Node {
	return &ext4Node{ext4fs: n.ext4fs, mu: n.mu, ino: ino}
}

func attrFromLwext4(a *lwext4.Attr, out *fuse.Attr) {
	out.Ino = uint64(a.Ino)
	out.Size = a.Size
	out.Mode = a.Mode
	out.Nlink = uint32(a.Links)
	out.Uid = a.Uid
	out.Gid = a.Gid
	out.Atime = uint64(a.Atime)
	out.Mtime = uint64(a.Mtime)
	out.Ctime = uint64(a.Ctime)
	out.Blocks = (a.Size + 511) / 512
	out.Blksize = 4096
}

// setOwner sets the uid/gid of ino to the FUSE caller's identity.
// Must be called with n.mu held.
func (n *ext4Node) setOwner(ctx context.Context, ino lwext4.Ino) {
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return
	}
	_ = n.ext4fs.SetAttr(ino, &lwext4.Attr{
		Uid: caller.Uid,
		Gid: caller.Gid,
	}, lwext4.AttrUid|lwext4.AttrGid)
}

func errToStatus(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	slog.Error("lwext4 FUSE error", "error", err)
	// lwext4 errors are formatted as "lwext4: op: <errno>".
	// For now, map all errors to EIO. We could parse the errno if needed.
	return syscall.EIO
}

func (n *ext4Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	childIno, err := n.ext4fs.Lookup(n.ino, name)
	if err != nil {
		n.mu.Unlock()
		return nil, syscall.ENOENT
	}
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode, Ino: uint64(childIno)}
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	attr, err := n.ext4fs.GetAttr(n.ino)
	n.mu.Unlock()
	if err != nil {
		return errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	return 0
}

func (n *ext4Node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var mask uint32
	a := &lwext4.Attr{}

	if m, ok := in.GetMode(); ok {
		a.Mode = m
		mask |= lwext4.AttrMode
	}
	if uid, ok := in.GetUID(); ok {
		a.Uid = uid
		mask |= lwext4.AttrUid
	}
	if gid, ok := in.GetGID(); ok {
		a.Gid = gid
		mask |= lwext4.AttrGid
	}
	if atime, ok := in.GetATime(); ok {
		a.Atime = uint32(atime.Unix())
		mask |= lwext4.AttrAtime
	}
	if mtime, ok := in.GetMTime(); ok {
		a.Mtime = uint32(mtime.Unix())
		mask |= lwext4.AttrMtime
	}

	n.mu.Lock()
	if size, ok := in.GetSize(); ok {
		// Truncate via file open/truncate/close.
		f, err := n.ext4fs.OpenFile(n.ino, syscall.O_WRONLY)
		if err != nil {
			n.mu.Unlock()
			return errToStatus(err)
		}
		if err := f.Truncate(int64(size)); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Warn("fuse truncate close error", "error", closeErr)
			}
			n.mu.Unlock()
			return errToStatus(err)
		}
		if err := f.Close(); err != nil {
			slog.Warn("fuse truncate close error", "error", err)
		}
	}

	if mask != 0 {
		if err := n.ext4fs.SetAttr(n.ino, a, mask); err != nil {
			n.mu.Unlock()
			return errToStatus(err)
		}
	}

	attr, err := n.ext4fs.GetAttr(n.ino)
	n.mu.Unlock()
	if err != nil {
		return errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	return 0
}

func (n *ext4Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	childIno, err := n.ext4fs.Mkdir(n.ino, name, mode|syscall.S_IFDIR)
	if err != nil {
		n.mu.Unlock()
		return nil, errToStatus(err)
	}
	n.setOwner(ctx, childIno)
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode, Ino: uint64(childIno)}
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.mu.Lock()
	childIno, err := n.ext4fs.Mknod(n.ino, name, mode|syscall.S_IFREG)
	if err != nil {
		n.mu.Unlock()
		return nil, nil, 0, errToStatus(err)
	}
	n.setOwner(ctx, childIno)
	f, err := n.ext4fs.OpenFile(childIno, syscall.O_RDWR)
	if err != nil {
		n.mu.Unlock()
		return nil, nil, 0, errToStatus(err)
	}
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		return nil, nil, 0, errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode, Ino: uint64(childIno)}
	return n.NewInode(ctx, child, stable), &ext4FileHandle{f: f, mu: n.mu}, 0, 0
}

func (n *ext4Node) Unlink(ctx context.Context, name string) syscall.Errno {
	n.mu.Lock()
	err := n.ext4fs.Unlink(n.ino, name)
	n.mu.Unlock()
	return errToStatus(err)
}

func (n *ext4Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	n.mu.Lock()
	err := n.ext4fs.Rmdir(n.ino, name)
	n.mu.Unlock()
	return errToStatus(err)
}

func (n *ext4Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	np := newParent.(*ext4Node)
	n.mu.Lock()
	err := n.ext4fs.Rename(n.ino, name, np.ino, newName)
	n.mu.Unlock()
	return errToStatus(err)
}

func (n *ext4Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	t := target.(*ext4Node)
	n.mu.Lock()
	err := n.ext4fs.Link(t.ino, n.ino, name)
	if err != nil {
		n.mu.Unlock()
		return nil, errToStatus(err)
	}
	attr, err := n.ext4fs.GetAttr(t.ino)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(t.ino)
	stable := fs.StableAttr{Mode: attr.Mode, Ino: uint64(t.ino)}
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	childIno, err := n.ext4fs.Symlink(n.ino, name, target)
	if err != nil {
		n.mu.Unlock()
		return nil, errToStatus(err)
	}
	n.setOwner(ctx, childIno)
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode, Ino: uint64(childIno)}
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n.mu.Lock()
	target, err := n.ext4fs.Readlink(n.ino)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}
	return []byte(target), 0
}

func (n *ext4Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.mu.Lock()
	entries, err := n.ext4fs.Readdir(n.ino)
	n.mu.Unlock()
	if err != nil {
		return nil, errToStatus(err)
	}

	result := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, fuse.DirEntry{
			Name: e.Name,
			Ino:  uint64(e.Inode),
			Mode: deTypeToMode(e.Type),
		})
	}
	return fs.NewListDirStream(result), 0
}

func deTypeToMode(t uint8) uint32 {
	switch t {
	case lwext4.TypeDir:
		return syscall.S_IFDIR
	case lwext4.TypeRegFile:
		return syscall.S_IFREG
	case lwext4.TypeSymlink:
		return syscall.S_IFLNK
	case lwext4.TypeChrdev:
		return syscall.S_IFCHR
	case lwext4.TypeBlkdev:
		return syscall.S_IFBLK
	case lwext4.TypeFIFO:
		return syscall.S_IFIFO
	case lwext4.TypeSock:
		return syscall.S_IFSOCK
	default:
		return 0
	}
}

func (n *ext4Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	f, err := n.ext4fs.OpenFile(n.ino, int(flags)&syscall.O_ACCMODE)
	n.mu.Unlock()
	if err != nil {
		return nil, 0, errToStatus(err)
	}
	return &ext4FileHandle{f: f, mu: n.mu}, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *ext4Node) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	n.mu.Lock()
	err := n.ext4fs.CacheFlush()
	n.mu.Unlock()
	return errToStatus(err)
}

// Statfs returns filesystem statistics.
func (n *ext4Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Return reasonable defaults. lwext4 doesn't expose statfs directly.
	out.Bsize = 4096
	out.Blocks = 100 * 1024 * 1024 * 1024 / 4096 // 100 GB
	out.Bfree = out.Blocks / 2
	out.Bavail = out.Bfree
	out.NameLen = 255
	return 0
}

var _ = time.Second // used by attrFromLwext4 timestamp conversion
