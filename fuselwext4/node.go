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
	"github.com/semistrict/loophole/metrics"
)

// orphanTracker tracks open file descriptor counts per inode and which inodes
// are orphaned (unlinked while open). Protected by the same mutex as lwext4 ops.
type orphanTracker struct {
	openFDs  map[lwext4.Ino]int
	orphaned map[lwext4.Ino]bool
}

func newOrphanTracker() *orphanTracker {
	return &orphanTracker{
		openFDs:  make(map[lwext4.Ino]int),
		orphaned: make(map[lwext4.Ino]bool),
	}
}

// unlink performs an orphan-aware unlink. If the child has open FDs and this
// is the last link, it uses the orphan path (deferring inode free to Release).
// Otherwise it uses regular unlink (immediate free). Caller must hold mu.
func (ot *orphanTracker) unlink(ext4fs *lwext4.FS, parentIno lwext4.Ino, name string) error {
	childIno, err := ext4fs.Lookup(parentIno, name)
	if err != nil {
		return err
	}
	if ot.openFDs[childIno] > 0 {
		attr, err := ext4fs.GetAttr(childIno)
		if err == nil && attr.Links <= 1 {
			_, err = ext4fs.UnlinkOrphan(parentIno, name)
			if err == nil {
				ot.orphaned[childIno] = true
			}
			return err
		}
	}
	return ext4fs.Unlink(parentIno, name)
}

// openFD increments the open file descriptor count for an inode. Caller must hold mu.
func (ot *orphanTracker) openFD(ino lwext4.Ino) {
	ot.openFDs[ino]++
}

// closeFD decrements the open fd count and frees the orphan if this was the
// last handle. Caller must hold mu.
func (ot *orphanTracker) closeFD(ext4fs *lwext4.FS, ino lwext4.Ino) {
	ot.openFDs[ino]--
	if ot.openFDs[ino] <= 0 {
		delete(ot.openFDs, ino)
		if ot.orphaned[ino] {
			delete(ot.orphaned, ino)
			if err := ext4fs.FreeOrphan(ino); err != nil {
				slog.Error("failed to free orphan inode", "ino", ino, "error", err)
			}
		}
	}
}

// ext4Node implements go-fuse's InodeEmbedder, delegating to lwext4.
// All lwext4 calls are serialized through mu since the C library is not thread-safe.
type ext4Node struct {
	fs.Inode
	ext4fs  *lwext4.FS
	mu      *sync.Mutex // shared across all nodes for this FS
	ino     lwext4.Ino
	orphans *orphanTracker // shared across all nodes for this FS
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
	return &ext4Node{ext4fs: n.ext4fs, mu: n.mu, ino: ino, orphans: n.orphans}
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
	done := metrics.FuseOp("lookup")
	n.mu.Lock()
	childIno, err := n.ext4fs.Lookup(n.ino, name)
	if err != nil {
		n.mu.Unlock()
		done(syscall.ENOENT)
		return nil, syscall.ENOENT
	}
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode & syscall.S_IFMT, Ino: uint64(childIno)}
	done(0)
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	done := metrics.FuseOp("getattr")
	n.mu.Lock()
	attr, err := n.ext4fs.GetAttr(n.ino)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return errno
	}
	attrFromLwext4(attr, &out.Attr)
	done(0)
	return 0
}

func (n *ext4Node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	done := metrics.FuseOp("setattr")
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
			errno := errToStatus(err)
			done(errno)
			return errno
		}
		if err := f.Truncate(int64(size)); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Warn("fuse truncate close error", "error", closeErr)
			}
			n.mu.Unlock()
			errno := errToStatus(err)
			done(errno)
			return errno
		}
		if err := f.Close(); err != nil {
			slog.Warn("fuse truncate close error", "error", err)
		}
	}

	if mask != 0 {
		if err := n.ext4fs.SetAttr(n.ino, a, mask); err != nil {
			n.mu.Unlock()
			errno := errToStatus(err)
			done(errno)
			return errno
		}
	}

	attr, err := n.ext4fs.GetAttr(n.ino)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return errno
	}
	attrFromLwext4(attr, &out.Attr)
	done(0)
	return 0
}

func (n *ext4Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	done := metrics.FuseOp("mkdir")
	n.mu.Lock()
	childIno, err := n.ext4fs.Mkdir(n.ino, name, mode|syscall.S_IFDIR)
	if err != nil {
		n.mu.Unlock()
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	n.setOwner(ctx, childIno)
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode & syscall.S_IFMT, Ino: uint64(childIno)}
	done(0)
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	done := metrics.FuseOp("create")
	n.mu.Lock()
	childIno, err := n.ext4fs.Mknod(n.ino, name, mode|syscall.S_IFREG)
	if err != nil {
		n.mu.Unlock()
		e := errToStatus(err)
		done(e)
		return nil, nil, 0, e
	}
	n.setOwner(ctx, childIno)
	f, err := n.ext4fs.OpenFile(childIno, syscall.O_RDWR)
	if err != nil {
		n.mu.Unlock()
		e := errToStatus(err)
		done(e)
		return nil, nil, 0, e
	}
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		e := errToStatus(err)
		done(e)
		return nil, nil, 0, e
	}
	n.orphans.openFD(childIno)
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode & syscall.S_IFMT, Ino: uint64(childIno)}
	done(0)
	return n.NewInode(ctx, child, stable), &ext4FileHandle{f: f, mu: n.mu, ino: childIno, ext4fs: n.ext4fs, orphans: n.orphans}, 0, 0
}

func (n *ext4Node) Unlink(ctx context.Context, name string) syscall.Errno {
	done := metrics.FuseOp("unlink")
	n.mu.Lock()
	err := n.orphans.unlink(n.ext4fs, n.ino, name)
	n.mu.Unlock()
	errno := errToStatus(err)
	done(errno)
	return errno
}

func (n *ext4Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	done := metrics.FuseOp("rmdir")
	n.mu.Lock()
	err := n.ext4fs.Rmdir(n.ino, name)
	n.mu.Unlock()
	errno := errToStatus(err)
	done(errno)
	return errno
}

func (n *ext4Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	done := metrics.FuseOp("rename")
	np := newParent.(*ext4Node)
	n.mu.Lock()
	err := n.ext4fs.Rename(n.ino, name, np.ino, newName)
	n.mu.Unlock()
	errno := errToStatus(err)
	done(errno)
	return errno
}

func (n *ext4Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	done := metrics.FuseOp("link")
	t := target.(*ext4Node)
	n.mu.Lock()
	err := n.ext4fs.Link(t.ino, n.ino, name)
	if err != nil {
		n.mu.Unlock()
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	attr, err := n.ext4fs.GetAttr(t.ino)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(t.ino)
	stable := fs.StableAttr{Mode: attr.Mode & syscall.S_IFMT, Ino: uint64(t.ino)}
	done(0)
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	done := metrics.FuseOp("symlink")
	n.mu.Lock()
	childIno, err := n.ext4fs.Symlink(n.ino, name, target)
	if err != nil {
		n.mu.Unlock()
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	n.setOwner(ctx, childIno)
	attr, err := n.ext4fs.GetAttr(childIno)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	attrFromLwext4(attr, &out.Attr)
	child := n.child(childIno)
	stable := fs.StableAttr{Mode: attr.Mode & syscall.S_IFMT, Ino: uint64(childIno)}
	done(0)
	return n.NewInode(ctx, child, stable), 0
}

func (n *ext4Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	done := metrics.FuseOp("readlink")
	n.mu.Lock()
	target, err := n.ext4fs.Readlink(n.ino)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}
	done(0)
	return []byte(target), 0
}

func (n *ext4Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	done := metrics.FuseOp("readdir")
	n.mu.Lock()
	entries, err := n.ext4fs.Readdir(n.ino)
	n.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return nil, errno
	}

	result := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, fuse.DirEntry{
			Name: e.Name,
			Ino:  uint64(e.Inode),
			Mode: deTypeToMode(e.Type),
		})
	}
	done(0)
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
	done := metrics.FuseOp("open")
	n.mu.Lock()
	f, err := n.ext4fs.OpenFile(n.ino, int(flags)&syscall.O_ACCMODE)
	if err != nil {
		n.mu.Unlock()
		errno := errToStatus(err)
		done(errno)
		return nil, 0, errno
	}
	n.orphans.openFD(n.ino)
	n.mu.Unlock()
	done(0)
	return &ext4FileHandle{f: f, mu: n.mu, ino: n.ino, ext4fs: n.ext4fs, orphans: n.orphans}, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *ext4Node) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	done := metrics.FuseOp("fsync")
	n.mu.Lock()
	err := n.ext4fs.CacheFlush()
	n.mu.Unlock()
	errno := errToStatus(err)
	done(errno)
	return errno
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
