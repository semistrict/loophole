// Package fuseblockdev implements a FUSE filesystem that exposes loophole volumes
// as device files. Each volume appears as a regular file that can be attached
// to a loop device and formatted with ext4.
package fuseblockdev

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole"
)

const defaultVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB

// Server manages the internal FUSE mount that exposes volume device files.
type Server struct {
	server   *fuse.Server
	MountDir string
	root     *rootNode
}

// Options configures the FUSE mount.
type Options struct {
	// VolumeSize is the apparent size of each device file in bytes.
	// Default: 100 GB.
	VolumeSize uint64

	// Debug enables FUSE debug logging.
	Debug bool
}

// Start mounts a FUSE filesystem at mountDir exposing device files
// for volumes managed by vm.
func Start(mountDir string, vm *loophole.VolumeManager, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}
	volumeSize := opts.VolumeSize
	if volumeSize == 0 {
		volumeSize = defaultVolumeSize
	}

	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("create fuse mount dir: %w", err)
	}

	root := &rootNode{vm: vm, volumeSize: volumeSize}

	cacheTTL := 5 * time.Second
	negTTL := time.Second
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        "loophole",
			Name:          "loophole",
			DisableXAttrs: true,
			MaxWrite:      1024 * 1024,
			MaxBackground: 128,
			DirectMount:   true,
			Debug:         opts.Debug,
		},
		EntryTimeout:    &cacheTTL,
		AttrTimeout:     &cacheTTL,
		NegativeTimeout: &negTTL,
		UID:          uint32(os.Getuid()),
		GID:          uint32(os.Getgid()),
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	return &Server{
		server:   server,
		MountDir: mountDir,
		root:     root,
	}, nil
}

// Wait blocks until the FUSE server is unmounted.
func (s *Server) Wait() {
	s.server.Wait()
}

// Unmount unmounts the FUSE filesystem.
func (s *Server) Unmount() error {
	return s.server.Unmount()
}

// UnmountStale attempts to unmount a stale FUSE mount at dir left over from
// a previous process (e.g. after a crash). It is safe to call if dir is not
// mounted or does not exist.
func UnmountStale(dir string) {
	if err := exec.Command("fusermount", "-u", dir).Run(); err != nil {
		slog.Debug("fusermount stale cleanup", "dir", dir, "error", err)
	}
}

// DevicePath returns the path to the device file for a volume.
func (s *Server) DevicePath(volumeName string) string {
	return s.MountDir + "/" + volumeName
}

// --- root node: flat directory of volume files ---

type rootNode struct {
	fs.Inode
	vm         *loophole.VolumeManager
	volumeSize uint64
}

var _ = (fs.NodeLookuper)((*rootNode)(nil))
var _ = (fs.NodeCreater)((*rootNode)(nil))
var _ = (fs.NodeReaddirer)((*rootNode)(nil))
var _ = (fs.NodeGetattrer)((*rootNode)(nil))

func (r *rootNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o755
	out.Nlink = 2
	return fs.OK
}

func (r *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	vol := r.vm.GetVolume(name)
	if vol == nil {
		return nil, syscall.ENOENT
	}
	if err := vol.AcquireRef(); err != nil {
		return nil, syscall.EIO
	}
	devNode := &deviceNode{vol: vol, volumeSize: r.volumeSize}
	stable := fs.StableAttr{Mode: syscall.S_IFREG}
	child := r.NewInode(ctx, devNode, stable)
	devNode.fillAttr(&out.Attr)
	return child, fs.OK
}

func (r *rootNode) Create(ctx context.Context, name string, _ uint32, _ uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	vol, err := r.vm.NewVolume(ctx, name)
	if err != nil {
		return nil, nil, 0, syscall.EEXIST
	}
	if err := vol.AcquireRef(); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	devNode := &deviceNode{vol: vol, volumeSize: r.volumeSize}
	stable := fs.StableAttr{Mode: syscall.S_IFREG}
	child := r.NewInode(ctx, devNode, stable)
	devNode.fillAttr(&out.Attr)
	if err := vol.AcquireRef(); err != nil {
		_ = vol.ReleaseRef(ctx)
		return nil, nil, 0, syscall.EIO
	}
	return child, &deviceHandle{vol: vol}, fuse.FOPEN_DIRECT_IO, fs.OK
}

func (r *rootNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	names := r.vm.Volumes()
	entries := make([]fuse.DirEntry, len(names))
	for i, name := range names {
		entries[i] = fuse.DirEntry{
			Mode: syscall.S_IFREG,
			Name: name,
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

// --- device file node ---

type deviceNode struct {
	fs.Inode
	vol        *loophole.Volume
	volumeSize uint64
	nodeOnce   sync.Once
}

type deviceHandle struct {
	vol  *loophole.Volume
	once sync.Once
}

func (h *deviceHandle) release(ctx context.Context) error {
	var err error
	h.once.Do(func() {
		err = h.vol.ReleaseRef(ctx)
	})
	return err
}

var _ = (fs.NodeGetattrer)((*deviceNode)(nil))
var _ = (fs.NodeSetattrer)((*deviceNode)(nil))
var _ = (fs.NodeOpener)((*deviceNode)(nil))
var _ = (fs.NodeReader)((*deviceNode)(nil))
var _ = (fs.NodeWriter)((*deviceNode)(nil))
var _ = (fs.NodeFsyncer)((*deviceNode)(nil))
var _ = (fs.NodeFlusher)((*deviceNode)(nil))
var _ = (fs.NodeReleaser)((*deviceNode)(nil))
var _ = (fs.NodeOnForgetter)((*deviceNode)(nil))
var _ = (fs.NodeAllocater)((*deviceNode)(nil))
var _ = (fs.NodeCopyFileRanger)((*deviceNode)(nil))

func (d *deviceNode) fillAttr(out *fuse.Attr) {
	if d.vol.ReadOnly() {
		out.Mode = syscall.S_IFREG | 0o400
	} else {
		out.Mode = syscall.S_IFREG | 0o600
	}
	out.Size = d.volumeSize
	out.Blocks = d.volumeSize / 512
	out.Nlink = 1
}

func (d *deviceNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	d.fillAttr(&out.Attr)
	return fs.OK
}

func (d *deviceNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		writable := mode&0o200 != 0
		if !writable && !d.vol.ReadOnly() {
			if err := d.vol.Freeze(ctx); err != nil {
				return syscall.EIO
			}
		}
	}
	return d.Getattr(ctx, nil, out)
}

func (d *deviceNode) Open(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if err := d.vol.AcquireRef(); err != nil {
		return nil, 0, syscall.EIO
	}
	return &deviceHandle{vol: d.vol}, fuse.FOPEN_DIRECT_IO, fs.OK
}

func (d *deviceNode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	handle, ok := fh.(*deviceHandle)
	if !ok {
		return fs.OK
	}
	if err := handle.release(ctx); err != nil {
		return syscall.EIO
	}
	return fs.OK
}

func (d *deviceNode) OnForget() {
	d.nodeOnce.Do(func() {
		if err := d.vol.ReleaseRef(context.Background()); err != nil {
			slog.Warn("release ref on forget", "error", err)
		}
	})
}

func (d *deviceNode) Read(ctx context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off < 0 || uint64(off) >= d.volumeSize {
		return fuse.ReadResultData(nil), fs.OK
	}

	end := uint64(off) + uint64(len(dest))
	if end > d.volumeSize {
		dest = dest[:d.volumeSize-uint64(off)]
	}

	n, err := d.vol.Read(ctx, uint64(off), dest)
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (d *deviceNode) Write(ctx context.Context, _ fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if off < 0 || uint64(off) >= d.volumeSize {
		return 0, syscall.EFBIG
	}

	end := uint64(off) + uint64(len(data))
	if end > d.volumeSize {
		data = data[:d.volumeSize-uint64(off)]
	}

	if err := d.vol.Write(ctx, uint64(off), data); err != nil {
		return 0, syscall.EIO
	}
	return uint32(len(data)), fs.OK
}

func (d *deviceNode) Fsync(ctx context.Context, fh fs.FileHandle, _ uint32) syscall.Errno {
	if _, ok := fh.(*deviceHandle); !ok {
		return syscall.EBADF
	}
	if err := d.vol.Flush(ctx); err != nil {
		return syscall.EIO
	}
	return fs.OK
}

func (d *deviceNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	if _, ok := fh.(*deviceHandle); !ok {
		return syscall.EBADF
	}
	if err := d.vol.Flush(ctx); err != nil {
		return syscall.EIO
	}
	return fs.OK
}

func (d *deviceNode) Allocate(ctx context.Context, _ fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	const (
		fallocKeepSize  = 0x01
		fallocPunchHole = 0x02
	)
	if mode != fallocKeepSize|fallocPunchHole {
		return syscall.ENOTSUP
	}

	if off >= d.volumeSize || size == 0 {
		return fs.OK
	}
	end := off + size
	if end > d.volumeSize {
		end = d.volumeSize
	}

	if err := d.vol.PunchHole(ctx, off, end-off); err != nil {
		return syscall.EIO
	}
	return fs.OK
}

func (d *deviceNode) CopyFileRange(ctx context.Context, _ fs.FileHandle, offIn uint64, outNode *fs.Inode, _ fs.FileHandle, offOut uint64, length uint64, _ uint64) (uint32, syscall.Errno) {
	dstNode, ok := outNode.Operations().(*deviceNode)
	if !ok {
		return 0, syscall.ENOTSUP
	}

	if offIn >= d.volumeSize {
		return 0, fs.OK
	}
	if offIn+length > d.volumeSize {
		length = d.volumeSize - offIn
	}
	if offOut >= dstNode.volumeSize {
		return 0, syscall.EFBIG
	}
	if offOut+length > dstNode.volumeSize {
		length = dstNode.volumeSize - offOut
	}

	n, err := dstNode.vol.CopyFrom(ctx, d.vol, offIn, offOut, length)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(n), fs.OK
}
