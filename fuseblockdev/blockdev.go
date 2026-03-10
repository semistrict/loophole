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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/metrics"
)

// safeFileName encodes a volume name for safe use as a filename.
// Encodes % → %25 and / → %2F.
func safeFileName(name string) string {
	name = strings.ReplaceAll(name, "%", "%25")
	name = strings.ReplaceAll(name, "/", "%2F")
	return name
}

// Server manages the internal FUSE mount that exposes volume device files.
// Volumes must be explicitly registered via Add before they are visible.
type Server struct {
	server   *fuse.Server
	MountDir string
	root     *rootNode

	mu        sync.Mutex
	volumes   map[string]loophole.Volume // safeFileName(name) → volume
	origNames map[string]string          // safeFileName(name) → original name
}

// Options configures the FUSE mount.
type Options struct {
	// Debug enables FUSE debug logging.
	Debug bool
}

// Start mounts a FUSE filesystem at mountDir.
// Volumes are not visible until registered via Add.
func Start(mountDir string, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}

	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("create fuse mount dir: %w", err)
	}

	srv := &Server{
		volumes:   make(map[string]loophole.Volume),
		origNames: make(map[string]string),
	}
	root := &rootNode{srv: srv}

	cacheTTL := 5 * time.Second
	negTTL := time.Second
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:          "loophole",
			Name:            "loophole",
			DisableXAttrs:   true,
			MaxWrite:        1024 * 1024,
			MaxBackground:   128,
			DirectMount:     true,
			Debug:           opts.Debug,
			EnableWriteback: true,
		},
		EntryTimeout:    &cacheTTL,
		AttrTimeout:     &cacheTTL,
		NegativeTimeout: &negTTL,
		UID:             uint32(os.Getuid()),
		GID:             uint32(os.Getgid()),
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	srv.server = server
	srv.MountDir = mountDir
	srv.root = root
	return srv, nil
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
	// Try fusermount first, then fusermount3 (Ubuntu 24.04+).
	if err := exec.Command("fusermount", "-u", dir).Run(); err != nil {
		if err2 := exec.Command("fusermount3", "-u", dir).Run(); err2 != nil {
			// Last resort: lazy unmount via umount.
			if err3 := exec.Command("umount", "-l", dir).Run(); err3 != nil {
				slog.Debug("fusermount stale cleanup", "dir", dir, "error", err3)
			}
		}
	}
}

// Add registers a volume so it becomes visible as a device file.
func (s *Server) Add(name string, vol loophole.Volume) {
	safe := safeFileName(name)
	s.mu.Lock()
	s.volumes[safe] = vol
	s.origNames[safe] = name
	s.mu.Unlock()
}

// Remove unregisters a volume. Existing FUSE inodes continue to work
// (they hold their own refs) but new Lookups will return ENOENT.
func (s *Server) Remove(name string) {
	safe := safeFileName(name)
	s.mu.Lock()
	delete(s.volumes, safe)
	delete(s.origNames, safe)
	s.mu.Unlock()
}

// getVolume looks up a volume by its safe (filesystem-encoded) name.
func (s *Server) getVolume(safeName string) loophole.Volume {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.volumes[safeName]
}

// volumeNames returns the safe (filesystem-encoded) names of all registered volumes.
func (s *Server) volumeNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.volumes))
	for name := range s.volumes {
		names = append(names, name)
	}
	return names
}

// DevicePath returns the path to the device file for a volume.
func (s *Server) DevicePath(volumeName string) string {
	return s.MountDir + "/" + safeFileName(volumeName)
}

// --- root node: flat directory of volume files ---

type rootNode struct {
	fs.Inode
	srv *Server
}

var _ = (fs.NodeLookuper)((*rootNode)(nil))
var _ = (fs.NodeReaddirer)((*rootNode)(nil))
var _ = (fs.NodeGetattrer)((*rootNode)(nil))

func (r *rootNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o755
	out.Nlink = 2
	return fs.OK
}

func (r *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	vol := r.srv.getVolume(name)
	if vol == nil {
		return nil, syscall.ENOENT
	}
	// Note: we do NOT acquire a ref here. The Open/Release pair handles the
	// critical ref that prevents the volume from being destroyed while in use.
	// Acquiring a ref in Lookup would delay volume cleanup until the kernel
	// evicts the inode (OnForget), which can be arbitrarily delayed.
	devNode := &deviceNode{srv: r.srv, safeName: name}
	stable := fs.StableAttr{Mode: syscall.S_IFREG}
	child := r.NewInode(ctx, devNode, stable)
	devNode.fillAttr(&out.Attr, vol)
	return child, fs.OK
}

func (r *rootNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	names := r.srv.volumeNames()
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
	srv      *Server
	safeName string
}

// volume returns the current volume for this device node.
// It looks up the volume dynamically from the server so that
// a re-registered volume (after unmount+remount) is picked up
// even if the kernel reuses a cached inode.
func (d *deviceNode) volume() loophole.Volume {
	return d.srv.getVolume(d.safeName)
}

type deviceHandle struct {
	vol  loophole.Volume
	once sync.Once
}

func (h *deviceHandle) release(ctx context.Context) error {
	var err error
	h.once.Do(func() {
		err = h.vol.ReleaseRef()
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
var _ = (fs.NodeAllocater)((*deviceNode)(nil))
var _ = (fs.NodeCopyFileRanger)((*deviceNode)(nil))

func (d *deviceNode) fillAttr(out *fuse.Attr, vol loophole.Volume) {
	if vol.ReadOnly() {
		out.Mode = syscall.S_IFREG | 0o400
	} else {
		out.Mode = syscall.S_IFREG | 0o600
	}
	out.Size = vol.Size()
	out.Blocks = vol.Size() / 512
	out.Nlink = 1
}

func (d *deviceNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	vol := d.volume()
	if vol == nil {
		return syscall.ENOENT
	}
	d.fillAttr(&out.Attr, vol)
	return fs.OK
}

func (d *deviceNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	vol := d.volume()
	if vol == nil {
		return syscall.ENOENT
	}
	if mode, ok := in.GetMode(); ok {
		writable := mode&0o200 != 0
		if !writable && !vol.ReadOnly() {
			if err := vol.Freeze(); err != nil {
				return syscall.EIO
			}
		}
	}
	return d.Getattr(ctx, nil, out)
}

func (d *deviceNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	done := metrics.FuseOp("open")
	vol := d.volume()
	if vol == nil {
		done(syscall.ENOENT)
		return nil, 0, syscall.ENOENT
	}
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 && vol.ReadOnly() {
		done(syscall.EROFS)
		return nil, 0, syscall.EROFS
	}
	if err := vol.AcquireRef(); err != nil {
		slog.Warn("fuse open: acquire ref", "error", err)
		done(syscall.EIO)
		return nil, 0, syscall.EIO
	}
	done(fs.OK)
	return &deviceHandle{vol: vol}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (d *deviceNode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	done := metrics.FuseOp("release")
	handle, ok := fh.(*deviceHandle)
	if !ok {
		done(fs.OK)
		return fs.OK
	}
	if err := handle.release(ctx); err != nil {
		done(syscall.EIO)
		return syscall.EIO
	}
	done(fs.OK)
	return fs.OK
}

func (d *deviceNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	done := metrics.FuseOp("read")
	h, ok := fh.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return nil, syscall.EBADF
	}
	vol := h.vol
	slog.Debug("blockdev: read", "off", off, "len", len(dest), "volSize", vol.Size())
	if off < 0 || uint64(off) >= vol.Size() {
		done(fs.OK)
		return fuse.ReadResultData(nil), fs.OK
	}

	end := uint64(off) + uint64(len(dest))
	if end > vol.Size() {
		dest = dest[:vol.Size()-uint64(off)]
	}

	n, err := vol.Read(ctx, dest, uint64(off))
	if err != nil {
		slog.Warn("blockdev: read error", "off", off, "len", len(dest), "error", err)
		done(syscall.EIO)
		return nil, syscall.EIO
	}
	slog.Debug("blockdev: read done", "off", off, "n", n)
	metrics.FuseBytes.WithLabelValues("read").Add(float64(n))
	done(fs.OK)
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (d *deviceNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	done := metrics.FuseOp("write")
	h, ok := fh.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return 0, syscall.EBADF
	}
	vol := h.vol
	if vol.ReadOnly() {
		done(syscall.EROFS)
		return 0, syscall.EROFS
	}
	slog.Debug("blockdev: write", "off", off, "len", len(data), "volSize", vol.Size())
	if off < 0 || uint64(off) >= vol.Size() {
		done(syscall.EFBIG)
		return 0, syscall.EFBIG
	}

	end := uint64(off) + uint64(len(data))
	if end > vol.Size() {
		data = data[:vol.Size()-uint64(off)]
	}

	if err := vol.Write(data, uint64(off)); err != nil {
		slog.Warn("blockdev: write error", "off", off, "len", len(data), "error", err)
		done(syscall.EIO)
		return 0, syscall.EIO
	}
	slog.Debug("blockdev: write done", "off", off, "len", len(data))
	metrics.FuseBytes.WithLabelValues("write").Add(float64(len(data)))
	done(fs.OK)
	return uint32(len(data)), fs.OK
}

func (d *deviceNode) Fsync(ctx context.Context, fh fs.FileHandle, _ uint32) syscall.Errno {
	slog.Debug("blockdev: fsync start")
	done := metrics.FuseOp("fsync")
	h, ok := fh.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return syscall.EBADF
	}
	// Use FlushLocal to freeze the memtable without waiting for S3 upload.
	// The background flush loop will handle the upload asynchronously.
	if err := h.vol.FlushLocal(); err != nil {
		slog.Warn("blockdev: fsync error", "error", err)
		done(syscall.EIO)
		return syscall.EIO
	}
	slog.Debug("blockdev: fsync done")
	done(fs.OK)
	return fs.OK
}

func (d *deviceNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	slog.Debug("blockdev: flush start")
	done := metrics.FuseOp("flush")
	h, ok := fh.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return syscall.EBADF
	}
	if err := h.vol.Flush(); err != nil {
		slog.Warn("blockdev: flush error", "error", err)
		done(syscall.EIO)
		return syscall.EIO
	}
	slog.Debug("blockdev: flush done")
	done(fs.OK)
	return fs.OK
}

func (d *deviceNode) Allocate(ctx context.Context, fh fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	const (
		fallocKeepSize  = 0x01
		fallocPunchHole = 0x02
		fallocZeroRange = 0x10
	)
	h, ok := fh.(*deviceHandle)
	if !ok {
		return syscall.EBADF
	}
	vol := h.vol
	if vol.ReadOnly() {
		metrics.FuseOps.WithLabelValues("fallocate_readonly", "error").Inc()
		return syscall.EROFS
	}

	var opName string
	switch mode {
	case fallocKeepSize | fallocPunchHole:
		opName = "fallocate_punch_hole"
	case fallocKeepSize | fallocZeroRange:
		opName = "fallocate_zero_range"
	default:
		slog.Warn("fuse allocate: unsupported mode", "mode", fmt.Sprintf("0x%x", mode))
		metrics.FuseOps.WithLabelValues("fallocate_unsupported", "error").Inc()
		return syscall.ENOTSUP
	}
	done := metrics.FuseOp(opName)

	if off >= vol.Size() || size == 0 {
		done(fs.OK)
		return fs.OK
	}
	end := off + size
	if end > vol.Size() {
		end = vol.Size()
	}

	slog.Debug("blockdev: fallocate", "op", opName, "off", off, "end", end, "len", end-off)
	if err := vol.PunchHole(off, end-off); err != nil {
		done(syscall.EIO)
		return syscall.EIO
	}
	done(fs.OK)
	return fs.OK
}

func (d *deviceNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, outNode *fs.Inode, fhOut fs.FileHandle, offOut uint64, length uint64, _ uint64) (uint32, syscall.Errno) {
	done := metrics.FuseOp("copy_file_range")
	hIn, ok := fhIn.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return 0, syscall.EBADF
	}
	hOut, ok := fhOut.(*deviceHandle)
	if !ok {
		done(syscall.EBADF)
		return 0, syscall.EBADF
	}
	srcVol := hIn.vol
	dstVol := hOut.vol
	if dstVol.ReadOnly() {
		done(syscall.EROFS)
		return 0, syscall.EROFS
	}

	if offIn >= srcVol.Size() {
		done(fs.OK)
		return 0, fs.OK
	}
	if offIn+length > srcVol.Size() {
		length = srcVol.Size() - offIn
	}
	if offOut >= dstVol.Size() {
		done(syscall.EFBIG)
		return 0, syscall.EFBIG
	}
	if offOut+length > dstVol.Size() {
		length = dstVol.Size() - offOut
	}

	n, err := dstVol.CopyFrom(srcVol, offIn, offOut, length)
	if err != nil {
		done(syscall.EIO)
		return 0, syscall.EIO
	}
	metrics.FuseBytes.WithLabelValues("copy_file_range").Add(float64(n))
	done(fs.OK)
	return uint32(n), fs.OK
}
