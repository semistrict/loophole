// Package fuseblockdev implements a FUSE filesystem that exposes loophole volumes
// as device files. Each volume appears as a regular file that can be attached
// to a loop device and formatted with ext4.
//
// This uses the low-level go-fuse RawFileSystem API for minimal overhead.
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

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/storage"
)

const (
	rootIno uint64 = 1
	// Device file inodes start at 2.
	firstDevIno uint64 = 2
)

// sliceReadResult implements fuse.ReadResult and the juicedata go-fuse
// withSlice interface for zero-copy reads via writev.
type sliceReadResult struct {
	slices  [][]byte
	size    int
	cleanup func()
}

func (r *sliceReadResult) Slices() ([][]byte, int) { return r.slices, 0 }
func (r *sliceReadResult) Size() int               { return r.size }
func (r *sliceReadResult) Done() {
	if r.cleanup != nil {
		r.cleanup()
	}
}
func (r *sliceReadResult) Bytes(buf []byte) ([]byte, int) {
	n := 0
	for _, s := range r.slices {
		n += copy(buf[n:], s)
	}
	return buf[:n], 0
}

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
	fs       *blockDevFS
}

// Options configures the FUSE mount.
type Options struct {
	// Debug enables FUSE debug logging.
	Debug bool
	// EnableWriteback enables FUSE writeback caching for better write performance.
	EnableWriteback bool
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

	bfs := newBlockDevFS()
	server, err := fuse.NewServer(bfs, mountDir, &fuse.MountOptions{
		FsName:          "loophole",
		Name:            "loophole",
		DisableXAttrs:   true,
		MaxWrite:        1024 * 1024,
		MaxBackground:   128,
		DirectMount:     true,
		Debug:           opts.Debug,
		EnableWriteback: opts.EnableWriteback,
		NoAllocForRead:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		return nil, fmt.Errorf("fuse wait mount: %w", err)
	}

	return &Server{
		server:   server,
		MountDir: mountDir,
		fs:       bfs,
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
func (s *Server) Add(name string, vol *storage.Volume) {
	s.fs.addVolume(safeFileName(name), vol)
}

// Remove unregisters a volume. Existing open file handles continue to work.
func (s *Server) Remove(name string) {
	s.fs.removeVolume(safeFileName(name))
}

// DevicePath returns the path to the device file for a volume.
func (s *Server) DevicePath(volumeName string) string {
	return s.MountDir + "/" + safeFileName(volumeName)
}

// --- low-level RawFileSystem implementation ---

// volumeInfo holds the volume and its assigned inode.
type volumeInfo struct {
	vol  *storage.Volume
	ino  uint64
	name string // safe file name
}

// fileHandle tracks an open file descriptor.
type fileHandle struct {
	vol  *storage.Volume
	once sync.Once
}

func (h *fileHandle) release() error {
	var err error
	h.once.Do(func() {
		err = h.vol.ReleaseRef()
	})
	return err
}

type blockDevFS struct {
	fuse.RawFileSystem

	mu sync.Mutex
	// name → volumeInfo
	volumes map[string]*volumeInfo
	// inode → volumeInfo (for fast lookup by NodeId)
	inodes map[uint64]*volumeInfo
	// next inode to allocate
	nextIno uint64
	// file handle map: fh → *fileHandle
	handles map[uint64]*fileHandle
	nextFh  uint64

	uid uint32
	gid uint32
}

func newBlockDevFS() *blockDevFS {
	return &blockDevFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		volumes:       make(map[string]*volumeInfo),
		inodes:        make(map[uint64]*volumeInfo),
		nextIno:       firstDevIno,
		handles:       make(map[uint64]*fileHandle),
		nextFh:        1,
		uid:           uint32(os.Getuid()),
		gid:           uint32(os.Getgid()),
	}
}

func (fs *blockDevFS) addVolume(safeName string, vol *storage.Volume) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if vi, ok := fs.volumes[safeName]; ok {
		vi.vol = vol
		return
	}
	vi := &volumeInfo{vol: vol, ino: fs.nextIno, name: safeName}
	fs.nextIno++
	fs.volumes[safeName] = vi
	fs.inodes[vi.ino] = vi
}

func (fs *blockDevFS) removeVolume(safeName string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	vi, ok := fs.volumes[safeName]
	if !ok {
		return
	}
	delete(fs.volumes, safeName)
	delete(fs.inodes, vi.ino)
}

func (fs *blockDevFS) lookupVolume(safeName string) *volumeInfo {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.volumes[safeName]
}

func (fs *blockDevFS) getByIno(ino uint64) *volumeInfo {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.inodes[ino]
}

func (fs *blockDevFS) allocHandle(vol *storage.Volume) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fh := fs.nextFh
	fs.nextFh++
	fs.handles[fh] = &fileHandle{vol: vol}
	return fh
}

func (fs *blockDevFS) getHandle(fh uint64) *fileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.handles[fh]
}

func (fs *blockDevFS) removeHandle(fh uint64) *fileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	h := fs.handles[fh]
	delete(fs.handles, fh)
	return h
}

// Cache TTLs.
var (
	entryTTL = 5 * time.Second
	attrTTL  = 5 * time.Second
	negTTL   = time.Second
)

func (fs *blockDevFS) fillRootAttr(attr *fuse.Attr) {
	attr.Ino = rootIno
	attr.Mode = syscall.S_IFDIR | 0o755
	attr.Nlink = 2
	attr.Owner = fuse.Owner{Uid: fs.uid, Gid: fs.gid}
}

func (fs *blockDevFS) fillDevAttr(attr *fuse.Attr, vi *volumeInfo) {
	attr.Ino = vi.ino
	attr.Mode = syscall.S_IFREG | 0o600
	attr.Size = vi.vol.Size()
	attr.Blocks = vi.vol.Size() / 512
	attr.Nlink = 1
	attr.Owner = fuse.Owner{Uid: fs.uid, Gid: fs.gid}
}

func (fs *blockDevFS) String() string { return "loophole-blockdev" }

func (fs *blockDevFS) Init(*fuse.Server) {}

func (fs *blockDevFS) Lookup(_ <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if header.NodeId != rootIno {
		return fuse.ENOENT
	}
	vi := fs.lookupVolume(name)
	if vi == nil {
		out.NodeId = 0
		out.SetEntryTimeout(negTTL)
		return fuse.OK
	}
	out.NodeId = vi.ino
	out.Generation = 1
	out.SetEntryTimeout(entryTTL)
	out.SetAttrTimeout(attrTTL)
	fs.fillDevAttr(&out.Attr, vi)
	return fuse.OK
}

func (fs *blockDevFS) Forget(nodeid, nlookup uint64) {
	// We don't ref-count inodes; volumes are managed explicitly via Add/Remove.
}

func (fs *blockDevFS) GetAttr(_ <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	out.SetTimeout(attrTTL)
	if in.NodeId == rootIno {
		fs.fillRootAttr(&out.Attr)
		return fuse.OK
	}
	vi := fs.getByIno(in.NodeId)
	if vi == nil {
		return fuse.ENOENT
	}
	fs.fillDevAttr(&out.Attr, vi)
	return fuse.OK
}

func (fs *blockDevFS) SetAttr(_ <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	out.SetTimeout(attrTTL)
	if in.NodeId == rootIno {
		fs.fillRootAttr(&out.Attr)
		return fuse.OK
	}
	vi := fs.getByIno(in.NodeId)
	if vi == nil {
		return fuse.ENOENT
	}
	fs.fillDevAttr(&out.Attr, vi)
	return fuse.OK
}

func (fs *blockDevFS) Open(_ <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	done := metrics.FuseOp("open")
	vi := fs.getByIno(in.NodeId)
	if vi == nil {
		done(fuse.ENOENT)
		return fuse.ENOENT
	}
	if err := vi.vol.AcquireRef(); err != nil {
		slog.Warn("fuse open: acquire ref", "error", err)
		done(fuse.Status(syscall.EIO))
		return fuse.Status(syscall.EIO)
	}
	out.Fh = fs.allocHandle(vi.vol)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	done(fuse.OK)
	return fuse.OK
}

func (fs *blockDevFS) Read(_ <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	done := metrics.FuseOp("read")
	h := fs.getHandle(in.Fh)
	if h == nil {
		done(fuse.Status(syscall.EBADF))
		return nil, fuse.Status(syscall.EBADF)
	}
	vol := h.vol
	off := in.Offset
	size := int(in.Size)
	if off >= vol.Size() {
		done(fuse.OK)
		return fuse.ReadResultData(nil), fuse.OK
	}
	if off+uint64(size) > vol.Size() {
		size = int(vol.Size() - off)
	}

	slices, cleanup, err := vol.ReadPages(context.Background(), off, size)
	if err != nil {
		slog.Warn("blockdev: read error", "off", off, "len", size, "error", err)
		done(fuse.Status(syscall.EIO))
		return nil, fuse.Status(syscall.EIO)
	}
	metrics.FuseBytes.WithLabelValues("read").Add(float64(size))
	done(fuse.OK)
	return &sliceReadResult{slices: slices, size: size, cleanup: cleanup}, fuse.OK
}

func (fs *blockDevFS) Write(_ <-chan struct{}, in *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	done := metrics.FuseOp("write")
	h := fs.getHandle(in.Fh)
	if h == nil {
		done(fuse.Status(syscall.EBADF))
		return 0, fuse.Status(syscall.EBADF)
	}
	vol := h.vol
	off := in.Offset
	slog.Debug("blockdev: write", "off", off, "len", len(data), "volSize", vol.Size())
	if off >= vol.Size() {
		done(fuse.Status(syscall.EFBIG))
		return 0, fuse.Status(syscall.EFBIG)
	}

	end := off + uint64(len(data))
	if end > vol.Size() {
		data = data[:vol.Size()-off]
	}

	if err := vol.Write(data, off); err != nil {
		slog.Warn("blockdev: write error", "off", off, "len", len(data), "error", err)
		done(fuse.Status(syscall.EIO))
		return 0, fuse.Status(syscall.EIO)
	}
	slog.Debug("blockdev: write done", "off", off, "len", len(data))
	metrics.FuseBytes.WithLabelValues("write").Add(float64(len(data)))
	done(fuse.OK)
	return uint32(len(data)), fuse.OK
}

func (fs *blockDevFS) Flush(_ <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	slog.Debug("blockdev: flush start")
	done := metrics.FuseOp("flush")
	h := fs.getHandle(in.Fh)
	if h == nil {
		done(fuse.Status(syscall.EBADF))
		return fuse.Status(syscall.EBADF)
	}
	if err := h.vol.Flush(); err != nil {
		slog.Warn("blockdev: flush error", "error", err)
		done(fuse.Status(syscall.EIO))
		return fuse.Status(syscall.EIO)
	}
	slog.Debug("blockdev: flush done")
	done(fuse.OK)
	return fuse.OK
}

func (fs *blockDevFS) Fsync(_ <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	slog.Debug("blockdev: fsync start")
	done := metrics.FuseOp("fsync")
	h := fs.getHandle(in.Fh)
	if h == nil {
		done(fuse.Status(syscall.EBADF))
		return fuse.Status(syscall.EBADF)
	}
	// Use FlushLocal to freeze the dirty pages without waiting for S3 upload.
	// The background flush loop will handle the upload asynchronously.
	if err := h.vol.FlushLocal(); err != nil {
		slog.Warn("blockdev: fsync error", "error", err)
		done(fuse.Status(syscall.EIO))
		return fuse.Status(syscall.EIO)
	}
	slog.Debug("blockdev: fsync done")
	done(fuse.OK)
	return fuse.OK
}

func (fs *blockDevFS) Release(_ <-chan struct{}, in *fuse.ReleaseIn) {
	done := metrics.FuseOp("release")
	h := fs.removeHandle(in.Fh)
	if h == nil {
		done(fuse.OK)
		return
	}
	if err := h.release(); err != nil {
		done(fuse.Status(syscall.EIO))
		return
	}
	done(fuse.OK)
}

func (fs *blockDevFS) Fallocate(_ <-chan struct{}, in *fuse.FallocateIn) fuse.Status {
	const (
		fallocKeepSize  = 0x01
		fallocPunchHole = 0x02
		fallocZeroRange = 0x10
	)

	h := fs.getHandle(in.Fh)
	if h == nil {
		return fuse.Status(syscall.EBADF)
	}
	vol := h.vol
	var opName string
	switch in.Mode {
	case fallocKeepSize | fallocPunchHole:
		opName = "fallocate_punch_hole"
	case fallocKeepSize | fallocZeroRange:
		opName = "fallocate_zero_range"
	default:
		slog.Warn("fuse allocate: unsupported mode", "mode", fmt.Sprintf("0x%x", in.Mode))
		metrics.FuseOps.WithLabelValues("fallocate_unsupported", "error").Inc()
		return fuse.Status(syscall.ENOTSUP)
	}
	done := metrics.FuseOp(opName)

	off := in.Offset
	length := in.Length
	if off >= vol.Size() || length == 0 {
		done(fuse.OK)
		return fuse.OK
	}
	end := off + length
	if end > vol.Size() {
		end = vol.Size()
	}

	slog.Debug("blockdev: fallocate", "op", opName, "off", off, "end", end, "len", end-off)
	if err := vol.PunchHole(off, end-off); err != nil {
		done(fuse.Status(syscall.EIO))
		return fuse.Status(syscall.EIO)
	}
	done(fuse.OK)
	return fuse.OK
}

func (fs *blockDevFS) CopyFileRange(_ <-chan struct{}, in *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	done := metrics.FuseOp("copy_file_range")
	hIn := fs.getHandle(in.FhIn)
	if hIn == nil {
		done(fuse.Status(syscall.EBADF))
		return 0, fuse.Status(syscall.EBADF)
	}
	hOut := fs.getHandle(in.FhOut)
	if hOut == nil {
		done(fuse.Status(syscall.EBADF))
		return 0, fuse.Status(syscall.EBADF)
	}
	srcVol := hIn.vol
	dstVol := hOut.vol
	offIn := in.OffIn
	offOut := in.OffOut
	length := in.Len

	if offIn >= srcVol.Size() {
		done(fuse.OK)
		return 0, fuse.OK
	}
	if offIn+length > srcVol.Size() {
		length = srcVol.Size() - offIn
	}
	if offOut >= dstVol.Size() {
		done(fuse.Status(syscall.EFBIG))
		return 0, fuse.Status(syscall.EFBIG)
	}
	if offOut+length > dstVol.Size() {
		length = dstVol.Size() - offOut
	}

	n, err := dstVol.CopyFrom(srcVol, offIn, offOut, length)
	if err != nil {
		done(fuse.Status(syscall.EIO))
		return 0, fuse.Status(syscall.EIO)
	}
	metrics.FuseBytes.WithLabelValues("copy_file_range").Add(float64(n))
	done(fuse.OK)
	return uint32(n), fuse.OK
}

func (fs *blockDevFS) OpenDir(_ <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if in.NodeId != rootIno {
		return fuse.ENOENT
	}
	return fuse.OK
}

func (fs *blockDevFS) ReadDir(_ <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	if in.NodeId != rootIno {
		return fuse.ENOENT
	}

	fs.mu.Lock()
	// Build a stable snapshot of volume entries.
	type entry struct {
		name string
		ino  uint64
	}
	entries := make([]entry, 0, len(fs.volumes))
	for name, vi := range fs.volumes {
		entries = append(entries, entry{name: name, ino: vi.ino})
	}
	fs.mu.Unlock()

	offset := int(in.Offset)
	for i := offset; i < len(entries); i++ {
		if !out.AddDirEntry(fuse.DirEntry{
			Mode: syscall.S_IFREG,
			Name: entries[i].name,
			Ino:  entries[i].ino,
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *blockDevFS) ReadDirPlus(_ <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	if in.NodeId != rootIno {
		return fuse.ENOENT
	}

	fs.mu.Lock()
	type entry struct {
		name string
		vi   *volumeInfo
	}
	entries := make([]entry, 0, len(fs.volumes))
	for name, vi := range fs.volumes {
		entries = append(entries, entry{name: name, vi: vi})
	}
	fs.mu.Unlock()

	offset := int(in.Offset)
	for i := offset; i < len(entries); i++ {
		e := entries[i]
		eo := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: syscall.S_IFREG,
			Name: e.name,
			Ino:  e.vi.ino,
		})
		if eo == nil {
			break
		}
		eo.NodeId = e.vi.ino
		eo.Generation = 1
		eo.SetEntryTimeout(entryTTL)
		eo.SetAttrTimeout(attrTTL)
		fs.fillDevAttr(&eo.Attr, e.vi)
	}
	return fuse.OK
}

func (fs *blockDevFS) ReleaseDir(in *fuse.ReleaseIn) {}

func (fs *blockDevFS) StatFs(_ <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.NameLen = 255
	out.Frsize = 4096
	out.Bsize = 4096
	out.Blocks = 1
	out.Bfree = 0
	out.Bavail = 0
	return fuse.OK
}
