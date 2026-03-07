// Package lwext4 provides Go bindings for the lwext4 C library,
// an in-process ext4 filesystem implementation.
//
// This package exposes an inode-level API suitable for building
// FUSE servers or other filesystem layers on top.
package lwext4

// #cgo CFLAGS: -DCONFIG_USE_DEFAULT_CFG=1
// #cgo CFLAGS: -DCONFIG_EXT4_BLOCKDEVS_COUNT=16
// #cgo CFLAGS: -DCONFIG_EXT4_MOUNTPOINTS_COUNT=16
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_OFLAGS=1
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ERRNO=0
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ASSERT=1
// #cgo CFLAGS: -DCONFIG_DEBUG_PRINTF=0
// #cgo CFLAGS: -DCONFIG_DEBUG_ASSERT=0
// #cgo CFLAGS: -I${SRCDIR}/../third_party/lwext4/include
// #include "lwext4_cgo.h"
import "C"
import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// rcError converts a C errno return code into a Go error, wrapping
// well-known errno values so that errors.Is works (e.g. os.ErrNotExist).
func rcError(prefix string, rc C.int) error {
	errno := syscall.Errno(rc)
	return fmt.Errorf("%s: %w", prefix, errno)
}

// Ino is an inode number.
type Ino uint32

// RootIno is the ext4 root directory inode.
const RootIno Ino = 2

var fsCounter atomic.Int64

// FormatOptions controls mkfs parameters.
type FormatOptions struct {
	BlockSize uint32 // default 4096
	Label     string
	NoJournal bool   // default false (journal enabled)
	Uid       uint32 // owner uid for root dir and lost+found (default 0)
	Gid       uint32 // owner gid for root dir and lost+found (default 0)
}

// FS is a mounted lwext4 filesystem instance.
type FS struct {
	mu         sync.Mutex // serializes all CGO calls; lwext4 is not thread-safe
	bdev       C.lh_bdev
	handle     int
	devName    string
	mountPoint string
	mp         C.lh_mp
}

// Format creates a new ext4 filesystem on dev and returns a mounted FS.
func Format(dev BlockDevice, size int64, opts *FormatOptions) (*FS, error) {
	blockSize := uint32(4096)
	if opts != nil && opts.BlockSize != 0 {
		blockSize = opts.BlockSize
	}

	phBsize := uint32(512)
	phBcnt := uint64(size) / uint64(phBsize)

	bdev, handle := createBlockdev(dev, phBsize, phBcnt)
	if bdev == nil {
		return nil, fmt.Errorf("lwext4: failed to create blockdev")
	}

	id := fsCounter.Add(1)
	devName := fmt.Sprintf("dev_%d", id)
	mountPoint := fmt.Sprintf("/mp_%d/", id)

	cDevName := C.CString(devName)
	defer C.free(unsafe.Pointer(cDevName))
	cMountPoint := C.CString(mountPoint)
	defer C.free(unsafe.Pointer(cMountPoint))

	rc := C.lh_device_register(bdev, cDevName)
	if rc != 0 {
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: ext4_device_register: %d", rc)
	}

	journal := 1
	if opts != nil && opts.NoJournal {
		journal = 0
	}
	var cLabel *C.char
	if opts != nil && opts.Label != "" {
		cLabel = C.CString(opts.Label)
		defer C.free(unsafe.Pointer(cLabel))
	}

	rc = C.lh_mkfs(bdev, C.int64_t(size), C.uint32_t(blockSize), C.int(journal), cLabel)
	if rc != 0 {
		C.lh_device_unregister(cDevName)
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: ext4_mkfs: %d", rc)
	}

	rc = C.lh_mount(cDevName, cMountPoint, 0)
	if rc != 0 {
		C.lh_device_unregister(cDevName)
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: ext4_mount: %d", rc)
	}

	C.lh_cache_write_back(cMountPoint, 1)

	mp := C.lh_get_mp(cMountPoint)
	if mp == nil {
		C.lh_umount(cMountPoint)
		C.lh_device_unregister(cDevName)
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: failed to get mountpoint")
	}

	fs := &FS{
		bdev:       bdev,
		handle:     handle,
		devName:    devName,
		mountPoint: mountPoint,
		mp:         mp,
	}

	// Set ownership on root dir and lost+found if requested.
	if opts != nil && (opts.Uid != 0 || opts.Gid != 0) {
		ownerAttr := &Attr{Uid: opts.Uid, Gid: opts.Gid}
		ownerMask := uint32(AttrUid | AttrGid)
		_ = fs.SetAttr(RootIno, ownerAttr, ownerMask)
		if lfIno, err := fs.Lookup(RootIno, "lost+found"); err == nil {
			_ = fs.SetAttr(lfIno, ownerAttr, ownerMask)
		}
	}

	return fs, nil
}

// Open mounts an existing ext4 filesystem from dev.
func Mount(dev BlockDevice, size int64) (*FS, error) {
	return mount(dev, size, false)
}

func MountReadOnly(dev BlockDevice, size int64) (*FS, error) {
	return mount(dev, size, true)
}

func mount(dev BlockDevice, size int64, readOnly bool) (*FS, error) {
	phBsize := uint32(512)
	phBcnt := uint64(size) / uint64(phBsize)

	bdev, handle := createBlockdev(dev, phBsize, phBcnt)
	if bdev == nil {
		return nil, fmt.Errorf("lwext4: failed to create blockdev")
	}

	id := fsCounter.Add(1)
	devName := fmt.Sprintf("dev_%d", id)
	mountPoint := fmt.Sprintf("/mp_%d/", id)

	cDevName := C.CString(devName)
	defer C.free(unsafe.Pointer(cDevName))
	cMountPoint := C.CString(mountPoint)
	defer C.free(unsafe.Pointer(cMountPoint))

	rc := C.lh_device_register(bdev, cDevName)
	if rc != 0 {
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: ext4_device_register: %d", rc)
	}

	var roFlag C.int
	if readOnly {
		roFlag = 1
	}
	rc = C.lh_mount(cDevName, cMountPoint, roFlag)
	if rc != 0 {
		C.lh_device_unregister(cDevName)
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: mount failed (error %d); volume may be corrupt or incompatible — try recreating it", rc)
	}

	C.lh_cache_write_back(cMountPoint, 1)

	mp := C.lh_get_mp(cMountPoint)
	if mp == nil {
		C.lh_umount(cMountPoint)
		C.lh_device_unregister(cDevName)
		destroyBlockdev(bdev, handle)
		return nil, fmt.Errorf("lwext4: failed to get mountpoint")
	}

	return &FS{
		bdev:       bdev,
		handle:     handle,
		devName:    devName,
		mountPoint: mountPoint,
		mp:         mp,
	}, nil
}

// Close unmounts and cleans up.
func (fs *FS) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cMountPoint := C.CString(fs.mountPoint)
	defer C.free(unsafe.Pointer(cMountPoint))
	cDevName := C.CString(fs.devName)
	defer C.free(unsafe.Pointer(cDevName))

	C.lh_cache_write_back(cMountPoint, 0)

	rc := C.lh_umount(cMountPoint)
	if rc != 0 {
		return fmt.Errorf("lwext4: ext4_umount: %d", rc)
	}

	C.lh_device_unregister(cDevName)
	destroyBlockdev(fs.bdev, fs.handle)
	return nil
}

// CacheFlush flushes all dirty cache entries to the block device.
func (fs *FS) CacheFlush() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.cacheFlushLocked()
}

func (fs *FS) cacheFlushLocked() error {
	cMountPoint := C.CString(fs.mountPoint)
	defer C.free(unsafe.Pointer(cMountPoint))
	rc := C.lh_cache_flush(cMountPoint)
	if rc != 0 {
		return fmt.Errorf("lwext4: ext4_cache_flush: %d", rc)
	}
	return nil
}

// Attr holds inode attributes.
type Attr struct {
	Ino   Ino
	Size  uint64
	Mode  uint32 // includes file type bits (S_IFDIR, S_IFREG, etc.)
	Uid   uint32
	Gid   uint32
	Atime uint32
	Mtime uint32
	Ctime uint32
	Links uint16
}

// SetAttr mask bits.
const (
	AttrMode  = 1 << 0
	AttrUid   = 1 << 1
	AttrGid   = 1 << 2
	AttrAtime = 1 << 3
	AttrMtime = 1 << 4
	AttrCtime = 1 << 5
	AttrSize  = 1 << 6
)

// DirEntry represents a single directory entry.
type DirEntry struct {
	Name  string
	Inode Ino
	Type  uint8 // EXT4_DE_REG_FILE, EXT4_DE_DIR, etc.
}

// Directory entry types (matching ext4_types.h EXT4_DE_* enum).
const (
	TypeUnknown = 0
	TypeRegFile = 1
	TypeDir     = 2
	TypeChrdev  = 3
	TypeBlkdev  = 4
	TypeFIFO    = 5
	TypeSock    = 6
	TypeSymlink = 7
)

// Lookup finds a child inode by name in a parent directory.
func (fs *FS) Lookup(parent Ino, name string) (Ino, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var child C.uint32_t
	rc := C.lh_lookup(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)), &child)
	if rc != 0 {
		return 0, rcError(fmt.Sprintf("lwext4: lookup(%d, %s)", parent, name), rc)
	}
	return Ino(child), nil
}

// GetAttr returns the attributes of an inode.
func (fs *FS) GetAttr(ino Ino) (*Attr, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var ca C.struct_inode_attr
	rc := C.lh_getattr(fs.mp, C.uint32_t(ino), &ca)
	if rc != 0 {
		return nil, rcError(fmt.Sprintf("lwext4: getattr(%d)", ino), rc)
	}
	return &Attr{
		Ino:   Ino(ca.ino),
		Size:  uint64(ca.size),
		Mode:  uint32(ca.mode),
		Uid:   uint32(ca.uid),
		Gid:   uint32(ca.gid),
		Atime: uint32(ca.atime),
		Mtime: uint32(ca.mtime),
		Ctime: uint32(ca.ctime),
		Links: uint16(ca.links),
	}, nil
}

// SetAttr sets attributes on an inode. Only fields indicated by mask are written.
func (fs *FS) SetAttr(ino Ino, attr *Attr, mask uint32) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ca := C.struct_inode_attr{
		mode:  C.uint32_t(attr.Mode),
		uid:   C.uint32_t(attr.Uid),
		gid:   C.uint32_t(attr.Gid),
		atime: C.uint32_t(attr.Atime),
		mtime: C.uint32_t(attr.Mtime),
		ctime: C.uint32_t(attr.Ctime),
	}
	rc := C.lh_setattr(fs.mp, C.uint32_t(ino), &ca, C.uint32_t(mask))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: setattr(%d)", ino), rc)
	}
	return nil
}

// Mknod creates a regular file in a parent directory. Returns the new inode.
func (fs *FS) Mknod(parent Ino, name string, mode uint32) (Ino, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var child C.uint32_t
	rc := C.lh_mknod(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)),
		C.uint32_t(mode), C.int(TypeRegFile), &child)
	if rc != 0 {
		return 0, rcError(fmt.Sprintf("lwext4: mknod(%d, %s)", parent, name), rc)
	}
	return Ino(child), nil
}

// Mkdir creates a directory in a parent directory. Returns the new inode.
func (fs *FS) Mkdir(parent Ino, name string, mode uint32) (Ino, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var child C.uint32_t
	rc := C.lh_mkdir(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)),
		C.uint32_t(mode), &child)
	if rc != 0 {
		return 0, rcError(fmt.Sprintf("lwext4: mkdir(%d, %s)", parent, name), rc)
	}
	return Ino(child), nil
}

// Unlink removes a non-directory entry from a parent directory.
func (fs *FS) Unlink(parent Ino, name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	rc := C.lh_unlink(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: unlink(%d, %s)", parent, name), rc)
	}
	return nil
}

// UnlinkOrphan removes a directory entry but, if the inode's link count drops
// to zero, adds it to the on-disk orphan list instead of freeing it. Returns
// the child inode number so the caller can track open handles.
func (fs *FS) UnlinkOrphan(parent Ino, name string) (Ino, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var child C.uint32_t
	rc := C.lh_unlink_orphan(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)), &child)
	if rc != 0 {
		return 0, rcError(fmt.Sprintf("lwext4: unlink_orphan(%d, %s)", parent, name), rc)
	}
	return Ino(child), nil
}

// FreeOrphan removes an inode from the orphan list and frees its data and inode.
// Call when the last open file handle for an orphaned inode is closed.
func (fs *FS) FreeOrphan(ino Ino) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	rc := C.lh_free_orphan(fs.mp, C.uint32_t(ino))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: free_orphan(%d)", ino), rc)
	}
	return nil
}

// OrphanRecover walks the on-disk orphan list and frees all orphaned inodes.
// Call once at mount time to clean up after crashes.
func (fs *FS) OrphanRecover() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	rc := C.lh_orphan_recover(fs.mp)
	if rc != 0 {
		return rcError("lwext4: orphan_recover", rc)
	}
	return nil
}

// Rmdir removes an empty directory from a parent directory.
func (fs *FS) Rmdir(parent Ino, name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	rc := C.lh_rmdir(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: rmdir(%d, %s)", parent, name), rc)
	}
	return nil
}

// Rename moves an entry from one parent directory to another.
func (fs *FS) Rename(srcParent Ino, srcName string, dstParent Ino, dstName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cSrc := C.CString(srcName)
	defer C.free(unsafe.Pointer(cSrc))
	cDst := C.CString(dstName)
	defer C.free(unsafe.Pointer(cDst))
	rc := C.lh_rename(fs.mp,
		C.uint32_t(srcParent), cSrc, C.uint32_t(len(srcName)),
		C.uint32_t(dstParent), cDst, C.uint32_t(len(dstName)))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: rename(%d/%s -> %d/%s)",
			srcParent, srcName, dstParent, dstName), rc)
	}
	return nil
}

// Link creates a hard link to an existing inode in a parent directory.
func (fs *FS) Link(ino Ino, newParent Ino, newName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(newName)
	defer C.free(unsafe.Pointer(cName))
	rc := C.lh_link(fs.mp, C.uint32_t(ino), C.uint32_t(newParent),
		cName, C.uint32_t(len(newName)))
	if rc != 0 {
		return rcError(fmt.Sprintf("lwext4: link(%d -> %d/%s)", ino, newParent, newName), rc)
	}
	return nil
}

// Symlink creates a symbolic link. Returns the new inode.
func (fs *FS) Symlink(parent Ino, name string, target string) (Ino, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cTarget := C.CString(target)
	defer C.free(unsafe.Pointer(cTarget))
	var child C.uint32_t
	rc := C.lh_symlink(fs.mp, C.uint32_t(parent), cName, C.uint32_t(len(name)),
		cTarget, C.uint32_t(len(target)), &child)
	if rc != 0 {
		return 0, rcError(fmt.Sprintf("lwext4: symlink(%d, %s)", parent, name), rc)
	}
	return Ino(child), nil
}

// Readlink reads the target of a symbolic link.
func (fs *FS) Readlink(ino Ino) (string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	buf := make([]byte, 4096)
	var rcnt C.size_t
	rc := C.lh_readlink(fs.mp, C.uint32_t(ino), (*C.char)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)), &rcnt)
	if rc != 0 {
		return "", rcError(fmt.Sprintf("lwext4: readlink(%d)", ino), rc)
	}
	return string(buf[:rcnt]), nil
}

// readdir callback bridge

type readdirState struct {
	entries []DirEntry
}

var (
	readdirMu     sync.Mutex
	readdirStates = map[uintptr]*readdirState{}
	readdirNextID uintptr
)

//export goReaddirCallback
func goReaddirCallback(de *C.struct_inode_dirent, ctx C.uintptr_t) C.int {
	id := uintptr(ctx)
	readdirMu.Lock()
	state := readdirStates[id]
	readdirMu.Unlock()
	if state == nil {
		return 1
	}
	nameLen := int(de.name_len)
	name := C.GoStringN(&de.name[0], C.int(nameLen))
	state.entries = append(state.entries, DirEntry{
		Name:  name,
		Inode: Ino(de.inode),
		Type:  uint8(de._type),
	})
	return 0
}

// Readdir returns all directory entries (excluding "." and "..").
func (fs *FS) Readdir(ino Ino) ([]DirEntry, error) {
	state := &readdirState{}

	readdirMu.Lock()
	readdirNextID++
	id := readdirNextID
	readdirStates[id] = state
	readdirMu.Unlock()

	defer func() {
		readdirMu.Lock()
		delete(readdirStates, id)
		readdirMu.Unlock()
	}()

	fs.mu.Lock()
	rc := C.lh_readdir_bridge(fs.mp, C.uint32_t(ino), C.uintptr_t(id))
	fs.mu.Unlock()
	if rc != 0 {
		return nil, rcError(fmt.Sprintf("lwext4: readdir(%d)", ino), rc)
	}
	return state.entries, nil
}

// Open opens a file by inode for reading.
func (fs *FS) Open(ino Ino) (*File, error) {
	return fs.OpenFile(ino, os.O_RDONLY)
}

// OpenFile opens a file by inode with the given flags (os.O_RDONLY, os.O_WRONLY, os.O_RDWR).
func (fs *FS) OpenFile(ino Ino, flags int) (*File, error) {
	// Convert Go os.O_* to POSIX O_* values that lwext4 expects.
	cFlags := 0
	switch flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_WRONLY:
		cFlags = 0x0001 // O_WRONLY
	case os.O_RDWR:
		cFlags = 0x0002 // O_RDWR
	default:
		cFlags = 0x0000 // O_RDONLY
	}

	fs.mu.Lock()
	f := C.lh_file_open(fs.mp, C.uint32_t(ino), C.int(cFlags))
	fs.mu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("lwext4: open(%d): failed", ino)
	}
	return &File{f: f, fs: fs}, nil
}

// File wraps an ext4_file handle.
// It implements io.Reader, io.Writer, io.ReaderAt, io.WriterAt, io.Seeker, and io.Closer.
type File struct {
	f  C.lh_file
	fs *FS
}

func (f *File) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.fs.mu.Lock()
	var rcnt C.size_t
	rc := C.lh_file_read(f.f, unsafe.Pointer(&p[0]), C.size_t(len(p)), &rcnt)
	f.fs.mu.Unlock()
	if rc != 0 {
		return int(rcnt), rcError("lwext4: fread", rc)
	}
	if rcnt == 0 {
		return 0, io.EOF
	}
	return int(rcnt), nil
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.fs.mu.Lock()
	rc := C.lh_file_seek(f.f, C.int64_t(off), 0) // SEEK_SET
	if rc != 0 {
		f.fs.mu.Unlock()
		return 0, rcError("lwext4: fseek", rc)
	}
	var rcnt C.size_t
	rc = C.lh_file_read(f.f, unsafe.Pointer(&p[0]), C.size_t(len(p)), &rcnt)
	f.fs.mu.Unlock()
	if rc != 0 {
		return int(rcnt), rcError("lwext4: fread", rc)
	}
	if int(rcnt) < len(p) {
		return int(rcnt), io.EOF
	}
	return int(rcnt), nil
}

func (f *File) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.fs.mu.Lock()
	var wcnt C.size_t
	rc := C.lh_file_write(f.f, unsafe.Pointer(&p[0]), C.size_t(len(p)), &wcnt)
	f.fs.mu.Unlock()
	if rc != 0 {
		return int(wcnt), rcError("lwext4: fwrite", rc)
	}
	return int(wcnt), nil
}

func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	// If writing past EOF, zero-fill the gap. lwext4's ext4_fwrite
	// allocates blocks but doesn't zero the gap between old EOF and the
	// write offset, violating POSIX sparse-file semantics.
	curSize := int64(C.lh_file_size(f.f))
	if off > curSize {
		if err := f.zeroFill(curSize, off-curSize); err != nil {
			return 0, err
		}
	}
	rc := C.lh_file_seek(f.f, C.int64_t(off), 0) // SEEK_SET
	if rc != 0 {
		return 0, rcError("lwext4: fseek", rc)
	}
	var wcnt C.size_t
	rc = C.lh_file_write(f.f, unsafe.Pointer(&p[0]), C.size_t(len(p)), &wcnt)
	if rc != 0 {
		return int(wcnt), rcError("lwext4: fwrite", rc)
	}
	return int(wcnt), nil
}

// zeroFill writes zeros from offset for length bytes.
func (f *File) zeroFill(offset, length int64) error {
	rc := C.lh_file_seek(f.f, C.int64_t(offset), 0)
	if rc != 0 {
		return rcError("lwext4: fseek (zerofill)", rc)
	}
	var zeros [4096]byte
	for length > 0 {
		n := int64(len(zeros))
		if n > length {
			n = length
		}
		var wcnt C.size_t
		rc = C.lh_file_write(f.f, unsafe.Pointer(&zeros[0]), C.size_t(n), &wcnt)
		if rc != 0 {
			return rcError("lwext4: fwrite (zerofill)", rc)
		}
		length -= int64(wcnt)
	}
	return nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	rc := C.lh_file_seek(f.f, C.int64_t(offset), C.uint32_t(whence))
	if rc != 0 {
		f.fs.mu.Unlock()
		return 0, rcError("lwext4: fseek", rc)
	}
	pos := C.lh_file_tell(f.f)
	f.fs.mu.Unlock()
	return int64(pos), nil
}

func (f *File) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	curSize := int64(C.lh_file_size(f.f))
	rc := C.lh_file_truncate(f.f, C.uint64_t(size))
	if rc != 0 {
		return rcError("lwext4: ftruncate", rc)
	}
	// If extending, zero-fill the new region. lwext4 allocates blocks
	// but may leave stale data in them.
	if size > curSize {
		if err := f.zeroFill(curSize, size-curSize); err != nil {
			return err
		}
	}
	return nil
}

func (f *File) Size() int64 {
	f.fs.mu.Lock()
	s := int64(C.lh_file_size(f.f))
	f.fs.mu.Unlock()
	return s
}

func (f *File) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	return f.fs.cacheFlushLocked()
}

func (f *File) Close() error {
	f.fs.mu.Lock()
	rc := C.lh_file_close(f.f)
	f.fs.mu.Unlock()
	if rc != 0 {
		return rcError("lwext4: fclose", rc)
	}
	return nil
}
