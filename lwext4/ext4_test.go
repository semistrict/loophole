package lwext4

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// memDev is a []byte-backed BlockDevice for testing.
type memDev struct {
	data []byte
}

func newMemDev(size int) *memDev {
	return &memDev{data: make([]byte, size)}
}

func (m *memDev) Read(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= uint64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(buf, m.data[offset:])
	return n, nil
}

func (m *memDev) Write(_ context.Context, data []byte, offset uint64) error {
	copy(m.data[offset:], data)
	return nil
}

func (m *memDev) ZeroRange(_ context.Context, offset, length uint64) error {
	clear(m.data[offset : offset+length])
	return nil
}

const testDevSize = 128 * 1024 * 1024 // 128 MB

func newTestFS(t *testing.T) *FS {
	t.Helper()
	dev := newMemDev(testDevSize)
	fs, err := Format(dev, int64(testDevSize), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	})
	return fs
}

func TestFormatAndClose(t *testing.T) {
	dev := newMemDev(testDevSize)
	fs, err := Format(dev, int64(testDevSize), nil)
	require.NoError(t, err)
	require.NoError(t, fs.Close())
}

func TestFormatWithJournal1GB(t *testing.T) {
	const size = 1 * 1024 * 1024 * 1024 // 1GB
	dev := newMemDev(size)
	fs, err := Format(dev, int64(size), nil) // journal enabled by default
	require.NoError(t, err)
	require.NoError(t, fs.Close())
}

func TestMountExisting(t *testing.T) {
	dev := newMemDev(testDevSize)
	fs, err := Format(dev, int64(testDevSize), nil)
	require.NoError(t, err)
	require.NoError(t, fs.Close())

	fs, err = Mount(dev, int64(testDevSize))
	require.NoError(t, err)
	require.NoError(t, fs.Close())
}

func TestRootInode(t *testing.T) {
	fs := newTestFS(t)

	attr, err := fs.GetAttr(RootIno)
	require.NoError(t, err)
	require.Equal(t, RootIno, attr.Ino)
	require.NotZero(t, attr.Mode&0o040000) // S_IFDIR
}

func TestMknodAndLookup(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "hello.txt", 0o644)
	require.NoError(t, err)
	require.NotZero(t, ino)

	found, err := fs.Lookup(RootIno, "hello.txt")
	require.NoError(t, err)
	require.Equal(t, ino, found)
}

func TestLookupNotFoundWrapsErrNotExist(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Lookup(RootIno, "nonexistent")
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCreateReadWrite(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "hello.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)
	data := []byte("Hello, lwext4!")
	n, err := f.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.NoError(t, f.Close())

	f, err = fs.Open(ino)
	require.NoError(t, err)
	buf, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, data, buf)
	require.NoError(t, f.Close())
}

func TestFileSeekAndSize(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "seek.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)
	_, err = f.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.Equal(t, int64(10), f.Size())

	pos, err := f.Seek(5, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(5), pos)

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("56789"), buf)
	require.NoError(t, f.Close())
}

func TestFileTruncate(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "trunc.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)
	_, err = f.Write([]byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, int64(13), f.Size())

	require.NoError(t, f.Truncate(5))
	require.Equal(t, int64(5), f.Size())
	require.NoError(t, f.Close())
}

func TestReadAtWriteAt(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "rw.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	n, err := f.WriteAt([]byte("ABCDE"), 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	n, err = f.WriteAt([]byte("XY"), 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	buf := make([]byte, 5)
	n, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("ABXYE"), buf)

	require.NoError(t, f.Close())
}

func TestGetAttrSizeAfterWriteClose(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "sized.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(4), f.Size())
	require.NoError(t, f.Close())

	// Reopen and check size.
	f2, err := fs.OpenFile(ino, os.O_RDONLY)
	require.NoError(t, err)
	require.Equal(t, int64(4), f2.Size())
	require.NoError(t, f2.Close())

	// Check via GetAttr.
	attr, err := fs.GetAttr(ino)
	require.NoError(t, err)
	require.Equal(t, uint64(4), attr.Size)

	// Check via Lookup + GetAttr.
	ino2, err := fs.Lookup(RootIno, "sized.txt")
	require.NoError(t, err)
	require.Equal(t, ino, ino2)
	attr2, err := fs.GetAttr(ino2)
	require.NoError(t, err)
	require.Equal(t, uint64(4), attr2.Size)
}

func TestMkdirAndReaddir(t *testing.T) {
	fs := newTestFS(t)

	dirIno, err := fs.Mkdir(RootIno, "subdir", 0o755)
	require.NoError(t, err)
	require.NotZero(t, dirIno)

	fileIno, err := fs.Mknod(dirIno, "file.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(fileIno, os.O_RDWR)
	require.NoError(t, err)
	_, err = f.Write([]byte("inside subdir"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	entries, err := fs.Readdir(dirIno)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file.txt", entries[0].Name)
	require.Equal(t, fileIno, entries[0].Inode)
}

func TestUnlink(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Mknod(RootIno, "a.txt", 0o644)
	require.NoError(t, err)

	require.NoError(t, fs.Unlink(RootIno, "a.txt"))

	_, err = fs.Lookup(RootIno, "a.txt")
	require.Error(t, err)
}

func TestRename(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "a.txt", 0o644)
	require.NoError(t, err)

	require.NoError(t, fs.Rename(RootIno, "a.txt", RootIno, "b.txt"))

	found, err := fs.Lookup(RootIno, "b.txt")
	require.NoError(t, err)
	require.Equal(t, ino, found)

	_, err = fs.Lookup(RootIno, "a.txt")
	require.Error(t, err)
}

func TestWriteAtPastEOF(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "sparse.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	// Write 5 bytes at offset 0.
	_, err = f.WriteAt([]byte("HELLO"), 0)
	require.NoError(t, err)
	require.Equal(t, int64(5), f.Size())

	// Write past EOF — gap should be zero-filled.
	_, err = f.WriteAt([]byte("WORLD"), 100)
	require.NoError(t, err)
	require.Equal(t, int64(105), f.Size())

	// Read the whole file and verify.
	buf := make([]byte, 105)
	n, err := f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 105, n)
	require.Equal(t, []byte("HELLO"), buf[:5])
	// Gap must be zeros.
	for i := 5; i < 100; i++ {
		require.Equal(t, byte(0), buf[i], "byte %d should be zero", i)
	}
	require.Equal(t, []byte("WORLD"), buf[100:])
	require.NoError(t, f.Close())
}

func TestTruncateExtendZeroFills(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "extend.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	_, err = f.Write([]byte("ABC"))
	require.NoError(t, err)

	// Extend to 1000 bytes.
	require.NoError(t, f.Truncate(1000))
	require.Equal(t, int64(1000), f.Size())

	// Read and verify: first 3 bytes are "ABC", rest are zeros.
	buf := make([]byte, 1000)
	n, err := f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 1000, n)
	require.Equal(t, []byte("ABC"), buf[:3])
	for i := 3; i < 1000; i++ {
		require.Equal(t, byte(0), buf[i], "byte %d should be zero", i)
	}
	require.NoError(t, f.Close())
}

func TestSeekPastEOF(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "seekpast.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	_, err = f.Write([]byte("data"))
	require.NoError(t, err)

	// Seek past EOF should succeed.
	pos, err := f.Seek(100, 0) // SEEK_SET
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)

	// Seek relative past EOF.
	pos, err = f.Seek(50, 1) // SEEK_CUR
	require.NoError(t, err)
	require.Equal(t, int64(150), pos)

	require.NoError(t, f.Close())
}

func TestGetSetAttr(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "perms.txt", 0o644)
	require.NoError(t, err)

	require.NoError(t, fs.SetAttr(ino, &Attr{Mode: 0o100755}, AttrMode))
	attr, err := fs.GetAttr(ino)
	require.NoError(t, err)
	require.Equal(t, uint32(0o755), attr.Mode&0o777)

	require.NoError(t, fs.SetAttr(ino, &Attr{Uid: 1000, Gid: 1000}, AttrUid|AttrGid))
	attr, err = fs.GetAttr(ino)
	require.NoError(t, err)
	require.Equal(t, uint32(1000), attr.Uid)
	require.Equal(t, uint32(1000), attr.Gid)
}

// TestSetAttrPreservesFileType verifies that SetAttr with AttrMode does not
// clobber the file type bits (S_IFDIR, S_IFREG, S_IFLNK). This reproduces
// the bug where FUSE Setattr(mode=0755) on a directory would turn it into
// a regular file by wiping the S_IFDIR bits.
//
// FUSE sends permission-only mode (no S_IFDIR/S_IFREG bits) in SetAttr,
// so inode_setattr must preserve the existing file type bits.
func TestSetAttrPreservesFileType(t *testing.T) {
	fs := newTestFS(t)

	// Create a directory and verify it has S_IFDIR.
	dirIno, err := fs.Mkdir(RootIno, "mydir", 0o755)
	require.NoError(t, err)
	attr, err := fs.GetAttr(dirIno)
	require.NoError(t, err)
	require.Equal(t, uint32(0o40755), attr.Mode, "directory should have S_IFDIR|0755")

	// SetAttr with permission-only mode (no S_IFDIR — this is what FUSE sends).
	require.NoError(t, fs.SetAttr(dirIno, &Attr{Mode: 0o700}, AttrMode))
	attr, err = fs.GetAttr(dirIno)
	require.NoError(t, err)
	require.Equal(t, uint32(0o40700), attr.Mode, "directory should still have S_IFDIR after SetAttr with perm-only mode")

	// Create a regular file and verify it has S_IFREG.
	fileIno, err := fs.Mknod(RootIno, "myfile", 0o644)
	require.NoError(t, err)
	attr, err = fs.GetAttr(fileIno)
	require.NoError(t, err)
	require.Equal(t, uint32(0o100644), attr.Mode, "file should have S_IFREG|0644")

	// SetAttr with permission-only mode (no S_IFREG — this is what FUSE sends).
	require.NoError(t, fs.SetAttr(fileIno, &Attr{Mode: 0o755}, AttrMode))
	attr, err = fs.GetAttr(fileIno)
	require.NoError(t, err)
	require.Equal(t, uint32(0o100755), attr.Mode, "file should still have S_IFREG after SetAttr with perm-only mode")
}

func TestRmdir(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Mkdir(RootIno, "rmdir", 0o755)
	require.NoError(t, err)

	require.NoError(t, fs.Rmdir(RootIno, "rmdir"))

	_, err = fs.Lookup(RootIno, "rmdir")
	require.Error(t, err)
}

func TestRmdirNonEmpty(t *testing.T) {
	fs := newTestFS(t)

	dirIno, err := fs.Mkdir(RootIno, "notempty", 0o755)
	require.NoError(t, err)

	_, err = fs.Mknod(dirIno, "file.txt", 0o644)
	require.NoError(t, err)

	err = fs.Rmdir(RootIno, "notempty")
	require.Error(t, err)
}

func TestLink(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "original.txt", 0o644)
	require.NoError(t, err)

	require.NoError(t, fs.Link(ino, RootIno, "hardlink.txt"))

	found, err := fs.Lookup(RootIno, "hardlink.txt")
	require.NoError(t, err)
	require.Equal(t, ino, found)

	attr, err := fs.GetAttr(ino)
	require.NoError(t, err)
	require.Equal(t, uint16(2), attr.Links)
}

func TestSymlink(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Mknod(RootIno, "target.txt", 0o644)
	require.NoError(t, err)

	linkIno, err := fs.Symlink(RootIno, "link.txt", "target.txt")
	require.NoError(t, err)
	require.NotZero(t, linkIno)

	target, err := fs.Readlink(linkIno)
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)
}

// TestTruncateDownThenWritePastEOF reproduces the fsx failure pattern:
// write non-zero data, truncate down, write past EOF to extend,
// then read the hole region — it must be zeros.
func TestTruncateDownThenWritePastEOF(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "trunc-hole.txt", 0o644)
	require.NoError(t, err)

	f, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	// Step 1: Write 128KB of 0xAA.
	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = 0xAA
	}
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	// Step 2: Truncate down to 16KB — bytes 16K-128K are gone.
	require.NoError(t, f.Truncate(16*1024))

	// Step 3: Write at the end to extend back to 128KB.
	// The region 16K to (128K-6) should be a zero-filled hole.
	marker := []byte("MARKER")
	_, err = f.WriteAt(marker, 128*1024-int64(len(marker)))
	require.NoError(t, err)

	// Step 4: Read the hole and verify it's all zeros.
	holeLen := 128*1024 - 16*1024 - len(marker)
	hole := make([]byte, holeLen)
	_, err = f.ReadAt(hole, 16*1024)
	require.NoError(t, err)
	for i := range hole {
		if hole[i] != 0 {
			t.Fatalf("byte %d (offset 0x%x): expected 0x00, got 0x%02x", i, 16*1024+i, hole[i])
		}
	}
	require.NoError(t, f.Close())
}

// TestTruncateViaSeparateHandle reproduces the FUSE Setattr pattern:
// write data via handle A, truncate via a SEPARATE handle B (as Setattr does),
// then write past EOF via handle A, read back — hole must be zeros.
func TestTruncateViaSeparateHandle(t *testing.T) {
	fs := newTestFS(t)

	ino, err := fs.Mknod(RootIno, "trunc-sep.txt", 0o644)
	require.NoError(t, err)

	// Handle A: write 128KB of 0xAA.
	fA, err := fs.OpenFile(ino, os.O_RDWR)
	require.NoError(t, err)

	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = 0xAA
	}
	_, err = fA.WriteAt(data, 0)
	require.NoError(t, err)

	// Handle B: truncate down to 16KB (mimics FUSE Setattr).
	fB, err := fs.OpenFile(ino, os.O_WRONLY)
	require.NoError(t, err)
	require.NoError(t, fB.Truncate(16*1024))
	require.NoError(t, fB.Close())

	// Handle A: write past EOF to extend back to 128KB.
	marker := []byte("MARKER")
	_, err = fA.WriteAt(marker, 128*1024-int64(len(marker)))
	require.NoError(t, err)

	// Handle A: read the hole — must be zeros.
	holeLen := 128*1024 - 16*1024 - len(marker)
	hole := make([]byte, holeLen)
	_, err = fA.ReadAt(hole, 16*1024)
	require.NoError(t, err)
	for i := range hole {
		if hole[i] != 0 {
			t.Fatalf("byte %d (offset 0x%x): expected 0x00, got 0x%02x", i, 16*1024+i, hole[i])
		}
	}
	require.NoError(t, fA.Close())
}

// tarEntry describes a filesystem entry for TestTarExtractRemount.
type tarEntry struct {
	name    string
	dir     bool
	symlink string // non-empty = symlink target
	data    []byte // file data (ignored for dirs/symlinks)
	mode    uint32 // permission bits (e.g. 0o755)
}

// TestTarExtractRemount simulates what "tar xf rootfs.tar" does through FUSE:
// create directories, files, and symlinks, set permissions (chmod), write data,
// then close/remount the filesystem and verify everything is readable.
//
// This reproduces the bug where readdir returns EIO and some inodes are
// inaccessible after remount.
func TestTarExtractRemount(t *testing.T) {
	dev := newMemDev(testDevSize)

	// Phase 1: Format and populate (simulates tar extraction).
	fs1, err := Format(dev, int64(testDevSize), nil)
	require.NoError(t, err)

	// Mimic a Debian rootfs layout.
	entries := []tarEntry{
		{name: "etc", dir: true, mode: 0o755},
		{name: "boot", dir: true, mode: 0o755},
		{name: "var", dir: true, mode: 0o755},
		{name: "usr", dir: true, mode: 0o755},
		{name: "usr/bin", dir: true, mode: 0o755},
		{name: "usr/lib", dir: true, mode: 0o755},
		{name: "usr/sbin", dir: true, mode: 0o755},
		{name: "home", dir: true, mode: 0o755},
		{name: "root", dir: true, mode: 0o700},
		{name: "tmp", dir: true, mode: 0o1777},
		{name: "dev", dir: true, mode: 0o755},
		{name: "proc", dir: true, mode: 0o755},
		{name: "sys", dir: true, mode: 0o755},
		{name: "run", dir: true, mode: 0o755},
		{name: "mnt", dir: true, mode: 0o755},
		{name: "opt", dir: true, mode: 0o755},
		{name: "srv", dir: true, mode: 0o755},
		{name: "media", dir: true, mode: 0o755},
		{name: "var/log", dir: true, mode: 0o755},
		{name: "var/tmp", dir: true, mode: 0o1777},
		{name: "var/lib", dir: true, mode: 0o755},
		{name: "etc/apt", dir: true, mode: 0o755},
		{name: "etc/default", dir: true, mode: 0o755},
		{name: "etc/network", dir: true, mode: 0o755},
		{name: "etc/systemd", dir: true, mode: 0o755},
		{name: "bin", symlink: "usr/bin"},
		{name: "lib", symlink: "usr/lib"},
		{name: "sbin", symlink: "usr/sbin"},
		{name: "vmlinuz", symlink: "boot/vmlinuz-6.1.0"},
		{name: "vmlinuz.old", symlink: "boot/vmlinuz-6.1.0"},
		{name: "etc/hostname", data: []byte("loophole\n"), mode: 0o644},
		{name: "etc/passwd", data: []byte("root:x:0:0:root:/root:/bin/bash\n"), mode: 0o644},
		{name: "etc/fstab", data: []byte("# /etc/fstab\n"), mode: 0o644},
		{name: "boot/vmlinuz-6.1.0", data: make([]byte, 8192), mode: 0o644},
		{name: ".dockerenv", data: []byte{}, mode: 0o755},
	}

	// Track created inodes for nested paths.
	dirInos := map[string]Ino{"": RootIno}

	for _, e := range entries {
		// Find parent directory.
		parentPath := ""
		baseName := e.name
		for i := len(e.name) - 1; i >= 0; i-- {
			if e.name[i] == '/' {
				parentPath = e.name[:i]
				baseName = e.name[i+1:]
				break
			}
		}
		parentIno := dirInos[parentPath]

		if e.dir {
			ino, err := fs1.Mkdir(parentIno, baseName, e.mode|0o040000)
			require.NoError(t, err, "mkdir %s", e.name)
			dirInos[e.name] = ino
			// Simulate tar's chmod after mkdir (FUSE sends perm-only mode).
			require.NoError(t, fs1.SetAttr(ino, &Attr{Mode: e.mode}, AttrMode))
		} else if e.symlink != "" {
			_, err := fs1.Symlink(parentIno, baseName, e.symlink)
			require.NoError(t, err, "symlink %s -> %s", e.name, e.symlink)
		} else {
			ino, err := fs1.Mknod(parentIno, baseName, e.mode|0o100000)
			require.NoError(t, err, "mknod %s", e.name)
			if len(e.data) > 0 {
				f, err := fs1.OpenFile(ino, os.O_WRONLY)
				require.NoError(t, err)
				_, err = f.Write(e.data)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			// Simulate tar's chmod.
			require.NoError(t, fs1.SetAttr(ino, &Attr{Mode: e.mode}, AttrMode))
		}
	}

	require.NoError(t, fs1.Close())

	// Phase 2: Remount and verify everything is accessible.
	fs2, err := Mount(dev, int64(testDevSize))
	require.NoError(t, err)

	// Verify root directory is readable.
	rootEntries, err := fs2.Readdir(RootIno)
	require.NoError(t, err, "readdir root")
	require.NotEmpty(t, rootEntries, "root should have entries")

	// Build a name->DirEntry map for root.
	rootMap := map[string]DirEntry{}
	for _, de := range rootEntries {
		rootMap[de.Name] = de
	}

	// Verify each top-level entry from our input.
	for _, e := range entries {
		// Only check top-level entries.
		topName := e.name
		for i := range e.name {
			if e.name[i] == '/' {
				topName = ""
				break
			}
		}
		if topName == "" {
			continue
		}

		de, ok := rootMap[topName]
		require.True(t, ok, "root should contain %q", topName)

		// Stat the inode.
		attr, err := fs2.GetAttr(de.Inode)
		require.NoError(t, err, "getattr %s (ino %d)", topName, de.Inode)

		if e.dir {
			require.Equal(t, uint32(0o40000), attr.Mode&0xF000,
				"%s should be a directory, got mode 0o%o", topName, attr.Mode)
			// Read directory contents.
			children, err := fs2.Readdir(de.Inode)
			require.NoError(t, err, "readdir %s", topName)
			_ = children
		} else if e.symlink != "" {
			require.Equal(t, uint32(0o120000), attr.Mode&0xF000,
				"%s should be a symlink, got mode 0o%o", topName, attr.Mode)
			target, err := fs2.Readlink(de.Inode)
			require.NoError(t, err, "readlink %s", topName)
			require.Equal(t, e.symlink, target)
		} else {
			require.Equal(t, uint32(0o100000), attr.Mode&0xF000,
				"%s should be a regular file, got mode 0o%o", topName, attr.Mode)
			if len(e.data) > 0 {
				f, err := fs2.Open(de.Inode)
				require.NoError(t, err, "open %s", topName)
				got, err := io.ReadAll(f)
				require.NoError(t, err, "read %s", topName)
				require.Equal(t, e.data, got, "data mismatch for %s", topName)
				require.NoError(t, f.Close())
			}
		}
	}

	// Verify nested directories are also readable.
	for _, nested := range []string{"var/log", "var/tmp", "var/lib", "etc/apt", "etc/default", "usr/bin"} {
		// Walk the path.
		parts := []string{}
		start := 0
		for i := range nested {
			if nested[i] == '/' {
				parts = append(parts, nested[start:i])
				start = i + 1
			}
		}
		parts = append(parts, nested[start:])

		ino := RootIno
		for _, part := range parts {
			childIno, err := fs2.Lookup(ino, part)
			require.NoError(t, err, "lookup %s in path %s", part, nested)
			ino = childIno
		}

		attr, err := fs2.GetAttr(ino)
		require.NoError(t, err, "getattr %s", nested)
		require.Equal(t, uint32(0o40000), attr.Mode&0xF000,
			"%s should be a directory, got mode 0o%o", nested, attr.Mode)

		children, err := fs2.Readdir(ino)
		require.NoError(t, err, "readdir %s", nested)
		_ = children
	}

	// Phase 3: Create a large directory with many entries to trigger htree/dir_index.
	// The Debian /etc has ~200 entries. Create a similar scale.
	etcIno, err := fs2.Lookup(RootIno, "etc")
	require.NoError(t, err)

	for i := range 200 {
		name := fmt.Sprintf("config_%03d.conf", i)
		ino, err := fs2.Mknod(etcIno, name, 0o100644)
		require.NoError(t, err, "mknod etc/%s", name)
		f, err := fs2.OpenFile(ino, os.O_WRONLY)
		require.NoError(t, err)
		_, err = fmt.Fprintf(f, "# config %d\n", i)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	require.NoError(t, fs2.CacheFlush())
	require.NoError(t, fs2.Close())

	// Remount again and verify large directory is readable.
	fs3, err := Mount(dev, int64(testDevSize))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, fs3.Close())
	}()

	etcIno2, err := fs3.Lookup(RootIno, "etc")
	require.NoError(t, err)
	etcEntries, err := fs3.Readdir(etcIno2)
	require.NoError(t, err, "readdir etc after large fill")
	// Original etc had: hostname, passwd, fstab, apt, default, network, systemd = 7
	// Plus 200 config files = 207 total.
	require.Len(t, etcEntries, 207, "etc should have 207 entries")

	// Stat every entry.
	for _, de := range etcEntries {
		attr, err := fs3.GetAttr(de.Inode)
		require.NoError(t, err, "getattr etc/%s (ino %d)", de.Name, de.Inode)
		_ = attr
	}

	// Also verify root is still readable.
	rootEntries2, err := fs3.Readdir(RootIno)
	require.NoError(t, err, "readdir root after remount 2")
	require.NotEmpty(t, rootEntries2)

	// Verify every root entry is stat-able.
	for _, de := range rootEntries2 {
		_, err := fs3.GetAttr(de.Inode)
		require.NoError(t, err, "getattr root/%s (ino %d)", de.Name, de.Inode)
	}
}
