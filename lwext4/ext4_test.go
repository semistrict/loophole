package lwext4

import (
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

func (m *memDev) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *memDev) WriteAt(p []byte, off int64) (int, error) {
	n := copy(m.data[off:], p)
	return n, nil
}

const testDevSize = 128 * 1024 * 1024 // 128 MB

func newTestFS(t *testing.T) *FS {
	t.Helper()
	dev := newMemDev(testDevSize)
	fs, err := Format(dev, int64(testDevSize), nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close() })
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
