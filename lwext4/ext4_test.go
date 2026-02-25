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
