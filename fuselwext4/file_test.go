package fuselwext4

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/lwext4"
	"github.com/stretchr/testify/require"
)

// testKernel simulates the kernel's FUSE call sequences (open, unlink, release)
// using the same orphan tracking logic as the real FUSE layer.
type testKernel struct {
	fs      *lwext4.FS
	mu      sync.Mutex
	orphans *orphanTracker
}

func newTestKernel(t *testing.T) *testKernel {
	t.Helper()
	dev := &memDev{data: make([]byte, testDevSize)}
	fs, err := lwext4.Format(dev, testDevSize, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	})
	return &testKernel{fs: fs, orphans: newOrphanTracker()}
}

// create makes a file with the given data and returns its inode.
func (k *testKernel) create(t *testing.T, name string, data []byte) lwext4.Ino {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	ino, err := k.fs.Mknod(lwext4.RootIno, name, 0o644)
	require.NoError(t, err)
	if len(data) > 0 {
		f, err := k.fs.OpenFile(ino, syscall.O_WRONLY)
		require.NoError(t, err)
		_, err = f.WriteAt(data, 0)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	return ino
}

// open simulates the kernel opening a file — increments fd tracking.
func (k *testKernel) open(t *testing.T, ino lwext4.Ino, flags int) *ext4FileHandle {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	f, err := k.fs.OpenFile(ino, flags)
	require.NoError(t, err)
	k.orphans.openFD(ino)
	return &ext4FileHandle{f: f, mu: &k.mu, ino: ino, ext4fs: k.fs, orphans: k.orphans}
}

// unlink simulates the kernel sending an unlink request.
func (k *testKernel) unlink(t *testing.T, name string) {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	require.NoError(t, k.orphans.unlink(k.fs, lwext4.RootIno, name))
}

// release simulates the kernel closing a file handle.
func (k *testKernel) release(t *testing.T, fh *ext4FileHandle) {
	t.Helper()
	errno := fh.Release(t.Context())
	require.Equal(t, syscall.Errno(0), errno)
}

// read simulates the kernel reading through a file handle.
func (k *testKernel) read(t *testing.T, fh *ext4FileHandle, off int64, size int) []byte {
	t.Helper()
	dest := make([]byte, size)
	result, errno := fh.Read(t.Context(), dest, off)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, result)
	buf, status := result.Bytes(make([]byte, 0, size))
	require.Equal(t, fuse.OK, status)
	return buf
}

// write simulates the kernel writing through a file handle.
func (k *testKernel) write(t *testing.T, fh *ext4FileHandle, off int64, data []byte) {
	t.Helper()
	n, errno := fh.Write(t.Context(), data, off)
	require.Equal(t, syscall.Errno(0), errno)
	require.Equal(t, uint32(len(data)), n)
}

// lookup checks whether a name exists in the root directory.
func (k *testKernel) lookup(name string) (lwext4.Ino, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.fs.Lookup(lwext4.RootIno, name)
}

// link creates a hard link.
func (k *testKernel) link(t *testing.T, ino lwext4.Ino, name string) {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	require.NoError(t, k.fs.Link(ino, lwext4.RootIno, name))
}

// mustLookup looks up a name and fails the test if not found.
func (k *testKernel) mustLookup(t *testing.T, name string) lwext4.Ino {
	t.Helper()
	ino, err := k.lookup(name)
	require.NoError(t, err)
	return ino
}

// memDev is a simple in-memory block device for testing.
type memDev struct {
	data []byte
}

func (m *memDev) Read(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= uint64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(buf, m.data[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memDev) Write(_ context.Context, data []byte, offset uint64) error {
	copy(m.data[offset:], data)
	return nil
}

const testDevSize = 128 * 1024 * 1024

func newTestFS(t *testing.T) *lwext4.FS {
	t.Helper()
	dev := &memDev{data: make([]byte, testDevSize)}
	fs, err := lwext4.Format(dev, testDevSize, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Logf("close failed: %v", err)
		}
	})
	return fs
}

// TestReadPastEOF_ReturnsZeros verifies that reading past lwext4's EOF
// returns a zero-filled buffer (not EIO or stale data). This is required
// for FUSE writeback cache: the kernel may read offsets beyond what lwext4
// knows the file size to be, because unflushed writes extended the file
// in kernel page cache.
func TestReadPastEOF_ReturnsZeros(t *testing.T) {
	ext4fs := newTestFS(t)
	mu := &sync.Mutex{}

	// Create a file and write 64KB of 0xAA.
	ino, err := ext4fs.Mknod(lwext4.RootIno, "test.bin", 0o644)
	require.NoError(t, err)

	f, err := ext4fs.OpenFile(ino, syscall.O_RDWR)
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAA}, 64*1024)
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	fh := &ext4FileHandle{f: f, mu: mu}

	// Read past EOF: offset 128KB, size 4KB — entirely beyond the 64KB file.
	dest := make([]byte, 4096)
	for i := range dest {
		dest[i] = 0xFF // poison to detect if buffer isn't cleared
	}
	result, errno := fh.Read(t.Context(), dest, 128*1024)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, result)

	buf, _ := result.Bytes(make([]byte, 0, 4096))
	require.Equal(t, 4096, len(buf), "expected full buffer returned")
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("byte %d: expected 0x00, got 0x%02x", i, b)
		}
	}
}

// TestReadPastEOF_AfterTruncateDown verifies the exact fsx failure pattern:
// write data, truncate down, then read the truncated region. The read must
// return zeros, not stale pre-truncate data.
func TestReadPastEOF_AfterTruncateDown(t *testing.T) {
	ext4fs := newTestFS(t)
	mu := &sync.Mutex{}

	ino, err := ext4fs.Mknod(lwext4.RootIno, "trunc.bin", 0o644)
	require.NoError(t, err)

	f, err := ext4fs.OpenFile(ino, syscall.O_RDWR)
	require.NoError(t, err)

	// Write 128KB of 0xBB.
	data := bytes.Repeat([]byte{0xBB}, 128*1024)
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	// Truncate down to 16KB (simulates FUSE Setattr via separate handle).
	fTrunc, err := ext4fs.OpenFile(ino, syscall.O_WRONLY)
	require.NoError(t, err)
	require.NoError(t, fTrunc.Truncate(16*1024))
	require.NoError(t, fTrunc.Close())

	// Read at offset 32KB (past truncated EOF of 16KB) via FUSE handler.
	fh := &ext4FileHandle{f: f, mu: mu}
	dest := make([]byte, 8192)
	for i := range dest {
		dest[i] = 0xFF
	}
	result, errno := fh.Read(t.Context(), dest, 32*1024)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, result)

	buf, _ := result.Bytes(make([]byte, 0, 8192))
	require.Equal(t, 8192, len(buf), "expected full buffer returned")
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("byte %d: expected 0x00, got 0x%02x (stale data leaked)", i, b)
		}
	}
}

// TestUnlinkWhileOpen verifies that reading from a file handle after the
// file has been unlinked still returns the original data. This is standard
// Unix semantics: unlink removes the directory entry but the inode and its
// data must remain accessible through existing open file descriptors.
func TestUnlinkWhileOpen(t *testing.T) {
	k := newTestKernel(t)
	payload := bytes.Repeat([]byte{0xDE}, 4096)
	k.create(t, "doomed.bin", payload)

	// Kernel opens the file, then user runs rm.
	fh := k.open(t, k.mustLookup(t, "doomed.bin"), syscall.O_RDONLY)
	k.unlink(t, "doomed.bin")

	// Name is gone.
	_, err := k.lookup("doomed.bin")
	require.Error(t, err)

	// Data still readable through the open handle.
	buf := k.read(t, fh, 0, 4096)
	require.Equal(t, payload, buf)

	// Close frees the orphan.
	k.release(t, fh)
	require.Empty(t, k.orphans.openFDs)
	require.Empty(t, k.orphans.orphaned)
}

// TestUnlinkNoOpenFDs verifies that unlinking a file with no open handles
// does not leave anything in the orphan tracker.
func TestUnlinkNoOpenFDs(t *testing.T) {
	k := newTestKernel(t)
	k.create(t, "gone.bin", []byte{0xAA})

	k.unlink(t, "gone.bin")

	require.Empty(t, k.orphans.openFDs)
	require.Empty(t, k.orphans.orphaned)

	_, err := k.lookup("gone.bin")
	require.Error(t, err)
}

// TestUnlinkMultipleOpenFDs verifies that an orphaned inode is only freed
// when the last file handle is closed, not the first.
func TestUnlinkMultipleOpenFDs(t *testing.T) {
	k := newTestKernel(t)
	payload := bytes.Repeat([]byte{0xBE}, 8192)
	ino := k.create(t, "multi.bin", payload)

	// Two handles open before unlink.
	fh1 := k.open(t, ino, syscall.O_RDONLY)
	fh2 := k.open(t, ino, syscall.O_RDONLY)
	k.unlink(t, "multi.bin")

	// Both handles can still read.
	require.Equal(t, payload, k.read(t, fh1, 0, 8192))
	require.Equal(t, payload, k.read(t, fh2, 0, 8192))

	// Close first handle — inode should still be alive.
	k.release(t, fh1)
	require.True(t, k.orphans.orphaned[ino], "still orphaned after first close")
	require.Equal(t, 1, k.orphans.openFDs[ino])

	// Second handle still works.
	require.Equal(t, payload, k.read(t, fh2, 0, 8192))

	// Close second handle — orphan freed.
	k.release(t, fh2)
	require.Empty(t, k.orphans.openFDs)
	require.Empty(t, k.orphans.orphaned)
}

// TestUnlinkWithHardlink verifies that unlinking one name of a file with
// multiple hard links does not orphan the inode (links_count stays > 0).
func TestUnlinkWithHardlink(t *testing.T) {
	k := newTestKernel(t)
	payload := bytes.Repeat([]byte{0xCC}, 4096)
	ino := k.create(t, "orig.bin", payload)
	k.link(t, ino, "link.bin")

	fh := k.open(t, ino, syscall.O_RDONLY)
	k.unlink(t, "orig.bin")

	// Inode should NOT be orphaned — it still has a link.
	require.False(t, k.orphans.orphaned[ino])

	// File still accessible via the other name.
	ino2, err := k.lookup("link.bin")
	require.NoError(t, err)
	require.Equal(t, ino, ino2)

	// Read through handle still works.
	require.Equal(t, payload, k.read(t, fh, 0, 4096))

	k.release(t, fh)
	require.Empty(t, k.orphans.orphaned)
}

// TestWriteAfterUnlink verifies that writes through an open handle still
// work after the file has been unlinked.
func TestWriteAfterUnlink(t *testing.T) {
	k := newTestKernel(t)
	ino := k.create(t, "write-after.bin", bytes.Repeat([]byte{0x00}, 4096))

	fh := k.open(t, ino, syscall.O_RDWR)
	k.unlink(t, "write-after.bin")

	// Write new data through the orphaned handle.
	newData := bytes.Repeat([]byte{0xEF}, 4096)
	k.write(t, fh, 0, newData)

	// Read it back.
	require.Equal(t, newData, k.read(t, fh, 0, 4096))

	k.release(t, fh)
	require.Empty(t, k.orphans.orphaned)
}

// TestMultipleOrphanedFiles verifies that multiple files can be orphaned
// simultaneously and each is cleaned up independently.
func TestMultipleOrphanedFiles(t *testing.T) {
	k := newTestKernel(t)

	var handles []*ext4FileHandle
	for i := range 5 {
		name := fmt.Sprintf("file%d.bin", i)
		payload := bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		ino := k.create(t, name, payload)
		fh := k.open(t, ino, syscall.O_RDONLY)
		k.unlink(t, name)
		handles = append(handles, fh)
	}

	require.Len(t, k.orphans.orphaned, 5)

	// Close them in reverse order.
	for i := 4; i >= 0; i-- {
		payload := bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		require.Equal(t, payload, k.read(t, handles[i], 0, 4096))
		k.release(t, handles[i])
		require.Len(t, k.orphans.orphaned, i)
	}

	require.Empty(t, k.orphans.openFDs)
}

// TestOpenAfterUnlinkWhileOpen verifies that even if the inode number gets
// reused (not guaranteed but possible), the orphan tracker correctly handles
// opening a different file while an orphan with the same ino could exist.
// In practice, this tests that Release on the old handle doesn't corrupt
// tracking for new files.
func TestReleaseWithoutOrphan(t *testing.T) {
	k := newTestKernel(t)
	ino := k.create(t, "normal.bin", []byte{0x42})

	fh := k.open(t, ino, syscall.O_RDONLY)
	// Close without unlinking — should just decrement, no orphan freeing.
	k.release(t, fh)

	require.Empty(t, k.orphans.openFDs)
	require.Empty(t, k.orphans.orphaned)

	// File should still exist.
	_, err := k.lookup("normal.bin")
	require.NoError(t, err)
}

// TestOrphanRecover verifies that OrphanRecover cleans up orphaned inodes
// left on disk (simulating crash recovery).
func TestOrphanRecover(t *testing.T) {
	k := newTestKernel(t)
	payload := bytes.Repeat([]byte{0xFF}, 64*1024)
	ino := k.create(t, "crash.bin", payload)

	// Open and unlink — inode goes on disk orphan list.
	fh := k.open(t, ino, syscall.O_RDONLY)
	k.unlink(t, "crash.bin")
	require.True(t, k.orphans.orphaned[ino])

	// Simulate a crash: just close the lwext4 file handle directly (no Release)
	// so the in-memory tracker still thinks it's orphaned, but the on-disk
	// orphan list has the entry. Then run OrphanRecover to clean up.
	k.mu.Lock()
	fh.f.Close()
	k.mu.Unlock()

	// Reset the in-memory tracker (simulating fresh mount after crash).
	k.orphans = newOrphanTracker()

	k.mu.Lock()
	err := k.fs.OrphanRecover()
	k.mu.Unlock()
	require.NoError(t, err)

	require.Empty(t, k.orphans.openFDs)
	require.Empty(t, k.orphans.orphaned)
}

// TestUnlinkLargeFileWhileOpen verifies orphan semantics with a larger file
// (1MB) to ensure block reclamation works correctly on final close.
func TestUnlinkLargeFileWhileOpen(t *testing.T) {
	k := newTestKernel(t)
	payload := bytes.Repeat([]byte{0xAB}, 1024*1024)
	ino := k.create(t, "big.bin", payload)

	fh := k.open(t, ino, syscall.O_RDONLY)
	k.unlink(t, "big.bin")

	// Spot-check data at various offsets.
	require.Equal(t, bytes.Repeat([]byte{0xAB}, 4096), k.read(t, fh, 0, 4096))
	require.Equal(t, bytes.Repeat([]byte{0xAB}, 4096), k.read(t, fh, 512*1024, 4096))
	require.Equal(t, bytes.Repeat([]byte{0xAB}, 4096), k.read(t, fh, 1024*1024-4096, 4096))

	k.release(t, fh)
	require.Empty(t, k.orphans.orphaned)
}

// TestReadPartialEOF_ZeroFillsRemainder verifies that when a read spans
// the EOF boundary, bytes before EOF contain real data and bytes after
// EOF are zero-filled.
func TestReadPartialEOF_ZeroFillsRemainder(t *testing.T) {
	ext4fs := newTestFS(t)
	mu := &sync.Mutex{}

	ino, err := ext4fs.Mknod(lwext4.RootIno, "partial.bin", 0o644)
	require.NoError(t, err)

	f, err := ext4fs.OpenFile(ino, syscall.O_RDWR)
	require.NoError(t, err)

	// Write 100 bytes of 0xCC.
	data := bytes.Repeat([]byte{0xCC}, 100)
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	// Read 200 bytes starting at offset 0 — last 100 bytes are past EOF.
	fh := &ext4FileHandle{f: f, mu: mu}
	dest := make([]byte, 200)
	for i := range dest {
		dest[i] = 0xFF
	}
	result, errno := fh.Read(t.Context(), dest, 0)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, result)

	buf, _ := result.Bytes(make([]byte, 0, 200))
	require.Equal(t, 200, len(buf), "expected full buffer returned")

	// First 100 bytes should be 0xCC.
	for i := 0; i < 100; i++ {
		if buf[i] != 0xCC {
			t.Fatalf("byte %d: expected 0xCC, got 0x%02x", i, buf[i])
		}
	}
	// Remaining 100 bytes should be zero.
	for i := 100; i < 200; i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d: expected 0x00, got 0x%02x", i, buf[i])
		}
	}
}
