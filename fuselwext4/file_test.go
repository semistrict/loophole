package fuselwext4

import (
	"bytes"
	"io"
	"sync"
	"syscall"
	"testing"

	"github.com/semistrict/loophole/lwext4"
	"github.com/stretchr/testify/require"
)

// memDev is a simple in-memory block device for testing.
type memDev struct {
	data []byte
}

func (m *memDev) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memDev) WriteAt(p []byte, off int64) (int, error) {
	copy(m.data[off:], p)
	return len(p), nil
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
