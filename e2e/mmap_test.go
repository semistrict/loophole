//go:build linux

package e2e

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_MmapTruncateStaleData reproduces the fsx failure pattern:
// write data, truncate down, extend via write past EOF, then mmap-read
// the region that was truncated away. It should be zeros.
func TestE2E_MmapTruncateStaleData(t *testing.T) {
	mp := stressMountGo(t, "mmap-trunc")
	path := mp.mountpoint + "/mmap-test"

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Step 1: Write 128KB of non-zero data.
	data := make([]byte, 128*1024)
	for i := range data {
		data[i] = 0xAA
	}
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	// Step 2: Truncate down to 16KB — bytes 16K-128K are gone.
	require.NoError(t, f.Truncate(16*1024))

	// Step 3: Extend file back to 128KB by writing at the end.
	// The region 16K-127K should be a zero-filled hole.
	marker := []byte("MARKER")
	_, err = f.WriteAt(marker, 128*1024-int64(len(marker)))
	require.NoError(t, err)

	// Sync to make sure everything is flushed.
	require.NoError(t, f.Sync())

	// Step 4a: read() the hole region and check it's zeros.
	hole := make([]byte, 128*1024-16*1024-len(marker))
	_, err = f.ReadAt(hole, 16*1024)
	require.NoError(t, err)
	for i := range hole {
		if hole[i] != 0 {
			t.Fatalf("read() byte %d (offset 0x%x): expected 0x00, got 0x%02x", i, 16*1024+i, hole[i])
		}
	}

	// Step 4b: mmap the file and check the hole region is zeros.
	size := 128 * 1024
	mapped, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	require.NoError(t, err)
	defer unix.Munmap(mapped)

	for i := 16 * 1024; i < 128*1024-len(marker); i++ {
		if mapped[i] != 0 {
			t.Fatalf("mmap byte %d (0x%x): expected 0x00, got 0x%02x", i, i, mapped[i])
		}
	}
}

// TestE2E_MmapWriteReadback writes via mmap then reads back via read()
// to verify mmap writes are properly flushed through FUSE.
func TestE2E_MmapWriteReadback(t *testing.T) {
	mp := stressMountGo(t, "mmap-write")
	path := mp.mountpoint + "/mmap-wr"

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Create a 64KB file of zeros.
	require.NoError(t, f.Truncate(64*1024))

	// mmap it writable.
	mapped, err := unix.Mmap(int(f.Fd()), 0, 64*1024, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)

	// Write a pattern via mmap.
	for i := range mapped {
		mapped[i] = byte(i % 251)
	}

	// Sync the mmap'd pages.
	require.NoError(t, unix.Msync(mapped, unix.MS_SYNC))
	unix.Munmap(mapped)

	// Read back via normal read and verify.
	buf := make([]byte, 64*1024)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)

	for i := range buf {
		expected := byte(i % 251)
		if buf[i] != expected {
			t.Fatalf("byte %d: expected 0x%02x, got 0x%02x", i, expected, buf[i])
		}
	}
}

// TestE2E_MmapCopyRange simulates fsx's COPY operation using
// read+write at different offsets, then reads back via mmap.
func TestE2E_MmapCopyRange(t *testing.T) {
	mp := stressMountGo(t, "mmap-copy")
	path := mp.mountpoint + "/mmap-copy"

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Write 256KB of patterned data.
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i % 199)
	}
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	// Copy 32KB from offset 64K to offset 128K (simulating copy_file_range).
	chunk := make([]byte, 32*1024)
	_, err = f.ReadAt(chunk, 64*1024)
	require.NoError(t, err)
	_, err = f.WriteAt(chunk, 128*1024)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	// Truncate down to 192K, then extend back to 256K.
	require.NoError(t, f.Truncate(192*1024))
	require.NoError(t, f.Truncate(256*1024))
	require.NoError(t, f.Sync())

	// mmap-read and verify:
	// - 128K-160K should have the copied data
	// - 192K-256K should be zeros (truncated then extended)
	mapped, err := unix.Mmap(int(f.Fd()), 0, 256*1024, unix.PROT_READ, unix.MAP_SHARED)
	require.NoError(t, err)
	defer unix.Munmap(mapped)

	// Verify the copy region.
	for i := 0; i < 32*1024; i++ {
		expected := byte((i + 64*1024) % 199)
		got := mapped[128*1024+i]
		if got != expected {
			t.Fatalf("copy region byte %d (offset 0x%x): expected 0x%02x, got 0x%02x",
				i, 128*1024+i, expected, got)
		}
	}

	// Verify the truncated+extended region is zeros.
	for i := 192 * 1024; i < 256*1024; i++ {
		if mapped[i] != 0 {
			t.Fatalf("extended region byte %d (0x%x): expected 0x00, got 0x%02x", i, i, mapped[i])
		}
	}
}
