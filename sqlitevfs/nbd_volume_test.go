package sqlitevfs

import (
	"net"
	"testing"

	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/nbdserve"
	"github.com/stretchr/testify/require"
)

// startTestNBDServer creates an in-process NBD server on a Unix socket
// backed by the test VolumeManager. Returns the socket path.
func startTestNBDServer(t *testing.T) string {
	t.Helper()
	mgr := testManager(t)

	// Create and format a volume so the superblock exists.
	vol, err := mgr.NewVolume(loophole.CreateParams{Volume: "testdb", Size: 256 * 1024 * 1024})
	require.NoError(t, err)
	err = FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	// Write some known data after the superblock so we can verify reads.
	require.NoError(t, vol.Write([]byte("HELLO NBD TEST"), 65536))
	require.NoError(t, vol.Flush())
	require.NoError(t, vol.ReleaseRef())

	// Start NBD server.
	srv := nbdserve.NewServer(mgr)
	sock := t.TempDir() + "/nbd.sock"
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	go srv.ServeListener(ln)
	return sock
}

func TestNBDVolumeReadWrite(t *testing.T) {
	sock := startTestNBDServer(t)

	vol, err := DialNBD(t.Context(), sock, "testdb")
	require.NoError(t, err)
	defer vol.ReleaseRef()

	require.Equal(t, "testdb", vol.Name())
	require.Equal(t, uint64(256*1024*1024), vol.Size())

	// Read the known data we wrote at offset 65536.
	buf := make([]byte, 14)
	n, err := vol.Read(t.Context(), buf, 65536)
	require.NoError(t, err)
	require.Equal(t, 14, n)
	require.Equal(t, "HELLO NBD TEST", string(buf))

	// Write new data and read it back.
	require.NoError(t, vol.Write([]byte("WRITTEN VIA NBD"), 65536+4096))
	buf2 := make([]byte, 15)
	_, err = vol.Read(t.Context(), buf2, 65536+4096)
	require.NoError(t, err)
	require.Equal(t, "WRITTEN VIA NBD", string(buf2))

	// Flush should succeed.
	require.NoError(t, vol.Flush())
}

func TestNBDVolumeHeader(t *testing.T) {
	sock := startTestNBDServer(t)

	vol, err := DialNBD(t.Context(), sock, "testdb")
	require.NoError(t, err)
	defer vol.ReleaseRef()

	// Should be able to read and parse the header.
	h, err := ReadHeader(t.Context(), vol)
	require.NoError(t, err)
	require.Equal(t, uint64(0), h.MainDBSize)
}

func TestNBDVolumeVFS(t *testing.T) {
	sock := startTestNBDServer(t)

	vol, err := DialNBD(t.Context(), sock, "testdb")
	require.NoError(t, err)
	defer vol.ReleaseRef()

	// Open a VolumeVFS on top of the NBD volume.
	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	// Open the main database file.
	f, flags, err := vfs.Open("main.db", OpenCreate|OpenReadWrite|OpenMainDB)
	require.NoError(t, err)
	require.NotZero(t, flags)

	// Write and read back.
	data := []byte("SQLite via NBD VFS")
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	readBuf := make([]byte, len(data))
	_, err = f.ReadAt(readBuf, 0)
	require.NoError(t, err)
	require.Equal(t, "SQLite via NBD VFS", string(readBuf))

	require.NoError(t, f.Close())
}

func TestNBDVolumeExportNotFound(t *testing.T) {
	sock := startTestNBDServer(t)

	_, err := DialNBD(t.Context(), sock, "nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonexistent")
}

func TestNBDVolumeReadZeros(t *testing.T) {
	sock := startTestNBDServer(t)

	vol, err := DialNBD(t.Context(), sock, "testdb")
	require.NoError(t, err)
	defer vol.ReleaseRef()

	// Read from an area that was never written — should be zeros.
	buf := make([]byte, 4096)
	_, err = vol.Read(t.Context(), buf, 1024*1024)
	require.NoError(t, err)
	for i, b := range buf {
		require.Equal(t, byte(0), b, "expected zero at offset %d", i)
	}
}

// Verify NBDVolume satisfies the nbd Export Flags function.
func TestNBDVolumeFlags(t *testing.T) {
	sock := startTestNBDServer(t)

	// Connect and verify export flags are reasonable.
	conn, err := net.Dial("unix", sock)
	require.NoError(t, err)
	defer conn.Close()

	cl, err := nbd.ClientHandshake(t.Context(), conn)
	require.NoError(t, err)

	exports, err := cl.List()
	require.NoError(t, err)
	require.Contains(t, exports, "testdb")
}
