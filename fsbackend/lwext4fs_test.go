package fsbackend

import (
	"context"
	"io"
	"testing"

	"github.com/semistrict/loophole/lwext4"
	"github.com/stretchr/testify/require"
)

type memDev struct {
	data []byte
}

func (m *memDev) Read(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= uint64(len(m.data)) {
		return 0, io.EOF
	}
	return copy(buf, m.data[offset:]), nil
}

func (m *memDev) Write(_ context.Context, data []byte, offset uint64) error {
	copy(m.data[offset:], data)
	return nil
}

func (m *memDev) ZeroRange(_ context.Context, offset, length uint64) error {
	clear(m.data[offset : offset+length])
	return nil
}

const testDevSize = 128 * 1024 * 1024

func newTestLwext4FS(t *testing.T) *lwext4FSImpl {
	t.Helper()
	dev := &memDev{data: make([]byte, testDevSize)}
	ext4fs, err := lwext4.Format(dev, testDevSize, nil)
	require.NoError(t, err)
	t.Cleanup(func() { ext4fs.Close() })
	return newLwext4FS(ext4fs)
}

// TestResolveFollowsSymlinks verifies that resolve (used by Open, Stat,
// ReadDir, etc.) follows symlinks at every path component.
// This reproduces the bug where "cat /etc/os-release" on a Debian rootfs
// hung forever because /etc/os-release is a symlink to ../usr/lib/os-release
// and resolve returned the symlink inode instead of following it.
func TestResolveFollowsSymlinks(t *testing.T) {
	fs := newTestLwext4FS(t)

	// Create /usr/lib/os-release with known content.
	require.NoError(t, fs.MkdirAll("/usr/lib", 0o755))
	require.NoError(t, fs.WriteFile("/usr/lib/os-release", []byte("PRETTY_NAME=\"Debian\"\n"), 0o644))

	// Create /etc as a directory.
	require.NoError(t, fs.MkdirAll("/etc", 0o755))

	// Create /etc/os-release as a symlink to ../usr/lib/os-release (relative).
	require.NoError(t, fs.Symlink("../usr/lib/os-release", "/etc/os-release"))

	// Open through the symlink — this is what "file cat" does.
	f, err := fs.Open("/etc/os-release")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "PRETTY_NAME=\"Debian\"\n", string(data))

	// Stat should return the target file's attributes (regular file, not symlink).
	info, err := fs.Stat("/etc/os-release")
	require.NoError(t, err)
	require.False(t, info.Mode().IsDir())
	require.True(t, info.Mode().IsRegular())

	// Lstat should return the symlink itself.
	linfo, err := fs.Lstat("/etc/os-release")
	require.NoError(t, err)
	require.NotZero(t, linfo.Mode()&io.SeekEnd) // just check it's different
	// More precisely: Lstat returns symlink type.
	require.NotEqual(t, linfo.Mode().Type(), info.Mode().Type())
}

// TestResolveFollowsDirectorySymlinks verifies that symlinks used as
// intermediate path components (e.g. /bin -> usr/bin) are followed.
func TestResolveFollowsDirectorySymlinks(t *testing.T) {
	fs := newTestLwext4FS(t)

	// Debian-style merged /usr layout.
	require.NoError(t, fs.MkdirAll("/usr/bin", 0o755))
	require.NoError(t, fs.WriteFile("/usr/bin/sh", []byte("#!/bin/sh\n"), 0o755))

	// /bin -> usr/bin (relative symlink)
	require.NoError(t, fs.Symlink("usr/bin", "/bin"))

	// Open /bin/sh — must resolve /bin symlink then find sh.
	f, err := fs.Open("/bin/sh")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "#!/bin/sh\n", string(data))

	// ReadDir through the symlink.
	names, err := fs.ReadDir("/bin")
	require.NoError(t, err)
	require.Contains(t, names, "sh")
}

// TestResolveAbsoluteSymlink verifies that absolute symlinks are resolved
// from the filesystem root.
func TestResolveAbsoluteSymlink(t *testing.T) {
	fs := newTestLwext4FS(t)

	require.NoError(t, fs.MkdirAll("/usr/lib", 0o755))
	require.NoError(t, fs.WriteFile("/usr/lib/real.txt", []byte("real content"), 0o644))

	// Absolute symlink: /link -> /usr/lib/real.txt
	require.NoError(t, fs.Symlink("/usr/lib/real.txt", "/link"))

	f, err := fs.Open("/link")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "real content", string(data))
}

// TestResolveChainedSymlinks verifies that chains of symlinks are followed.
func TestResolveChainedSymlinks(t *testing.T) {
	fs := newTestLwext4FS(t)

	require.NoError(t, fs.WriteFile("/target.txt", []byte("found it"), 0o644))
	require.NoError(t, fs.Symlink("target.txt", "/link1"))
	require.NoError(t, fs.Symlink("link1", "/link2"))
	require.NoError(t, fs.Symlink("link2", "/link3"))

	f, err := fs.Open("/link3")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "found it", string(data))
}

// TestResolveTooManySymlinks verifies the loop detection limit.
func TestResolveTooManySymlinks(t *testing.T) {
	fs := newTestLwext4FS(t)

	// Create a symlink loop: /a -> /b, /b -> /a
	require.NoError(t, fs.Symlink("b", "/a"))
	require.NoError(t, fs.Symlink("a", "/b"))

	_, err := fs.Open("/a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many symlinks")
}
