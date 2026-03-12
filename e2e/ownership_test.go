//go:build linux

package e2e

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestE2E_CreateFileOwnership(t *testing.T) {
	mp := stressMountGo(t, "own-file").mountpoint

	path := mp + "/hello.txt"
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	var st syscall.Stat_t
	require.NoError(t, syscall.Stat(path, &st))

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	require.Equal(t, uid, st.Uid, "file uid should match caller")
	require.Equal(t, gid, st.Gid, "file gid should match caller")
}

func TestE2E_MkdirOwnership(t *testing.T) {
	mp := stressMountGo(t, "own-dir").mountpoint

	path := mp + "/subdir"
	require.NoError(t, os.Mkdir(path, 0o755))

	var st syscall.Stat_t
	require.NoError(t, syscall.Stat(path, &st))

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	require.Equal(t, uid, st.Uid, "dir uid should match caller")
	require.Equal(t, gid, st.Gid, "dir gid should match caller")
}

func TestE2E_RootAndLostFoundOwnership(t *testing.T) {
	mp := stressMountGo(t, "own-root").mountpoint

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())

	// Root directory should be owned by the formatting process.
	var rootSt syscall.Stat_t
	require.NoError(t, syscall.Stat(mp, &rootSt))
	require.Equal(t, uid, rootSt.Uid, "root dir uid should match caller")
	require.Equal(t, gid, rootSt.Gid, "root dir gid should match caller")

	// lost+found should also be owned by the formatting process.
	var lfSt syscall.Stat_t
	require.NoError(t, syscall.Stat(mp+"/lost+found", &lfSt))
	require.Equal(t, uid, lfSt.Uid, "lost+found uid should match caller")
	require.Equal(t, gid, lfSt.Gid, "lost+found gid should match caller")
}

func TestE2E_SymlinkOwnership(t *testing.T) {
	mp := stressMountGo(t, "own-sym").mountpoint

	target := mp + "/target.txt"
	f, err := os.Create(target)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	link := mp + "/link.txt"
	require.NoError(t, os.Symlink("target.txt", link))

	var st syscall.Stat_t
	require.NoError(t, syscall.Lstat(link, &st))

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	require.Equal(t, uid, st.Uid, "symlink uid should match caller")
	require.Equal(t, gid, st.Gid, "symlink gid should match caller")
}
