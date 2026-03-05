//go:build linux

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// syncFS flushes the filesystem at the given mountpoint using syncfs(2).
// Unlike the global sync command, this only syncs the target filesystem,
// avoiding hangs when a FUSE blockdev mount is also present.
func syncFS(t *testing.T, mountpoint string) {
	t.Helper()
	fd, err := unix.Open(mountpoint, unix.O_RDONLY, 0)
	require.NoError(t, err)
	defer unix.Close(fd)
	err = unix.Syncfs(fd)
	require.NoError(t, err)
}
