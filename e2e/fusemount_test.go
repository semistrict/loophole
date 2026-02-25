//go:build linux

package e2e

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
)

// TestE2E_Lwext4FUSEMountIsReal proves that lwext4fuse mode creates a real
// FUSE mount visible in /proc/mounts, and that OS-level file I/O goes through it.
func TestE2E_Lwext4FUSEMountIsReal(t *testing.T) {
	if mode() != loophole.ModeLwext4FUSE {
		t.Skip("lwext4fuse-only test")
	}

	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "fuse-proof")

	require.NoError(t, b.Create(ctx, "fuse-proof"))
	require.NoError(t, b.Mount(ctx, "fuse-proof", mp))

	// 1. The mountpoint must appear in /proc/mounts as a fuse mount.
	mounts, err := os.ReadFile("/proc/mounts")
	require.NoError(t, err)

	var fuseLine string
	for _, line := range strings.Split(string(mounts), "\n") {
		if strings.Contains(line, mp) {
			fuseLine = line
			break
		}
	}
	require.NotEmpty(t, fuseLine, "mountpoint %s not found in /proc/mounts", mp)
	require.Contains(t, fuseLine, "fuse", "mount at %s is not FUSE: %s", mp, fuseLine)
	t.Logf("/proc/mounts: %s", fuseLine)

	// 2. Write a file through the OS path (kernel → FUSE → go-fuse → lwext4).
	testPath := mp + "/proof.txt"
	require.NoError(t, os.WriteFile(testPath, []byte("hello from FUSE\n"), 0o644))

	// 3. Read it back through the OS path.
	data, err := os.ReadFile(testPath)
	require.NoError(t, err)
	require.Equal(t, "hello from FUSE\n", string(data))

	// 4. Verify directory listing through the OS path.
	entries, err := os.ReadDir(mp)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Contains(t, names, "proof.txt")
	t.Logf("ls %s: %v", mp, names)

	// 5. Stat the file through the OS path.
	info, err := os.Stat(testPath)
	require.NoError(t, err)
	require.Equal(t, int64(16), info.Size())
}
