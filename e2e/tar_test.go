//go:build linux

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_TarExtractRemount extracts a real rootfs tar through the FUSE mount
// using the in-process backend, unmounts, remounts, and verifies every entry.
//
// Set ROOTFS_TAR to the path of the tar file. Skips if not set or not found.
func TestE2E_TarExtractRemount(t *testing.T) {
	tarPath := os.Getenv("ROOTFS_TAR")
	if tarPath == "" {
		tarPath = "/app/rootfs.tar"
	}
	if _, err := os.Stat(tarPath); err != nil {
		t.Skipf("skipping: tar not found at %s (set ROOTFS_TAR)", tarPath)
	}

	b := newBackend(t)
	ctx := t.Context()
	vol := "tartest"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: vol}))
	require.NoError(t, b.Mount(ctx, vol, mp))

	runCmd(t, "tar", "xf", tarPath, "-C", mp)

	if needsKernelExt4() {
		syncFS(t, mp)
	}

	require.NoError(t, b.Unmount(ctx, mp))
	require.NoError(t, b.Mount(ctx, vol, mp))

	walkAndVerify(t, mp)
}

// TestE2E_TarExtractRemountCLI reproduces the exact populate-vm.sh + verify-vm.sh
// flow using the loophole CLI daemon. This catches bugs that only manifest when
// going through the CLI daemon (separate process) rather than the in-process backend.
//
// Set ROOTFS_TAR to the path of the tar file. Skips if not set or not found.
func TestE2E_TarExtractRemountCLI(t *testing.T) {
	tarPath := os.Getenv("ROOTFS_TAR")
	if tarPath == "" {
		tarPath = "/app/rootfs.tar"
	}
	if _, err := os.Stat(tarPath); err != nil {
		t.Skipf("skipping: tar not found at %s (set ROOTFS_TAR)", tarPath)
	}

	loophole, err := exec.LookPath("loophole")
	if err != nil {
		t.Skip("skipping: loophole binary not in PATH")
	}

	mp := t.TempDir()

	// Start daemon (exactly like populate-vm.sh).
	runCmd(t, loophole, "start", "-d", "s3://testbucket", "--debug")
	t.Cleanup(func() {
		exec.Command(loophole, "unmount", mp).Run()
		exec.Command(loophole, "stop").Run()
	})

	// Delete old volume if it exists, then create fresh.
	exec.Command(loophole, "delete", "-y", "clitest").Run()
	runCmd(t, loophole, "create", "clitest")

	// Mount, extract tar, unmount (exactly like populate-vm.sh).
	runCmd(t, loophole, "mount", "clitest", mp)
	runCmd(t, "tar", "xf", tarPath, "-C", mp)
	runCmd(t, loophole, "unmount", mp)

	// Remount and verify (exactly like verify-vm.sh).
	runCmd(t, loophole, "mount", "clitest", mp)

	walkAndVerify(t, mp)
}

// walkAndVerify recursively walks the filesystem at root and verifies every
// entry is stat-able and every directory is readable.
func walkAndVerify(t *testing.T, root string) {
	t.Helper()
	var totalEntries int
	var walkErr error
	var walk func(dir string)
	walk = func(dir string) {
		if walkErr != nil {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			walkErr = err
			t.Errorf("readdir %s: %v", dir, err)
			return
		}
		for _, e := range entries {
			totalEntries++
			entryPath := filepath.Join(dir, e.Name())
			info, err := e.Info()
			if err != nil {
				walkErr = err
				t.Errorf("stat %s: %v", entryPath, err)
				return
			}
			if info.IsDir() {
				walk(entryPath)
			}
		}
	}
	walk(root)
	require.NoError(t, walkErr)
	t.Logf("verified %d entries", totalEntries)
	require.Greater(t, totalEntries, 1000, "expected a real rootfs with many entries")
}
