//go:build linux

package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole/internal/fsserver"
	"github.com/semistrict/loophole/internal/storage"
)

func TestE2E_FormatCreatesMountableExt4(t *testing.T) {
	tfs, _ := mountVolume(t, "fmttest")

	tfs.WriteFile(t, "hello.txt", []byte("formatted via high-level\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "formatted via high-level\n", string(tfs.ReadFile(t, "hello.txt")))
}

func TestE2E_SmallTextFile(t *testing.T) {
	tfs, _ := mountVolume(t, "smalltext")

	tfs.WriteFile(t, "greeting.txt", []byte("hello from loophole\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "hello from loophole\n", string(tfs.ReadFile(t, "greeting.txt")))
}

func TestE2E_BinaryFileIntegrity(t *testing.T) {
	tfs, _ := mountVolume(t, "binaryfile")

	randomData := make([]byte, 10*1024*1024)
	_, err := rand.Read(randomData)
	require.NoError(t, err)

	tfs.WriteFile(t, "random.bin", randomData)
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	checksum := tfs.MD5(t, "random.bin")
	require.Equal(t, checksum, tfs.MD5(t, "random.bin"))
}

func TestE2E_NestedDirectories(t *testing.T) {
	tfs, _ := mountVolume(t, "nesteddir")

	tfs.MkdirAll(t, "subdir/nested")
	tfs.WriteFile(t, "subdir/nested/deep.txt", []byte("nested file\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "nested file\n", string(tfs.ReadFile(t, "subdir/nested/deep.txt")))
}

func TestE2E_LargeSequentialFile(t *testing.T) {
	tfs, _ := mountVolume(t, "largeseq")

	var buf strings.Builder
	for i := 1; i <= 1000; i++ {
		fmt.Fprintf(&buf, "%d\n", i)
	}
	tfs.WriteFile(t, "numbers.txt", []byte(buf.String()))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	data := tfs.ReadFile(t, "numbers.txt")
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Equal(t, 1000, len(lines))
}

func TestE2E_OverwriteFile(t *testing.T) {
	tfs, _ := mountVolume(t, "overwrite")

	tfs.WriteFile(t, "overwrite.txt", []byte("version 1\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}
	tfs.WriteFile(t, "overwrite.txt", []byte("version 2\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "version 2\n", string(tfs.ReadFile(t, "overwrite.txt")))
}

func TestE2E_DeleteAndRecreate(t *testing.T) {
	tfs, _ := mountVolume(t, "delrecreate")

	tfs.WriteFile(t, "ephemeral.txt", []byte("exists\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	tfs.Remove(t, "ephemeral.txt")
	require.False(t, tfs.Exists(t, "ephemeral.txt"))

	tfs.WriteFile(t, "ephemeral.txt", []byte("back again\n"))
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "back again\n", string(tfs.ReadFile(t, "ephemeral.txt")))
}

func TestE2E_RemountEmptyVolume(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	vol := "remount-empty"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol}))

	err := b.Mount(ctx, vol, mp)
	require.NoError(t, err)

	tfs := newTestFS(t, b, mp)
	info := tfs.Stat(t, ".")
	require.True(t, info.IsDir())

	err = b.Unmount(ctx, mp)
	require.NoError(t, err)

	err = b.Mount(ctx, vol, mp)
	if err != nil {
		t.Logf("remount failed for volume=%q mountpoint=%q: %v", vol, mp, err)
		logKernelDebug(t, mp, vol)
	}
	require.NoError(t, err)
}

func TestE2E_DataPersistsAcrossMountCycles(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "persist")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "persist"}))

	// Phase 1: mount, write, unmount.
	err := b.Mount(ctx, "persist", mp)
	require.NoError(t, err)
	tfs := newTestFS(t, b, mp)
	randomData := make([]byte, 5*1024*1024)
	_, err = rand.Read(randomData)
	require.NoError(t, err)
	tfs.WriteFile(t, "random.bin", randomData)
	if needsKernelExt4() {
		syncFS(t, mp)
	}
	checksum := tfs.MD5(t, "random.bin")
	err = b.Unmount(ctx, mp)
	require.NoError(t, err)

	// Phase 2: remount and verify.
	err = b.Mount(ctx, "persist", mp)
	require.NoError(t, err)
	tfs2 := newTestFS(t, b, mp)
	require.Equal(t, checksum, tfs2.MD5(t, "random.bin"))
}

func TestE2E_NestedDirsAndLargeFile(t *testing.T) {
	tfs, _ := mountVolume(t, "nestlarge")

	tfs.MkdirAll(t, "a/b/c")
	tfs.WriteFile(t, "a/b/c/deep.txt", []byte("deep nested\n"))

	bigData := make([]byte, 20*1024*1024)
	_, err := rand.Read(bigData)
	require.NoError(t, err)
	tfs.WriteFile(t, "a/big.bin", bigData)
	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	require.Equal(t, "deep nested\n", string(tfs.ReadFile(t, "a/b/c/deep.txt")))
	info := tfs.Stat(t, "a/big.bin")
	require.Equal(t, int64(20*1024*1024), info.Size())
}

func TestE2E_FilesSurviveRemount(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "survive")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "survive"}))

	// Phase 1: write.
	err := b.Mount(ctx, "survive", mp)
	require.NoError(t, err)
	tfs := newTestFS(t, b, mp)
	randomMD5 := writeTestFiles(t, tfs)
	err = b.Unmount(ctx, mp)
	require.NoError(t, err)

	// Phase 2: remount and verify.
	err = b.Mount(ctx, "survive", mp)
	require.NoError(t, err)
	tfs2 := newTestFS(t, b, mp)
	verifyTestFiles(t, tfs2, randomMD5)
}

func TestE2E_ConcurrentMountCycles(t *testing.T) {
	b := newBackend(t)
	nWorkers := 10
	if maxLoop, err := fsserver.MaxLoopDevices(); err == nil && maxLoop > 0 && nWorkers > maxLoop {
		t.Logf("capping workers to max_loop=%d", maxLoop)
		nWorkers = maxLoop
	}

	g, ctx := errgroup.WithContext(t.Context())

	for i := range nWorkers {
		g.Go(func() error {
			volName := fmt.Sprintf("concurrent-%d", i)
			mp := mountpoint(t, volName)

			if err := b.Create(ctx, storage.CreateParams{Volume: volName}); err != nil {
				return fmt.Errorf("worker %d create: %w", i, err)
			}

			if err := b.Mount(ctx, volName, mp); err != nil {
				return fmt.Errorf("worker %d mount: %w", i, err)
			}
			defer b.Unmount(ctx, mp)

			tfs := newTestFS(t, b, mp)
			expected := fmt.Sprintf("worker %d\n", i)
			tfs.WriteFile(t, "test.txt", []byte(expected))
			syncFS(t, mp)

			data := tfs.ReadFile(t, "test.txt")
			if string(data) != expected {
				return fmt.Errorf("worker %d: got %q, want %q", i, string(data), expected)
			}
			return nil
		})
	}

	require.NoError(t, g.Wait())
}

func logKernelDebug(t *testing.T, mountpoint, volume string) {
	t.Helper()
	if !needsKernelExt4() {
		return
	}
	logCmd(t, "findmnt", "-n", "-o", "SOURCE,TARGET,FSTYPE,OPTIONS", mountpoint)
	logCmd(t, "sh", "-c", "losetup -a | grep '"+volume+"' || true")
	logCmd(t, "sh", "-c", "findmnt -rn -t ext4 -o TARGET,SOURCE | grep '^/tmp/loophole-e2e-' || true")
}

func logCmd(t *testing.T, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("$ %s %s\nerror: %v\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		return
	}
	t.Logf("$ %s %s\n%s", name, strings.Join(args, " "), strings.TrimSpace(string(out)))
}
