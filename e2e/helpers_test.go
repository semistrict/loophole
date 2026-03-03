//go:build linux

package e2e

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/lsm"
)

// mode returns the LOOPHOLE_MODE for tests, using DefaultMode() as fallback.
func mode() loophole.Mode {
	return loophole.DefaultMode()
}

const defaultVolumeSize = 1024 * 1024 * 1024 // 1 GB

// needsRoot returns true if the mode requires root privileges.
func needsRoot() bool {
	switch mode() {
	case loophole.ModeFUSE, loophole.ModeNBD, loophole.ModeTestNBDTCP:
		return true
	default:
		return false
	}
}

// needsRealMount returns true if the mode does real filesystem mounts.
// All current modes (fuse, nbd, testnbdtcp, lwext4fuse) use real mounts.
func needsRealMount() bool {
	return true
}

// needsKernelExt4 returns true if the mode uses kernel ext4 (needs sync, FIFREEZE, etc.).
func needsKernelExt4() bool {
	switch mode() {
	case loophole.ModeFUSE, loophole.ModeNBD, loophole.ModeTestNBDTCP:
		return true
	default:
		return false
	}
}

func defaultEndpoint() string {
	return envOrDefault("S3_ENDPOINT", "http://localhost:9000")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func skipE2E(t *testing.T) {
	t.Helper()
	if needsRoot() && os.Getuid() != 0 {
		t.Skip("mode requires root; skipping")
	}
}

func skipKernelOnly(t *testing.T) {
	t.Helper()
	if !needsKernelExt4() {
		t.Skip("test requires kernel ext4 mode")
	}
}

// ---------- Backend creation ----------

// newBackend creates a fsbackend.Service for the current mode.
func newBackend(t *testing.T) fsbackend.Service {
	t.Helper()
	skipE2E(t)

	inst := uniqueInstance(t)
	ctx := t.Context()

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm := lsm.NewManager(store, t.TempDir(), lsm.Config{}, nil, nil, nil)

	backend := newBackendForMode(t, vm, inst)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = backend.Close(cleanupCtx)
	})
	return backend
}

// ---------- testFS: unified file I/O ----------

// testFS wraps fsbackend.FS for test convenience methods.
type testFS struct {
	fs         fsbackend.FS
	mountpoint string
}

func newTestFS(t *testing.T, b fsbackend.Service, mountpoint string) testFS {
	t.Helper()
	f, err := b.FS(mountpoint)
	require.NoError(t, err)
	return testFS{fs: f, mountpoint: mountpoint}
}

func (f testFS) WriteFile(t *testing.T, name string, data []byte) {
	t.Helper()
	err := f.fs.WriteFile(name, data, 0o644)
	require.NoError(t, err)
}

func (f testFS) ReadFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := f.fs.ReadFile(name)
	require.NoError(t, err)
	return data
}

func (f testFS) MkdirAll(t *testing.T, name string) {
	t.Helper()
	err := f.fs.MkdirAll(name, 0o755)
	require.NoError(t, err)
}

func (f testFS) Remove(t *testing.T, name string) {
	t.Helper()
	err := f.fs.Remove(name)
	require.NoError(t, err)
}

func (f testFS) Exists(t *testing.T, name string) bool {
	t.Helper()
	_, err := f.fs.Stat(name)
	return err == nil
}

func (f testFS) Stat(t *testing.T, name string) os.FileInfo {
	t.Helper()
	info, err := f.fs.Stat(name)
	require.NoError(t, err)
	return info
}

func (f testFS) MD5(t *testing.T, name string) string {
	t.Helper()
	h := md5.New()
	file, err := f.fs.Open(name)
	require.NoError(t, err)
	defer file.Close()
	_, err = io.Copy(h, file)
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

func (f testFS) ReadDir(t *testing.T, name string) []string {
	t.Helper()
	names, err := f.fs.ReadDir(name)
	require.NoError(t, err)
	return names
}

// ---------- Shared helpers ----------

func uniqueInstance(t *testing.T) loophole.Instance {
	t.Helper()
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}
	prefix := fmt.Sprintf("test-%s", uuid.NewString()[:8])
	return loophole.Instance{
		ProfileName: "test",
		Bucket:      bucket,
		Prefix:      prefix,
		Endpoint:    defaultEndpoint(),
		AccessKey:   envOrDefault("AWS_ACCESS_KEY_ID", "rustfsadmin"),
		SecretKey:   envOrDefault("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
		Region:      envOrDefault("AWS_REGION", ""),
	}
}

// writeTestFiles writes a standard set of test files.
func writeTestFiles(t *testing.T, tfs testFS) string {
	t.Helper()

	tfs.WriteFile(t, "greeting.txt", []byte("hello from loophole\n"))

	randomData := make([]byte, 10*1024*1024)
	_, err := rand.Read(randomData)
	require.NoError(t, err)
	tfs.WriteFile(t, "random.bin", randomData)

	tfs.MkdirAll(t, "subdir/nested")
	tfs.WriteFile(t, "subdir/nested/deep.txt", []byte("nested file\n"))

	var buf strings.Builder
	for i := 1; i <= 1000; i++ {
		fmt.Fprintf(&buf, "%d\n", i)
	}
	tfs.WriteFile(t, "numbers.txt", []byte(buf.String()))

	if needsKernelExt4() {
		syncFS(t, tfs.mountpoint)
	}

	return tfs.MD5(t, "random.bin")
}

// verifyTestFiles asserts the standard test files are intact.
func verifyTestFiles(t *testing.T, tfs testFS, randomMD5 string) {
	t.Helper()

	require.Equal(t, "hello from loophole\n", string(tfs.ReadFile(t, "greeting.txt")))
	require.Equal(t, randomMD5, tfs.MD5(t, "random.bin"))
	require.Equal(t, "nested file\n", string(tfs.ReadFile(t, "subdir/nested/deep.txt")))

	data := tfs.ReadFile(t, "numbers.txt")
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Equal(t, 1000, len(lines))
}

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

// hasTool returns true if the given tool is in PATH.
func hasTool(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// runCmd runs a command and fails the test on error.
func runCmd(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	require.NoError(t, err, "command failed: %s %v", name, args)
}

// mountVolume is a convenience: creates a backend, creates+formats a volume, mounts it, returns testFS + backend.
func mountVolume(t *testing.T, name string) (testFS, fsbackend.Service) {
	t.Helper()
	b := newBackend(t)
	ctx := t.Context()

	err := b.Create(ctx, client.CreateParams{Volume: name, Size: 256 * 1024 * 1024}) // 256MB — 8GB is too slow through FUSE
	require.NoError(t, err)

	mp := mountpoint(t, name)
	err = b.Mount(ctx, name, mp)
	require.NoError(t, err)
	return newTestFS(t, b, mp), b
}

// mountpoint returns a mountpoint path for a volume name.
// For modes with real mounts, returns a temp directory.
// For in-process lwext4 mode, returns a logical key.
func mountpoint(t *testing.T, volume string) string {
	if needsRealMount() {
		dir := t.TempDir()
		return dir
	}
	return "/" + volume
}
