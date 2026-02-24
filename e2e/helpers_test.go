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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

// e2eMode controls how tests interact with the filesystem.
//
//	"kernel"  — FUSE + loop device + kernel ext4 (Linux-only, root required)
//	"lwext4"  — in-process lwext4 via fsbackend (cross-platform, no root)
//
// Set via E2E_MODE env var. Default: "lwext4" on macOS, "kernel" on Linux.
func e2eMode() string {
	if m := os.Getenv("E2E_MODE"); m != "" {
		return m
	}
	if runtime.GOOS == "linux" {
		return "kernel"
	}
	return "lwext4"
}

func isKernelMode() bool { return e2eMode() == "kernel" }
func isNBDMode() bool    { return os.Getenv("LOOPHOLE_MODE") == "nbd" }

func defaultS3Options() *loophole.S3Options {
	return &loophole.S3Options{
		AccessKey: envOrDefault("AWS_ACCESS_KEY_ID", "rustfsadmin"),
		SecretKey: envOrDefault("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
		Region:    envOrDefault("AWS_REGION", "us-east-1"),
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
	if isKernelMode() && os.Getuid() != 0 {
		t.Skip("kernel mode requires root; skipping E2E test")
	}
	if isKernelMode() && runtime.GOOS != "linux" {
		t.Skip("kernel mode requires Linux; skipping E2E test")
	}
}

func skipKernelOnly(t *testing.T) {
	t.Helper()
	if !isKernelMode() {
		t.Skip("test requires kernel mode (FUSE + loop + ext4)")
	}
}

const defaultVolumeSize = 1024 * 1024 * 1024 // 1 GB

// ---------- Backend creation ----------

// newBackend creates a *fsbackend.Backend for the current e2e mode.
// On kernel mode it uses NewFUSE; on lwext4 mode it uses NewLwext4.
func newBackend(t *testing.T) *fsbackend.Backend {
	t.Helper()
	skipE2E(t)

	inst := uniqueInstance(t)
	ctx := t.Context()

	store, err := loophole.NewS3Store(ctx, inst, defaultS3Options())
	require.NoError(t, err)

	_ = loophole.FormatSystem(ctx, store, 4*1024*1024)

	vm, err := loophole.NewVolumeManager(ctx, store, t.TempDir(), 20, 200)
	require.NoError(t, err)

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
	fs fsbackend.FS
}

func newTestFS(t *testing.T, b *fsbackend.Backend, mountpoint string) testFS {
	t.Helper()
	f, err := b.FS(mountpoint)
	require.NoError(t, err)
	return testFS{fs: f}
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
	raw := fmt.Sprintf("s3://%s/%s", bucket, prefix)
	inst, err := loophole.ParseInstance(raw)
	require.NoError(t, err)
	inst.Endpoint = defaultEndpoint()
	return inst
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

	if isKernelMode() {
		syncFS(t)
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

// syncFS calls sync to flush filesystem buffers (kernel mode only).
func syncFS(t *testing.T) {
	t.Helper()
	err := exec.Command("sync").Run()
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
func mountVolume(t *testing.T, name string) (testFS, *fsbackend.Backend) {
	t.Helper()
	b := newBackend(t)
	ctx := t.Context()

	err := b.Create(ctx, name)
	require.NoError(t, err)

	mp := mountpoint(name)
	if isKernelMode() {
		os.MkdirAll(mp, 0o755)
	}
	err = b.Mount(ctx, name, mp)
	require.NoError(t, err)
	return newTestFS(t, b, mp), b
}

// mountpoint returns a mountpoint path for a volume name.
// For kernel mode, returns a real temp directory.
// For lwext4 mode, returns a logical key.
func mountpoint(volume string) string {
	if isKernelMode() {
		return fmt.Sprintf("/tmp/loophole-e2e-%s", volume)
	}
	return "/" + volume
}
