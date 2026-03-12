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

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/fsbackend"
)

const defaultVolumeSize = 1024 * 1024 * 1024 // 1 GB

// needsRoot returns true if the mode requires root privileges.
func needsRoot() bool {
	return true
}

// needsRealMount returns true if the mode does real filesystem mounts.
func needsRealMount() bool {
	return true
}

// needsKernelExt4 returns true if the mode uses kernel ext4 (needs sync, FIFREEZE, etc.).
func needsKernelExt4() bool {
	return true
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

func skipE2E(t testing.TB) {
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

// ---------- testBackend: wraps client + daemon for tests ----------

// testBackend routes control operations (Create, Mount, Clone, etc.) through
// the daemon's HTTP API via the client, while providing direct access to the
// daemon's backend for FS and VolumeManager operations.
type testBackend struct {
	*fsbackend.Backend
	c           *client.Client
	createdVols []string
	mountedMPs  []string
	deviceVols  []string
}

func (b *testBackend) Create(ctx context.Context, p loophole.CreateParams) error {
	// The daemon's handleCreate interprets Size==0 as "clone from zygote".
	// Tests always want fresh volumes, so set a default size.
	if p.Size == 0 {
		p.Size = defaultVolumeSize
	}
	if err := b.c.Create(ctx, p); err != nil {
		return err
	}
	b.createdVols = append(b.createdVols, p.Volume)
	return nil
}

func (b *testBackend) Mount(ctx context.Context, volume, mountpoint string) error {
	if err := b.c.Mount(ctx, volume, mountpoint); err != nil {
		return err
	}
	b.mountedMPs = append(b.mountedMPs, mountpoint)
	return nil
}

func (b *testBackend) Unmount(ctx context.Context, mountpoint string) error {
	err := b.c.Unmount(ctx, mountpoint)
	if err == nil {
		// Remove from tracked list.
		for i, mp := range b.mountedMPs {
			if mp == mountpoint {
				b.mountedMPs = append(b.mountedMPs[:i], b.mountedMPs[i+1:]...)
				break
			}
		}
	}
	return err
}

func (b *testBackend) Clone(ctx context.Context, srcMountpoint, cloneName string) error {
	if err := b.c.Clone(ctx, client.CloneParams{
		Mountpoint: srcMountpoint,
		Clone:      cloneName,
	}); err != nil {
		return err
	}
	b.createdVols = append(b.createdVols, cloneName)
	return nil
}

func (b *testBackend) Checkpoint(ctx context.Context, mountpoint string) (string, error) {
	return b.c.Checkpoint(ctx, mountpoint)
}

func (b *testBackend) CloneFromCheckpoint(ctx context.Context, volume, checkpointID, cloneName string) error {
	if err := b.c.Clone(ctx, client.CloneParams{
		Volume:     volume,
		Checkpoint: checkpointID,
		Clone:      cloneName,
	}); err != nil {
		return err
	}
	b.createdVols = append(b.createdVols, cloneName)
	return nil
}

func (b *testBackend) FreezeVolume(ctx context.Context, volume string, compact bool) error {
	if err := b.c.Freeze(ctx, volume); err != nil {
		return err
	}
	filtered := b.mountedMPs[:0]
	for _, mp := range b.mountedMPs {
		if b.VolumeAt(mp) == volume || !b.IsMounted(mp) {
			continue
		}
		filtered = append(filtered, mp)
	}
	b.mountedMPs = filtered
	return nil
}

func (b *testBackend) DeviceAttach(ctx context.Context, volume string) (string, error) {
	path, err := b.c.DeviceAttach(ctx, volume)
	if err != nil {
		return "", err
	}
	b.deviceVols = append(b.deviceVols, volume)
	return path, nil
}

func (b *testBackend) DeviceDetach(ctx context.Context, volume string) error {
	err := b.c.DeviceDetach(ctx, volume)
	if err == nil {
		for i, vol := range b.deviceVols {
			if vol == volume {
				b.deviceVols = append(b.deviceVols[:i], b.deviceVols[i+1:]...)
				break
			}
		}
	}
	return err
}

func (b *testBackend) ListCheckpoints(ctx context.Context, volume string) ([]loophole.CheckpointInfo, error) {
	return b.c.ListCheckpoints(ctx, volume)
}

func (b *testBackend) DeviceCheckpoint(ctx context.Context, volume string) (string, error) {
	return b.c.DeviceCheckpoint(ctx, volume)
}

func (b *testBackend) DeviceClone(ctx context.Context, volume, clone string) error {
	if err := b.c.DeviceClone(ctx, client.DeviceCloneParams{Volume: volume, Clone: clone}); err != nil {
		return err
	}
	b.createdVols = append(b.createdVols, clone)
	return nil
}

// Close is a no-op — the shared daemon owns the backend.
func (b *testBackend) Close(ctx context.Context) error {
	return nil
}

// cleanup unmounts and deletes all volumes created by this test.
func (b *testBackend) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, mp := range b.mountedMPs {
		_ = b.c.Unmount(ctx, mp)
		// Remove the mount directory (only relevant for real mounts).
		if needsRealMount() {
			_ = os.Remove(mp)
		}
	}
	for _, vol := range b.deviceVols {
		_ = b.c.DeviceDetach(ctx, vol)
	}
	for _, vol := range b.createdVols {
		_ = b.c.Delete(ctx, vol)
	}
}

// ---------- Backend creation ----------

// newBackend returns a testBackend that routes operations through the daemon's HTTP API.
func newBackend(t testing.TB) *testBackend {
	t.Helper()
	skipE2E(t)
	if tt, ok := t.(*testing.T); ok {
		trackMetrics(tt)
	}

	b := &testBackend{
		Backend: testDaemon.Backend(),
		c:       testClient,
	}
	t.Cleanup(b.cleanup)
	return b
}

// ---------- testFS: unified file I/O ----------

// testFS wraps a rooted filesystem for test convenience methods.
type testFS struct {
	fs         *fsbackend.RootFS
	mountpoint string
}

func newTestFS(t testing.TB, b *testBackend, mountpoint string) testFS {
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

func uniqueInstance(t testing.TB) loophole.Instance {
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

// mountVolume is a convenience: creates a volume, mounts it, returns testFS + backend.
func mountVolume(t testing.TB, name string) (testFS, *testBackend) {
	t.Helper()
	b := newBackend(t)
	ctx := t.Context()

	err := b.Create(ctx, client.CreateParams{Volume: name, Size: 512 * 1024 * 1024}) // 512MB keeps the FUSE e2e path reasonably fast
	require.NoError(t, err)

	mp := mountpoint(t, name)
	err = b.Mount(ctx, name, mp)
	require.NoError(t, err)
	return newTestFS(t, b, mp), b
}

// mountpoint returns a mountpoint path for a volume name.
// For modes with real mounts, returns a temp directory.
// Returns the backing mountpoint path used by kernel ext4.
func mountpoint(t testing.TB, volume string) string {
	if needsRealMount() {
		// Don't use t.TempDir() — its cleanup fires in LIFO order and would
		// try to remove the directory before the FUSE unmount happens.
		// Instead, create a temp dir manually. The testBackend.cleanup()
		// unmounts first, then removes the mount dirs.
		dir, err := os.MkdirTemp("", t.Name())
		require.NoError(t, err)
		return dir
	}
	return "/" + volume
}
