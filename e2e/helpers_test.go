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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/objstore"
	"github.com/semistrict/loophole/storage"
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

// ---------- testBackend: owner-process harness for tests ----------

type testOwner struct {
	cmd        *exec.Cmd
	client     *client.Client
	done       chan error
	logPath    string
	volume     string
	mountpoint string
	device     string
}

type testBackend struct {
	mu          sync.Mutex
	owners      map[string]*testOwner
	createdVols []string
	mountedMPs  []string
	deviceVols  []string
}

func (b *testBackend) Create(ctx context.Context, p storage.CreateParams) error {
	if p.Size == 0 {
		p.Size = defaultVolumeSize
	}
	if !p.NoFormat {
		formatMount, err := os.MkdirTemp("", "loophole-e2e-format-*")
		if err != nil {
			return err
		}
		owner, err := startCreateOwner(ctx, p, formatMount)
		if err != nil {
			_ = os.Remove(formatMount)
			return err
		}
		if err := shutdownOwner(ctx, owner); err != nil {
			_ = os.Remove(formatMount)
			return err
		}
		if err := os.Remove(formatMount); err != nil && !os.IsNotExist(err) {
			return err
		}
		b.trackCreated(p.Volume)
		return nil
	}
	vm, cleanup, err := openDirectManager(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, err := vm.NewVolume(p); err != nil {
		return err
	}
	b.trackCreated(p.Volume)
	return nil
}

func (b *testBackend) Mount(ctx context.Context, volume, mountpoint string) error {
	if owner := b.ownerByVolume(volume); owner != nil {
		if owner.mountpoint == mountpoint {
			return nil
		}
		return fmt.Errorf("volume %q is already mounted at %s", volume, owner.mountpoint)
	}
	owner, err := startMountedOwner(ctx, volume, mountpoint)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.owners[volume] = owner
	b.mountedMPs = appendTracked(b.mountedMPs, owner.mountpoint)
	b.mu.Unlock()
	return nil
}

func (b *testBackend) Unmount(ctx context.Context, mountpoint string) error {
	owner := b.ownerByMountpoint(mountpoint)
	if owner == nil {
		return fmt.Errorf("no owner for mountpoint %s", mountpoint)
	}
	if err := shutdownOwner(ctx, owner); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.owners, owner.volume)
	b.mountedMPs = removeTracked(b.mountedMPs, owner.mountpoint)
	b.mu.Unlock()
	return nil
}

func (b *testBackend) Clone(ctx context.Context, srcMountpoint, cloneName string) error {
	owner := b.ownerByMountpoint(srcMountpoint)
	if owner == nil {
		return fmt.Errorf("no owner for mountpoint %s", srcMountpoint)
	}
	if err := owner.client.Clone(ctx, client.CloneParams{
		Mountpoint: srcMountpoint,
		Clone:      cloneName,
	}); err != nil {
		return err
	}
	b.trackCreated(cloneName)
	return nil
}

func (b *testBackend) Checkpoint(ctx context.Context, mountpoint string) (string, error) {
	owner := b.ownerByMountpoint(mountpoint)
	if owner == nil {
		return "", fmt.Errorf("no owner for mountpoint %s", mountpoint)
	}
	return owner.client.Checkpoint(ctx, mountpoint)
}

func (b *testBackend) CloneFromCheckpoint(ctx context.Context, volume, checkpointID, cloneName string) error {
	owner, err := b.ensureDeviceOwner(ctx, volume)
	if err != nil {
		return err
	}
	if err := owner.client.Clone(ctx, client.CloneParams{
		Volume:     volume,
		Checkpoint: checkpointID,
		Clone:      cloneName,
	}); err != nil {
		return err
	}
	b.trackCreated(cloneName)
	return nil
}

func (b *testBackend) DeviceAttach(ctx context.Context, volume string) (string, error) {
	owner, err := startAttachedOwner(ctx, volume)
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	b.owners[volume] = owner
	b.deviceVols = appendTracked(b.deviceVols, volume)
	b.mu.Unlock()
	return owner.device, nil
}

func (b *testBackend) DeviceDetach(ctx context.Context, volume string) error {
	owner := b.ownerByVolume(volume)
	if owner == nil {
		return fmt.Errorf("no owner for volume %s", volume)
	}
	if err := shutdownOwner(ctx, owner); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.owners, volume)
	b.deviceVols = removeTracked(b.deviceVols, volume)
	b.mu.Unlock()
	return nil
}

func (b *testBackend) ListCheckpoints(ctx context.Context, volume string) ([]storage.CheckpointInfo, error) {
	vm, cleanup, err := openDirectManager(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return vm.ListCheckpoints(ctx, volume)
}

func (b *testBackend) DeviceCheckpoint(ctx context.Context, volume string) (string, error) {
	owner, err := b.ensureDeviceOwner(ctx, volume)
	if err != nil {
		return "", err
	}
	return owner.client.DeviceCheckpoint(ctx, volume)
}

func (b *testBackend) DeviceClone(ctx context.Context, volume, clone string) error {
	owner, err := b.ensureDeviceOwner(ctx, volume)
	if err != nil {
		return err
	}
	if err := owner.client.DeviceClone(ctx, client.DeviceCloneParams{Volume: volume, Clone: clone}); err != nil {
		return err
	}
	b.trackCreated(clone)
	return nil
}

func (b *testBackend) Close(ctx context.Context) error {
	b.cleanup()
	return nil
}

func (b *testBackend) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b.mu.Lock()
	owners := make([]*testOwner, 0, len(b.owners))
	for _, owner := range b.owners {
		owners = append(owners, owner)
	}
	mounted := append([]string(nil), b.mountedMPs...)
	deviceVols := append([]string(nil), b.deviceVols...)
	created := append([]string(nil), b.createdVols...)
	b.mu.Unlock()
	for _, owner := range owners {
		_ = shutdownOwner(ctx, owner)
	}
	for _, mp := range mounted {
		if needsRealMount() {
			_ = os.Remove(mp)
		}
	}
	_ = deviceVols
	vm, cleanup, err := openDirectManager(ctx)
	if err != nil {
		return
	}
	defer cleanup()
	for _, vol := range created {
		_ = vm.DeleteVolume(ctx, vol)
	}
}

// ---------- Backend creation ----------

func newBackend(t testing.TB) *testBackend {
	t.Helper()
	skipE2E(t)
	if tt, ok := t.(*testing.T); ok {
		trackMetrics(tt)
	}

	b := &testBackend{
		owners: make(map[string]*testOwner),
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

func uniqueInstance(t testing.TB) env.ResolvedProfile {
	t.Helper()
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}
	prefix := fmt.Sprintf("test-%s", uuid.NewString()[:8])
	return env.ResolvedProfile{
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

func (b *testBackend) FS(mountpoint string) (*fsbackend.RootFS, error) {
	return fsbackend.NewRootFS(mountpoint), nil
}

func (b *testBackend) VolumeAt(mountpoint string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, owner := range b.owners {
		if owner.mountpoint == mountpoint {
			return owner.volume
		}
	}
	return ""
}

func (b *testBackend) IsMounted(mountpoint string) bool {
	return b.VolumeAt(mountpoint) != ""
}

func (b *testBackend) ownerByVolume(volume string) *testOwner {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.owners[volume]
}

func (b *testBackend) ownerByMountpoint(mountpoint string) *testOwner {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, owner := range b.owners {
		if owner.mountpoint == mountpoint {
			return owner
		}
	}
	return nil
}

func (b *testBackend) trackCreated(volume string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createdVols = appendTracked(b.createdVols, volume)
}

func (b *testBackend) ensureDeviceOwner(ctx context.Context, volume string) (*testOwner, error) {
	if owner := b.ownerByVolume(volume); owner != nil {
		return owner, nil
	}
	owner, err := startAttachedOwner(ctx, volume)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.owners[volume] = owner
	b.deviceVols = appendTracked(b.deviceVols, volume)
	b.mu.Unlock()
	return owner, nil
}

func appendTracked(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func removeTracked(values []string, value string) []string {
	for i, existing := range values {
		if existing == value {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func openDirectManager(ctx context.Context) (*storage.Manager, func(), error) {
	var store objstore.ObjectStore
	var err error
	if testInst.LocalDir != "" {
		store, err = objstore.NewFileStore(testInst.LocalDir)
	} else {
		store, err = objstore.NewS3Store(ctx, testInst)
	}
	if err != nil {
		return nil, nil, err
	}
	vm := storage.NewManager(store, testDir.Cache(testInst.ProfileName), storage.Config{}, nil, nil)
	return vm, func() {
		_ = vm.Close(context.Background())
	}, nil
}

func startMountedOwner(ctx context.Context, volume, mountpoint string) (*testOwner, error) {
	owner, err := startOwnerProcess(ctx, volume, "mount", volume, mountpoint)
	if err != nil {
		return nil, err
	}
	owner.mountpoint = mountpoint
	return owner, nil
}

func startCreateOwner(ctx context.Context, p storage.CreateParams, mountpoint string) (*testOwner, error) {
	args := []string{"create", "--mount", mountpoint}
	if p.Size != 0 {
		args = append(args, "--size", fmt.Sprintf("%d", p.Size))
	}
	if p.NoFormat {
		args = append(args, "--no-format")
	}
	args = append(args, p.Volume)
	owner, err := startOwnerProcess(ctx, p.Volume, args...)
	if err != nil {
		return nil, err
	}
	owner.mountpoint = mountpoint
	return owner, nil
}

func startAttachedOwner(ctx context.Context, volume string) (*testOwner, error) {
	owner, err := startOwnerProcess(ctx, volume, "device", "attach", volume)
	if err != nil {
		return nil, err
	}
	status, err := owner.client.Status(ctx)
	if err != nil {
		_ = shutdownOwner(context.Background(), owner)
		return nil, err
	}
	owner.device = status.Device
	return owner, nil
}

func startOwnerProcess(ctx context.Context, volume string, args ...string) (*testOwner, error) {
	socketPath := testDir.VolumeSocket(volume)
	c := client.NewFromSocket(socketPath)
	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := c.Status(timeoutCtx); err == nil {
		return nil, fmt.Errorf("volume %q is already managed at %s", volume, socketPath)
	}

	logPath := filepath.Join(string(testDir), fmt.Sprintf("owner-%s.log", volume))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(testBin, append([]string{"-p", testInst.ProfileName}, args...)...)
	cmd.Env = append(os.Environ(), "LOOPHOLE_HOME="+string(testDir))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		util.SafeClose(logFile, "close owner log file on start failure")
		return nil, err
	}
	go func() {
		done <- cmd.Wait()
		util.SafeClose(logFile, "close owner log file")
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	for {
		status, err := c.Status(waitCtx)
		if err == nil {
			owner := &testOwner{
				cmd:     cmd,
				client:  c,
				done:    done,
				logPath: logPath,
				volume:  volume,
			}
			if status.Mountpoint != "" {
				owner.mountpoint = status.Mountpoint
			}
			if status.Device != "" {
				owner.device = status.Device
			}
			return owner, nil
		}
		select {
		case err := <-done:
			return nil, fmt.Errorf("owner for %s exited before ready: %w\n%s", volume, err, tailFile(logPath, 80))
		case <-time.After(25 * time.Millisecond):
		case <-waitCtx.Done():
			_ = cmd.Process.Signal(syscall.SIGTERM)
			<-done
			return nil, fmt.Errorf("owner for %s did not become ready: %w\n%s", volume, waitCtx.Err(), tailFile(logPath, 80))
		}
	}
}

func shutdownOwner(ctx context.Context, owner *testOwner) error {
	if owner == nil || owner.cmd == nil || owner.cmd.Process == nil {
		return nil
	}
	if err := owner.cmd.Process.Signal(syscall.SIGINT); err != nil && !strings.Contains(err.Error(), "process already finished") {
		return err
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- <-owner.done
	}()
	select {
	case err := <-waitDone:
		if err == nil || strings.Contains(err.Error(), "signal: interrupt") {
			return nil
		}
		return err
	case <-ctx.Done():
		_ = owner.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-waitDone:
			if err != nil && !strings.Contains(err.Error(), "signal: terminated") && !strings.Contains(err.Error(), "signal: interrupt") {
				return err
			}
			return ctx.Err()
		case <-time.After(5 * time.Second):
			_ = owner.cmd.Process.Kill()
			return ctx.Err()
		}
	}
}

func tailFile(path string, maxLines int) string {
	if maxLines <= 0 {
		maxLines = 40
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n")
}
