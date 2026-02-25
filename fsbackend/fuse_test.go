//go:build linux

package fsbackend

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fuseblockdev"
)

func skipIfNotLinuxRoot(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux")
	}
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}
}

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

func newFUSEBackend(t *testing.T) Service {
	t.Helper()
	skipIfNotLinuxRoot(t)

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}
	prefix := fmt.Sprintf("test-%s", uuid.NewString()[:8])
	raw := fmt.Sprintf("s3://%s/%s", bucket, prefix)
	inst, err := loophole.ParseInstance(raw)
	require.NoError(t, err)
	inst.Endpoint = defaultEndpoint()

	ctx := t.Context()
	store, err := loophole.NewS3Store(ctx, inst, defaultS3Options())
	require.NoError(t, err)

	_ = loophole.FormatSystem(ctx, store, 4*1024*1024)

	vm, err := loophole.NewVolumeManager(ctx, store, t.TempDir(), 20, 200)
	require.NoError(t, err)

	dir := loophole.Dir(t.TempDir())
	b, err := NewFUSE(dir.Fuse(inst), vm, &fuseblockdev.Options{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = b.Close(cleanupCtx)
	})
	return b
}

// loopDeviceCount returns the number of currently attached loop devices.
func loopDeviceCount(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("losetup", "-l", "-n").Output()
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}
	return len(lines)
}

func TestFUSE_CreateMountUnmount_CleansUpLoopDevice(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	before := loopDeviceCount(t)

	require.NoError(t, b.Create(ctx, "looptest"))

	mp := t.TempDir()
	require.NoError(t, b.Mount(ctx, "looptest", mp))

	during := loopDeviceCount(t)
	require.Equal(t, before+1, during, "mount should attach exactly one loop device")

	require.NoError(t, b.Unmount(ctx, mp))

	after := loopDeviceCount(t)
	require.Equal(t, before, after, "unmount should detach the loop device")
}

func TestFUSE_MultipleVolumes_AllCleaned(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	before := loopDeviceCount(t)
	n := 3

	mps := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("multi-%d", i)
		require.NoError(t, b.Create(ctx, name))
		mps[i] = t.TempDir()
		require.NoError(t, b.Mount(ctx, name, mps[i]))
	}

	during := loopDeviceCount(t)
	require.Equal(t, before+n, during, "should have %d loop devices attached", n)

	for _, mp := range mps {
		require.NoError(t, b.Unmount(ctx, mp))
	}

	after := loopDeviceCount(t)
	require.Equal(t, before, after, "all loop devices should be detached after unmount")
}

func TestFUSE_Close_CleansUpLoopDevices(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	before := loopDeviceCount(t)

	require.NoError(t, b.Create(ctx, "closetest"))
	mp := t.TempDir()
	require.NoError(t, b.Mount(ctx, "closetest", mp))

	require.Equal(t, before+1, loopDeviceCount(t))

	// Close should unmount everything and detach loop devices.
	require.NoError(t, b.Close(ctx))

	after := loopDeviceCount(t)
	require.Equal(t, before, after, "Close should detach all loop devices")
}

func TestFUSE_CreateDoesNotLeakLoopDevice(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	before := loopDeviceCount(t)

	require.NoError(t, b.Create(ctx, "noleak"))

	after := loopDeviceCount(t)
	require.Equal(t, before, after, "Create/Format should not leak a loop device")
}
