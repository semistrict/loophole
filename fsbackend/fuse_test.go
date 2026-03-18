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
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/storage"
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

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type fuseBackend struct {
	Service
	fuseDir string
}

func newFUSEBackend(t *testing.T) *fuseBackend {
	t.Helper()
	skipIfNotLinuxRoot(t)

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}
	prefix := fmt.Sprintf("test-%s", uuid.NewString()[:8])
	inst := loophole.Instance{
		ProfileName: "test",
		Bucket:      bucket,
		Prefix:      prefix,
		Endpoint:    envOrDefault("S3_ENDPOINT", "http://localhost:9000"),
		AccessKey:   envOrDefault("AWS_ACCESS_KEY_ID", "rustfsadmin"),
		SecretKey:   envOrDefault("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
		Region:      envOrDefault("AWS_REGION", ""),
	}

	ctx := t.Context()
	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm := storage.NewManager(store, t.TempDir(), storage.Config{}, nil, nil)

	dir := loophole.Dir(t.TempDir())
	fuseDir := dir.Fuse(inst.ProfileName)
	b, err := NewFUSE(fuseDir, vm, &fuseblockdev.Options{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = b.Close(cleanupCtx)
	})
	return &fuseBackend{Service: b, fuseDir: fuseDir}
}

// loopDevicesFor returns the number of loop devices whose backing file
// is under fuseDir. This isolates each test from loop device state
// changes caused by other tests or the host (e.g. udev in OrbStack).
func loopDevicesFor(t *testing.T, fuseDir string) int {
	t.Helper()
	out, err := exec.Command("losetup", "-l", "-n", "-O", "NAME,BACK-FILE").Output()
	require.NoError(t, err)
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		if strings.Contains(line, fuseDir) {
			count++
		}
	}
	return count
}

func TestFUSE_CreateMountUnmount_CleansUpLoopDevice(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	require.Equal(t, 0, loopDevicesFor(t, b.fuseDir))

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "looptest"}))

	mp := t.TempDir()
	require.NoError(t, b.Mount(ctx, "looptest", mp))

	require.Equal(t, 1, loopDevicesFor(t, b.fuseDir), "mount should attach exactly one loop device")

	require.NoError(t, b.Unmount(ctx, mp))

	require.Eventually(t, func() bool {
		return loopDevicesFor(t, b.fuseDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "unmount should detach the loop device")
}

func TestFUSE_MultipleVolumes_AllCleaned(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	n := 3

	mps := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("multi-%d", i)
		require.NoError(t, b.Create(ctx, client.CreateParams{Volume: name}))
		mps[i] = t.TempDir()
		require.NoError(t, b.Mount(ctx, name, mps[i]))
	}

	require.Equal(t, n, loopDevicesFor(t, b.fuseDir), "should have %d loop devices attached", n)

	for _, mp := range mps {
		require.NoError(t, b.Unmount(ctx, mp))
	}

	require.Eventually(t, func() bool {
		return loopDevicesFor(t, b.fuseDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "all loop devices should be detached after unmount")
}

func TestFUSE_Close_CleansUpLoopDevices(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "closetest"}))
	mp := t.TempDir()
	require.NoError(t, b.Mount(ctx, "closetest", mp))

	require.Equal(t, 1, loopDevicesFor(t, b.fuseDir))

	// Close should unmount everything and detach loop devices.
	require.NoError(t, b.Close(ctx))

	require.Eventually(t, func() bool {
		return loopDevicesFor(t, b.fuseDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "Close should detach all loop devices")
}

func TestFUSE_CreateDoesNotLeakLoopDevice(t *testing.T) {
	b := newFUSEBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "noleak"}))

	require.Equal(t, 0, loopDevicesFor(t, b.fuseDir), "Create/Format should not leak a loop device")
}
