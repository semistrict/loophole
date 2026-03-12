//go:build linux

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
)

func TestE2E_ExecFirecracker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Firecracker e2e in short mode")
	}
	if os.Getenv(firecrackerE2EEnv) == "" {
		t.Skip("set LOOPHOLE_RUN_FIRECRACKER_E2E=1 to run Firecracker exec e2e")
	}
	if runtime.GOARCH != "amd64" {
		t.Skipf("Firecracker exec e2e currently requires amd64 guest binary, got %s", runtime.GOARCH)
	}

	requireFirecrackerExecAssets(t)
	m := mode()

	dir := loophole.Dir(t.TempDir())
	storeDir := filepath.Join(string(dir), "store")
	require.NoError(t, os.MkdirAll(storeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(string(dir), "config.toml"), []byte(fmt.Sprintf(`
default_profile = "test"

[profiles.test]
local_dir = %q
mode = "%s"
log_level = "debug"
sandbox_mode = "firecracker"
`, storeDir, m)), 0o644))

	cfg, err := loophole.LoadConfig(dir)
	require.NoError(t, err)
	inst, err := cfg.Resolve("test")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	d, err := daemon.Start(ctx, inst, dir, daemon.Options{Foreground: true})
	require.NoError(t, err)
	go func() {
		_ = d.Serve(ctx)
	}()

	c := client.New(dir, inst)
	waitForDaemonReady(t, c)

	volume := "exec-firecracker"
	rootfsImage := buildFirecrackerRootfsImage(t)

	imgSize, err := fileSize(rootfsImage)
	require.NoError(t, err)
	image, err := os.Open(rootfsImage)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, image.Close())
	}()
	require.NoError(t, c.DeviceDD(ctx, client.CreateParams{
		Type:   defaultVolumeType(),
		Volume: volume,
		Size:   imgSize,
	}, image, nil))
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanCancel()
		_ = c.Delete(cleanCtx, volume)
	})

	r := sandboxExecWithSocket(t, dir.Socket(inst.ProfileName), volume, "echo+hello")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "hello\n", r.Stdout)

	r = sandboxExecWithSocket(t, dir.Socket(inst.ProfileName), volume, "cat+/etc/os-release")
	require.Equal(t, 0, r.ExitCode)
	require.Contains(t, r.Stdout, "Loophole Firecracker Test")

	r = sandboxExecWithSocket(t, dir.Socket(inst.ProfileName), volume, "cat+/proc/1/comm")
	require.Equal(t, 0, r.ExitCode)
}

func TestE2E_FirecrackerClone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Firecracker e2e in short mode")
	}
	if os.Getenv(firecrackerE2EEnv) == "" {
		t.Skip("set LOOPHOLE_RUN_FIRECRACKER_E2E=1 to run Firecracker clone e2e")
	}
	if runtime.GOARCH != "amd64" {
		t.Skipf("Firecracker clone e2e currently requires amd64 guest binary, got %s", runtime.GOARCH)
	}

	requireFirecrackerExecAssets(t)
	m := mode()

	dir := loophole.Dir(t.TempDir())
	storeDir := filepath.Join(string(dir), "store")
	require.NoError(t, os.MkdirAll(storeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(string(dir), "config.toml"), []byte(fmt.Sprintf(`
default_profile = "test"

[profiles.test]
local_dir = %q
mode = "%s"
log_level = "debug"
sandbox_mode = "firecracker"
`, storeDir, m)), 0o644))

	cfg, err := loophole.LoadConfig(dir)
	require.NoError(t, err)
	inst, err := cfg.Resolve("test")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	d, err := daemon.Start(ctx, inst, dir, daemon.Options{Foreground: true})
	require.NoError(t, err)
	go func() {
		_ = d.Serve(ctx)
	}()

	c := client.New(dir, inst)
	waitForDaemonReady(t, c)

	volume := "clone-test-rootfs"
	rootfsImage := buildFirecrackerRootfsImage(t)

	imgSize, err := fileSize(rootfsImage)
	require.NoError(t, err)
	image, err := os.Open(rootfsImage)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, image.Close())
	}()
	require.NoError(t, c.DeviceDD(ctx, client.CreateParams{
		Type:   defaultVolumeType(),
		Volume: volume,
		Size:   imgSize,
	}, image, nil))
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanCancel()
		_ = c.Delete(cleanCtx, volume)
	})

	sock := dir.Socket(inst.ProfileName)

	// Step 1: Boot VM by running a command.
	r := sandboxExecWithSocket(t, sock, volume, "echo+hello")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "hello\n", r.Stdout)

	// Step 2: Snapshot the running VM.
	snap := vmSnapshotWithSocket(t, sock, volume)
	require.Equal(t, volume, snap.SourceVolume)
	require.NotEmpty(t, snap.SnapshotPath)
	require.NotEmpty(t, snap.MemCloneVolume)

	// Step 3: Restore the snapshot into a clone.
	cloneName := "sandbox-clone-1"
	cloneResult := vmRestoreWithSocket(t, sock, snap, cloneName)
	require.Equal(t, cloneName, cloneResult.Volume)
	require.NotZero(t, cloneResult.GuestCID)
	require.NotEmpty(t, cloneResult.Netns)

	// Step 4: Exec on the clone — verify it works independently.
	r = sandboxExecWithSocket(t, sock, cloneName, "echo+clone-works")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "clone-works\n", r.Stdout)

	// Step 5: Verify original VM still works.
	r = sandboxExecWithSocket(t, sock, volume, "echo+original-still-works")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "original-still-works\n", r.Stdout)
}

func vmSnapshotWithSocket(t *testing.T, sock, volume string) daemon.VMSnapshot {
	t.Helper()
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}

	reqBody, err := json.Marshal(map[string]string{"volume": volume})
	require.NoError(t, err)

	resp, err := hc.Post("http://loophole/sandbox/vm/snapshot", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "snapshot failed: %s", string(body))

	var snap daemon.VMSnapshot
	require.NoError(t, json.Unmarshal(body, &snap))
	return snap
}

func vmRestoreWithSocket(t *testing.T, sock string, snap daemon.VMSnapshot, cloneName string) daemon.VMCloneResult {
	t.Helper()
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}

	reqBody, err := json.Marshal(map[string]string{
		"snapshot_path":    snap.SnapshotPath,
		"mem_clone_volume": snap.MemCloneVolume,
		"source_volume":    snap.SourceVolume,
		"clone_name":       cloneName,
	})
	require.NoError(t, err)

	resp, err := hc.Post("http://loophole/sandbox/vm/restore", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "restore failed: %s", string(body))

	var result daemon.VMCloneResult
	require.NoError(t, json.Unmarshal(body, &result))
	return result
}

func requireFirecrackerExecAssets(t *testing.T) {
	t.Helper()
	requireFirecrackerPrereqs(t)
	require.True(t, hasTool("mkfs.ext4"), "mkfs.ext4 is required for Firecracker exec test")
	require.True(t, hasTool("ip"), "ip (iproute2) is required for Firecracker exec test")
	require.True(t, hasTool("iptables"), "iptables is required for Firecracker exec test")

	workDir := envOrDefault("LOOPHOLE_FIRECRACKER_WORK_DIR", "/tmp/loophole-firecracker-e2e")
	if v := os.Getenv("LOOPHOLE_FIRECRACKER_BIN"); v == "" {
		v = filepath.Join(workDir, "firecracker", "build", "cargo_target", "debug", "firecracker")
		if _, err := os.Stat(v); err != nil {
			t.Skipf("missing firecracker binary at %s (or set LOOPHOLE_FIRECRACKER_BIN)", v)
		}
		t.Setenv("LOOPHOLE_FIRECRACKER_BIN", v)
	}
	if v := os.Getenv("LOOPHOLE_FIRECRACKER_KERNEL"); v == "" {
		v = filepath.Join(workDir, "vmlinux")
		if _, err := os.Stat(v); err != nil {
			t.Skipf("missing firecracker kernel at %s (or set LOOPHOLE_FIRECRACKER_KERNEL)", v)
		}
		t.Setenv("LOOPHOLE_FIRECRACKER_KERNEL", v)
	}
	t.Setenv("LOOPHOLE_FIRECRACKER_BOOT_TIMEOUT", "30s")
}

func buildFirecrackerRootfsImage(t *testing.T) string {
	t.Helper()

	repo := repoRoot(t)
	guestAgent := filepath.Join(repo, "cf-demo", "bin", "loophole-guest-agent")
	if _, err := os.Stat(guestAgent); os.IsNotExist(err) {
		cmd := exec.Command("make", "cf-demo-guest-agent-bin")
		cmd.Dir = repo
		output, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "build guest agent:\n%s", output)
	}

	busybox := findBusybox(t)
	rootDir := filepath.Join(t.TempDir(), "fc-rootfs")
	require.NoError(t, os.MkdirAll(filepath.Join(rootDir, "bin"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(rootDir, "etc"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(rootDir, "sbin"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(rootDir, "tmp"), 0o755))

	copyFileTo(t, busybox, filepath.Join(rootDir, "bin", "busybox"), 0o755)
	copyFileTo(t, guestAgent, filepath.Join(rootDir, "sbin", "loophole-guest-agent"), 0o755)
	copyStringTo(t, "NAME=Loophole Firecracker Test\nID=loophole-firecracker\n", filepath.Join(rootDir, "etc", "os-release"), 0o644)

	for _, applet := range []string{"sh", "cat", "ls", "echo"} {
		require.NoError(t, os.Symlink("busybox", filepath.Join(rootDir, "bin", applet)))
	}

	imagePath := filepath.Join(t.TempDir(), "rootfs.ext4")
	const imageSize = 1 << 30
	f, err := os.Create(imagePath)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(imageSize))
	require.NoError(t, f.Close())
	cmd := exec.Command("mkfs.ext4", "-d", rootDir, "-F", imagePath)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "mkfs.ext4:\n%s", output)
	return imagePath
}

func copyFileTo(t *testing.T, src string, dst string, mode os.FileMode) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, data, mode))
}

func copyStringTo(t *testing.T, text string, dst string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(dst, []byte(text), mode))
}

func fileSize(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func sandboxExecWithSocket(t *testing.T, sock, volume, cmd string) execResult {
	t.Helper()
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}

	url := "http://loophole/sandbox/exec?cmd=" + cmd
	if volume != "" {
		url += "&volume=" + volume
	}
	resp, err := hc.Post(url, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "exec failed: %s", string(body))

	var result execResult
	require.NoError(t, json.Unmarshal(body, &result))
	return result
}

func waitForDaemonReady(t *testing.T, c *client.Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := c.Status(t.Context()); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for daemon socket %s", c.Socket())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
