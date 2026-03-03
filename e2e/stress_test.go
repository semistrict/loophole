//go:build linux

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// Stress tests are Linux-only: they need real ext4 mounts for fsx/fio.

func stressMount(t *testing.T, name string) string {
	t.Helper()
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, name)
	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: name}))
	err := b.Mount(ctx, name, mp)
	require.NoError(t, err)
	return mp
}

func TestE2E_FsxBasic(t *testing.T) {
	if !hasTool("fsx") {
		t.Skip("fsx not installed")
	}
	mp := stressMount(t, "fsx-basic")
	runCmd(t, "fsx", "-N", "5000", "-l", "1048576", "-S", "42", fmt.Sprintf("%s/fsx-testfile", mp))
}

func TestE2E_FsxWithPunchHole(t *testing.T) {
	skipKernelOnly(t)
	if !hasTool("fsx") {
		t.Skip("fsx not installed")
	}
	mp := stressMount(t, "fsx-punch")
	runCmd(t, "fsx", "-H", "-N", "5000", "-l", "1048576", "-S", "123", fmt.Sprintf("%s/fsx-punch-testfile", mp))
}

func TestE2E_FsxHeavy(t *testing.T) {
	if !hasTool("fsx") {
		t.Skip("fsx not installed")
	}
	mp := stressMount(t, "fsx-heavy")
	runCmd(t, "fsx", "-H", "-N", "20000", "-l", "4194304", "-S", "999", fmt.Sprintf("%s/fsx-heavy-testfile", mp))
}

func TestE2E_FioRandomRW(t *testing.T) {
	if !hasTool("fio") {
		t.Skip("fio not installed")
	}
	mp := stressMount(t, "fio-randrw")
	runCmd(t, "fio",
		"--name=randrw",
		fmt.Sprintf("--directory=%s", mp),
		"--rw=randrw",
		"--bs=4k",
		"--size=8M",
		"--numjobs=2",
		"--runtime=15",
		"--time_based",
		"--verify=crc32c",
		"--verify_fatal=1",
		"--group_reporting",
	)
}

func TestE2E_FioSequentialWriteVerify(t *testing.T) {
	if !hasTool("fio") {
		t.Skip("fio not installed")
	}
	mp := stressMount(t, "fio-seqver")

	// Write phase.
	runCmd(t, "fio",
		"--name=seqwrite",
		fmt.Sprintf("--directory=%s", mp),
		"--rw=write",
		"--bs=64k",
		"--size=16M",
		"--verify=sha256",
		"--verify_fatal=1",
		"--do_verify=0",
	)
	syncFS(t, mp)

	// Verify phase.
	runCmd(t, "fio",
		"--name=seqwrite",
		fmt.Sprintf("--directory=%s", mp),
		"--rw=write",
		"--bs=64k",
		"--size=16M",
		"--verify=sha256",
		"--verify_fatal=1",
		"--verify_only",
	)
}

func TestE2E_FioFallocatePunchHole(t *testing.T) {
	skipKernelOnly(t)
	if !hasTool("fio") {
		t.Skip("fio not installed")
	}
	mp := stressMount(t, "fio-falloc")
	cmd := exec.Command("fio",
		"--name=falloc",
		fmt.Sprintf("--directory=%s", mp),
		"--rw=randwrite",
		"--bs=4k",
		"--size=4M",
		"--fallocate=keep",
		"--numjobs=1",
		"--runtime=10",
		"--time_based",
		"--verify=crc32c",
		"--verify_fatal=1",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		// Dump recent kernel log to see what ext4 error caused the failure.
		dmesg, _ := exec.Command("dmesg", "-T", "--since", "60 seconds ago").Output()
		t.Logf("dmesg (last 60s):\n%s", string(dmesg))
		t.Fatalf("fio failed: %v", err)
	}
}
