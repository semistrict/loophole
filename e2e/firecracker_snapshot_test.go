//go:build linux

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const firecrackerE2EEnv = "LOOPHOLE_RUN_FIRECRACKER_E2E"

func TestE2E_FirecrackerCycledSnapshotRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Firecracker e2e in short mode")
	}
	if os.Getenv(firecrackerE2EEnv) == "" {
		t.Skip("set LOOPHOLE_RUN_FIRECRACKER_E2E=1 to run Firecracker snapshot e2e")
	}

	requireFirecrackerPrereqs(t)
	cleanupConflictingFirecrackerDemo(t)

	repoRoot := repoRoot(t)
	workDir := envOrDefault("LOOPHOLE_FIRECRACKER_WORK_DIR", "/tmp/loophole-firecracker-e2e")
	sessionName := envOrDefault("LOOPHOLE_FIRECRACKER_SESSION_NAME", "loophole-e2e")
	scriptPath := filepath.Join(repoRoot, "scripts", "loophole-e2e.sh")
	args := []string{scriptPath, "--force"}
	if os.Getenv("LOOPHOLE_FIRECRACKER_SKIP_BUILD") != "" {
		args = append(args, "--skip-build")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"WORK_DIR="+workDir,
		"SESSION_NAME="+sessionName,
		"BOOT_TIMEOUT=240",
		"CLONE_TIMEOUT=180",
		"PROMPT_TIMEOUT=60",
		"STABILITY_TIMEOUT=20",
	)

	output, err := cmd.CombinedOutput()
	dump := tmuxSessionDump(t, sessionName)

	if keep := os.Getenv("LOOPHOLE_KEEP_FIRECRACKER_SESSION"); keep == "" && !t.Failed() {
		t.Cleanup(func() {
			_ = exec.Command("tmux", "kill-session", "-t", sessionName).Run()
		})
	}

	require.NoErrorf(t, err, "firecracker e2e failed\nscript output:\n%s\n\ntmux dump:\n%s", output, dump)

	require.Contains(t, string(output), "E2E multi-generation clone heartbeat verified")
	assertPaneContains(t, dump, sessionName, 0, "ORIG-HEARTBEAT")
	assertPaneContains(t, dump, sessionName, 1, "CLONE1-HEARTBEAT")
	assertPaneContains(t, dump, sessionName, 2, "CLONE1-HEARTBEAT")
	assertNoCrashPatterns(t, dump)
}

func requireFirecrackerPrereqs(t *testing.T) {
	t.Helper()

	switch runtime.GOARCH {
	case "arm64", "amd64":
	default:
		t.Skipf("unsupported arch for Firecracker e2e: %s", runtime.GOARCH)
	}

	if !fileExists("/dev/kvm") {
		t.Skip("/dev/kvm is required for Firecracker e2e")
	}

	for _, tool := range []string{
		"bash",
		"cargo",
		"curl",
		"go",
		"ip",
		"iptables",
		"make",
		"python3",
		"sudo",
		"tmux",
		"unsquashfs",
		"wget",
	} {
		if !hasTool(tool) {
			t.Skipf("missing required tool for Firecracker e2e: %s", tool)
		}
	}

	cmd := exec.Command("sudo", "-n", "true")
	if err := cmd.Run(); err != nil {
		t.Skip("Firecracker e2e requires passwordless sudo")
	}
}

func cleanupConflictingFirecrackerDemo(t *testing.T) {
	t.Helper()

	cmd := exec.Command("bash", "-lc", `
tmux kill-session -t loophole-e2e 2>/dev/null || true
sleep 1
sudo ip netns del fc-clone-1 2>/dev/null || true
sudo ip netns del fc-clone-2 2>/dev/null || true
sudo ip link del veth-h1 2>/dev/null || true
sudo ip link del veth-h2 2>/dev/null || true
sudo ip link del tap0 2>/dev/null || true
`)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "failed to clean conflicting Firecracker demo state:\n%s", output)
}

func repoRoot(t *testing.T) string {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Clean(filepath.Join(cwd, ".."))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func tmuxSessionDump(t *testing.T, sessionName string) string {
	t.Helper()

	var out bytes.Buffer
	listCmd := exec.Command("tmux", "list-panes", "-t", sessionName, "-F", "#{pane_index}")
	panes, err := listCmd.Output()
	if err != nil {
		return fmt.Sprintf("unable to list panes for session %q: %v", sessionName, err)
	}

	for _, paneIdx := range strings.Fields(string(panes)) {
		target := fmt.Sprintf("%s:0.%s", sessionName, paneIdx)
		cmd := exec.Command("tmux", "capture-pane", "-t", target, "-p", "-S", "-300")
		data, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(&out, "--- %s (capture error: %v) ---\n", target, err)
			continue
		}
		fmt.Fprintf(&out, "--- %s ---\n%s\n", target, data)
	}

	return out.String()
}

func assertPaneContains(t *testing.T, dump, sessionName string, pane int, needle string) {
	t.Helper()
	sectionHeader := fmt.Sprintf("--- %s:0.%d ---", sessionName, pane)
	require.Containsf(t, dump, sectionHeader, "missing tmux pane dump for pane %d", pane)
	start := strings.Index(dump, sectionHeader)
	require.NotEqual(t, -1, start)
	section := dump[start:]
	if next := strings.Index(section[len(sectionHeader):], "\n--- "); next >= 0 {
		section = section[:len(sectionHeader)+next+1]
	}
	require.Containsf(t, section, needle, "missing %q in pane %d", needle, pane)
}

func assertNoCrashPatterns(t *testing.T, dump string) {
	t.Helper()
	for _, pattern := range []string{
		"Segmentation fault",
		"Unable to handle kernel NULL pointer dereference",
		"Unable to handle kernel paging request",
		"Internal error: Oops",
		"Kernel panic",
	} {
		require.NotContainsf(t, dump, pattern, "unexpected crash signature in tmux dump: %s", pattern)
	}
}
