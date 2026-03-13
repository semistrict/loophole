//go:build linux

package e2e

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestE2E_ExecChroots(t *testing.T) {
	skipE2E(t)
	setupBusyboxVolume(t, "exec-chroot")

	// The exec endpoint should chroot — marker.txt should be at /.
	r := sandboxExec(t, "exec-chroot", "cat+/marker.txt")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, "chrooted\n", r.Stdout)

	// /etc/resolv.conf from host should be visible.
	hostResolv, err := os.ReadFile("/etc/resolv.conf")
	require.NoError(t, err)
	r = sandboxExec(t, "exec-chroot", "cat+/etc/resolv.conf")
	require.Equal(t, 0, r.ExitCode)
	require.Equal(t, string(hostResolv), r.Stdout)
}

func TestE2E_ExecHasProc(t *testing.T) {
	skipE2E(t)
	setupBusyboxVolume(t, "exec-proc")

	// /proc should be mounted.
	r := sandboxExec(t, "exec-proc", "cat+/proc/1/comm")
	require.Equal(t, 0, r.ExitCode)
	require.NotEmpty(t, strings.TrimSpace(r.Stdout))

	// /proc/self should exist.
	r = sandboxExec(t, "exec-proc", "ls+/proc/self")
	require.Equal(t, 0, r.ExitCode)
}

func TestE2E_ExecWithoutVolume(t *testing.T) {
	t.Skip("host-only sandbox exec is not supported in the single-volume owner model")
}
