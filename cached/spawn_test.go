package cached

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnsureDaemonFastPathWithExistingSocket(t *testing.T) {
	dir := t.TempDir()
	sock := SocketPath(dir)
	require.NoError(t, os.MkdirAll(filepath.Dir(sock), 0o755))

	l, err := net.Listen("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, l.Close())
	})

	require.NoError(t, EnsureDaemon(dir))
}

func TestEnsureDaemonReturnsBinaryLookupErrorWithoutSpawning(t *testing.T) {
	t.Setenv("LOOPHOLE_CACHED_BIN", filepath.Join(t.TempDir(), "missing-loophole"))
	t.Setenv("PATH", "")

	err := EnsureDaemon(t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "find loophole binary")
}

func TestFindLoopholeBinaryUsesEnvOverride(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "loophole")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755))

	t.Setenv("LOOPHOLE_CACHED_BIN", bin)

	got, err := findLoopholeBinary()
	require.NoError(t, err)
	require.Equal(t, bin, got)
}
