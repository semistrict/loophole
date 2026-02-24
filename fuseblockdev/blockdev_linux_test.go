package fuseblockdev_test

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuseFallocatePunchHole(t *testing.T) {
	env := setupFuse(t)

	data := bytes.Repeat([]byte("A"), 128)
	_, err := env.f.WriteAt(data, 0)
	require.NoError(t, err)

	err = syscall.Fallocate(int(env.f.Fd()), 0x03, 0, 64) // PUNCH_HOLE | KEEP_SIZE
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = env.f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 64), buf)

	buf2 := make([]byte, 64)
	_, err = env.f.ReadAt(buf2, 64)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("A"), 64), buf2)
}
