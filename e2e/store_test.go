//go:build linux

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Store tests are kernel-only: they test the raw FUSE device layout.

func TestE2E_VolumeFileExists(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()

	err := b.Create(ctx, "testvolume")
	require.NoError(t, err)

	device, err := b.DeviceMount(ctx, "testvolume")
	require.NoError(t, err)
	require.FileExists(t, device)
}
