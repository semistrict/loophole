package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/storage"
)

// Store tests are kernel-only: they test the raw FUSE device layout.

func TestE2E_VolumeFileExists(t *testing.T) {
	skipKernelOnly(t)
	b := newBackend(t)
	ctx := t.Context()

	err := b.Create(ctx, storage.CreateParams{Volume: "testvolume"})
	require.NoError(t, err)

	device, err := b.DeviceAttach(ctx, "testvolume")
	require.NoError(t, err)
	require.FileExists(t, device)
}
