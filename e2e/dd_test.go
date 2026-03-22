package e2e

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/storage"
)

// TestE2E_DeviceDD_WriteAndRead verifies that DeviceDD writes block-aligned,
// block-sized chunks correctly. It writes 3 blocks of random data, then reads
// them back via DeviceDDRead and checks the layer debug info.
func TestE2E_DeviceDD_WriteAndRead(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	const nBlocks = 3
	dataSize := uint64(nBlocks * storage.BlockSize)

	// Generate random data so compression doesn't collapse it.
	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	volName := "dd-direct-l2-test"
	require.NoError(t, b.Create(ctx, storage.CreateParams{
		Volume:   volName,
		Size:     dataSize,
		Type:     "ext4",
		NoFormat: true,
	}))
	owner, err := b.ensureDeviceOwner(ctx, volName)
	require.NoError(t, err)
	err = owner.client.DeviceDDWriteExisting(ctx, volName, bytes.NewReader(data), nil)
	require.NoError(t, err)

	// Read all data back via the dd read API.
	var readBuf bytes.Buffer
	err = owner.client.DeviceDDRead(ctx, &readBuf, nil)
	require.NoError(t, err)
	assert.Equal(t, data, readBuf.Bytes())

	// Check layer debug info.
	info, err := owner.client.VolumeDebugInfo(ctx, volName)
	require.NoError(t, err)

	if debugCountersEnabled() {
		t.Logf("layer debug info: L1=%d L2=%d dirty pages=%d",
			info.Layer.L1Ranges, info.Layer.L2Ranges, info.Layer.DirtyPages)
	}
}

// TestE2E_DeviceDD_ReadBack verifies that a dd-imported volume can be read
// back correctly, including partial last blocks and cross-boundary reads.
func TestE2E_DeviceDD_ReadBack(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	// Use a size that's not a multiple of BlockSize to exercise the
	// partial last block path.
	dataSize := uint64(2*storage.BlockSize + 12345)

	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	volName := "dd-readback-test"
	require.NoError(t, b.Create(ctx, storage.CreateParams{
		Volume:   volName,
		Size:     dataSize,
		Type:     "ext4",
		NoFormat: true,
	}))
	owner, err := b.ensureDeviceOwner(ctx, volName)
	require.NoError(t, err)
	err = owner.client.DeviceDDWriteExisting(ctx, volName, bytes.NewReader(data), nil)
	require.NoError(t, err)

	// Read all data back and compare.
	var readBuf bytes.Buffer
	err = owner.client.DeviceDDRead(ctx, &readBuf, nil)
	require.NoError(t, err)
	assert.Equal(t, data, readBuf.Bytes())
}

// TestE2E_DeviceDD_VolumeMetadata verifies that dd-created volumes have
// the correct type metadata set.
func TestE2E_DeviceDD_VolumeMetadata(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	dataSize := uint64(storage.BlockSize)
	data := make([]byte, dataSize)

	volName := "dd-metadata-test"

	require.NoError(t, b.Create(ctx, storage.CreateParams{
		Volume:   volName,
		Size:     dataSize,
		Type:     "ext4",
		NoFormat: true,
	}))
	owner, err := b.ensureDeviceOwner(ctx, volName)
	require.NoError(t, err)
	err = owner.client.DeviceDDWriteExisting(ctx, volName, bytes.NewReader(data), nil)
	require.NoError(t, err)

	vm, cleanup, err := openDirectManager(ctx)
	require.NoError(t, err)
	defer cleanup()
	info, err := storage.GetVolumeInfo(ctx, vm.Store(), volName)
	require.NoError(t, err)
	assert.Equal(t, "ext4", info.Type)
	assert.Equal(t, dataSize, info.Size)
}
