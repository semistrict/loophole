package e2e

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/storage2"
)

// TestE2E_DeviceDD_DirectL2 verifies that DeviceDD writes block-aligned,
// block-sized chunks that land directly in L2 (bypassing memtable → L0 →
// compaction). It writes 3 blocks of random data, then reads them back via
// DeviceDDRead and checks the layer debug info.
func TestE2E_DeviceDD_DirectL2(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	const nBlocks = 3
	dataSize := uint64(nBlocks * storage2.BlockSize)

	// Generate random data so compression doesn't collapse it.
	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	volName := "dd-direct-l2-test"

	err = testClient.DeviceDD(ctx, client.CreateParams{
		Volume: volName,
		Size:   dataSize,
		Type:   "ext4",
	}, bytes.NewReader(data), nil)
	require.NoError(t, err)
	b.createdVols = append(b.createdVols, volName)

	// Read all data back via the dd read API.
	var readBuf bytes.Buffer
	err = testClient.DeviceDDRead(ctx, volName, dataSize, &readBuf, nil)
	require.NoError(t, err)
	assert.Equal(t, data, readBuf.Bytes())

	// Check that data went directly to L2, not through memtable/L0/L1.
	info, err := testClient.VolumeDebugInfo(ctx, volName)
	require.NoError(t, err)

	t.Logf("layer debug info: L0=%d L1=%d L2=%d memtable=%d",
		info.Layer.L0Count, info.Layer.L1Ranges, info.Layer.L2Ranges, info.Layer.MemtablePages)

	assert.Equal(t, 0, info.Layer.L0Count, "L0 should be empty (direct L2 path)")
	assert.Equal(t, 0, info.Layer.L1Ranges, "L1 should be empty (direct L2 path)")
	assert.Equal(t, 0, info.Layer.MemtablePages, "memtable should be empty (direct L2 path)")
	assert.Greater(t, info.Layer.L2Ranges, 0, "L2 should have entries")
}

// TestE2E_DeviceDD_ReadBack verifies that a dd-imported volume can be read
// back correctly, including partial last blocks and cross-boundary reads.
func TestE2E_DeviceDD_ReadBack(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	// Use a size that's not a multiple of BlockSize to exercise the
	// partial last block path.
	dataSize := uint64(2*storage2.BlockSize + 12345)

	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	volName := "dd-readback-test"

	err = testClient.DeviceDD(ctx, client.CreateParams{
		Volume: volName,
		Size:   dataSize,
		Type:   "ext4",
	}, bytes.NewReader(data), nil)
	require.NoError(t, err)
	b.createdVols = append(b.createdVols, volName)

	// Read all data back and compare.
	var readBuf bytes.Buffer
	err = testClient.DeviceDDRead(ctx, volName, dataSize, &readBuf, nil)
	require.NoError(t, err)
	assert.Equal(t, data, readBuf.Bytes())
}

// TestE2E_DeviceDD_VolumeMetadata verifies that dd-created volumes have
// the correct type metadata set.
func TestE2E_DeviceDD_VolumeMetadata(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	dataSize := uint64(storage2.BlockSize)
	data := make([]byte, dataSize)

	volName := "dd-metadata-test"

	err := testClient.DeviceDD(ctx, client.CreateParams{
		Volume: volName,
		Size:   dataSize,
		Type:   "ext4",
	}, bytes.NewReader(data), nil)
	require.NoError(t, err)
	b.createdVols = append(b.createdVols, volName)

	info, err := testClient.VolumeInfo(ctx, volName)
	require.NoError(t, err)
	assert.Equal(t, "ext4", info.Type)
	assert.Equal(t, dataSize, info.Size)
}
