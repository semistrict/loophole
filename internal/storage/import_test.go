package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/stretchr/testify/require"
)

func TestCreateVolumeFromImage(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	_, _, err := FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	// Two full blocks of non-zero data.
	imageData := bytes.Repeat([]byte("abcd"), BlockSize/4)
	imageData = append(imageData, bytes.Repeat([]byte("wxyz"), BlockSize/4)...)

	require.NoError(t, CreateVolumeFromImage(ctx, store, "seeded", VolumeTypeExt4, bytes.NewReader(imageData)))

	// Open the volume and read back.
	m := &Manager{BlobStore: store}
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	vol, err := m.OpenVolume("seeded")
	require.NoError(t, err)

	got, err := vol.ReadAt(ctx, 0, len(imageData))
	require.NoError(t, err)
	require.Equal(t, imageData, got)
}

func TestCreateVolumeFromImageSparse(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	_, _, err := FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	// First block is all zeros (should be skipped), second block has data.
	imageData := make([]byte, 2*BlockSize)
	for i := BlockSize; i < 2*BlockSize; i++ {
		imageData[i] = byte((i * 7) % 251)
	}

	require.NoError(t, CreateVolumeFromImage(ctx, store, "sparse", VolumeTypeExt4, bytes.NewReader(imageData)))

	m := &Manager{BlobStore: store}
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	vol, err := m.OpenVolume("sparse")
	require.NoError(t, err)

	// First block should be zeros.
	zeros, err := vol.ReadAt(ctx, 0, BlockSize)
	require.NoError(t, err)
	require.Equal(t, make([]byte, BlockSize), zeros)

	// Second block should have data.
	got, err := vol.ReadAt(ctx, BlockSize, BlockSize)
	require.NoError(t, err)
	require.Equal(t, imageData[BlockSize:], got)
}

func TestCreateVolumeFromImagePadsTail(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	_, _, err := FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	// Image not aligned to PageSize.
	imageData := bytes.Repeat([]byte("z"), PageSize+123)

	require.NoError(t, CreateVolumeFromImage(ctx, store, "tail", VolumeTypeExt4, bytes.NewReader(imageData)))

	m := &Manager{BlobStore: store}
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	vol, err := m.OpenVolume("tail")
	require.NoError(t, err)

	got, err := vol.ReadAt(ctx, 0, len(imageData))
	require.NoError(t, err)
	require.Equal(t, imageData, got)

	// Padded tail should be zeros.
	paddedTail, err := vol.ReadAt(ctx, uint64(len(imageData)), PageSize-123)
	require.NoError(t, err)
	require.Equal(t, make([]byte, PageSize-123), paddedTail)
}

func TestCreateVolumeFromImageDuplicateName(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	_, _, err := FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	data := bytes.Repeat([]byte{0xAA}, PageSize)

	require.NoError(t, CreateVolumeFromImage(ctx, store, "dup", VolumeTypeExt4, bytes.NewReader(data)))
	err = CreateVolumeFromImage(ctx, store, "dup", VolumeTypeExt4, bytes.NewReader(data))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}
