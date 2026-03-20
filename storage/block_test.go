package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockTombstoneRoundTrip(t *testing.T) {
	pages := []blockPage{
		{offset: 0, data: bytes.Repeat([]byte{0xAA}, PageSize)},
		{offset: 1, data: nil}, // tombstone
		{offset: 2, data: bytes.Repeat([]byte{0xBB}, PageSize)},
	}

	blob, err := buildBlock(0, pages, true)
	require.NoError(t, err)

	pb, err := parseBlock(blob, true)
	require.NoError(t, err)

	ctx := t.Context()

	// Real page 0.
	data, found, err := pb.findPage(ctx, PageIdx(0))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bytes.Repeat([]byte{0xAA}, PageSize), data)

	// Tombstone page 1: found=true, data=zeros.
	data, found, err = pb.findPage(ctx, PageIdx(1))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, zeroPage[:], data)

	// Real page 2.
	data, found, err = pb.findPage(ctx, PageIdx(2))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bytes.Repeat([]byte{0xBB}, PageSize), data)

	// Page 3 not in block.
	_, found, err = pb.findPage(ctx, PageIdx(3))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestPatchBlockWithTombstones(t *testing.T) {
	// Build initial block with pages 0, 1, 2.
	initial := []blockPage{
		{offset: 0, data: bytes.Repeat([]byte{0x11}, PageSize)},
		{offset: 1, data: bytes.Repeat([]byte{0x22}, PageSize)},
		{offset: 2, data: bytes.Repeat([]byte{0x33}, PageSize)},
	}
	blob, err := buildBlock(0, initial, true)
	require.NoError(t, err)
	pb, err := parseBlock(blob, true)
	require.NoError(t, err)

	// Patch: tombstone pages 0 and 2, add new page 3.
	newOffsets := map[uint16]struct{}{0: {}, 2: {}, 3: {}}
	existing := pb.compressedEntriesExcluding(newOffsets) // keeps page 1
	newPages := []blockPage{
		{offset: 0, data: nil}, // tombstone
		{offset: 2, data: nil}, // tombstone
		{offset: 3, data: bytes.Repeat([]byte{0x44}, PageSize)},
	}

	patched, err := patchBlock(0, existing, newPages, true)
	require.NoError(t, err)
	pb2, err := parseBlock(patched, true)
	require.NoError(t, err)

	ctx := t.Context()

	// Page 0: tombstone.
	data, found, err := pb2.findPage(ctx, PageIdx(0))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, zeroPage[:], data)

	// Page 1: original data.
	data, found, err = pb2.findPage(ctx, PageIdx(1))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bytes.Repeat([]byte{0x22}, PageSize), data)

	// Page 2: tombstone.
	data, found, err = pb2.findPage(ctx, PageIdx(2))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, zeroPage[:], data)

	// Page 3: new data.
	data, found, err = pb2.findPage(ctx, PageIdx(3))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bytes.Repeat([]byte{0x44}, PageSize), data)
}
