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

func TestDecodeHeaderRoundTrip(t *testing.T) {
	hdr := blockHeader{
		Magic:      blockMagic,
		Version:    blockVersion,
		BlockIdx:   42,
		NumEntries: 1024,
		DictSize:   0,
		DictOffset: 46,
		DataOffset: 46,
		IdxOffset:  4242880,
	}
	buf := make([]byte, blockHeaderSize)
	encodeHeader(buf, hdr)
	got := decodeHeader(buf)
	assert.Equal(t, hdr, got)
}

func TestDecodeIndexEntryRoundTrip(t *testing.T) {
	ie := blockIndexEntry{
		PageOffset: 511,
		DataOffset: 123456789,
		DataLen:    4000,
		CRC32:      0xDEADBEEF,
	}
	buf := make([]byte, blockIndexEntrySize)
	encodeIndexEntry(buf, ie)
	got := decodeIndexEntry(buf)
	assert.Equal(t, ie, got)
}

func TestPatchBlockDeduplicatesOffsetsPreferringNewPages(t *testing.T) {
	oldData := bytes.Repeat([]byte{0x11}, PageSize)
	newData := bytes.Repeat([]byte{0x22}, PageSize)

	initial, err := buildBlock(0, []blockPage{{offset: 37, data: oldData}}, true)
	require.NoError(t, err)
	pb, err := parseBlock(initial, true)
	require.NoError(t, err)

	// Simulate a buggy caller supplying the same page offset from both the
	// copied existing entries and the newly written pages. patchBlock must
	// emit a canonical block with a single entry for the latest page.
	existing := pb.compressedEntriesExcluding(map[uint16]struct{}{})
	patched, err := patchBlock(0, existing, []blockPage{{offset: 37, data: newData}}, true)
	require.NoError(t, err)

	pb2, err := parseBlock(patched, true)
	require.NoError(t, err)
	require.Len(t, pb2.index, 1)
	require.Equal(t, uint16(37), pb2.index[0].PageOffset)

	got, found, err := pb2.findPage(t.Context(), PageIdx(37))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, newData, got)
}
