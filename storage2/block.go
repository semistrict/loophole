package storage2

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

// Block file format (L1 and L2):
//
//   [header][dict bytes][compressed page data...][index entries...]
//
// The header stores a shared zstd dictionary trained from the pages in the
// block. Each page is independently compressed using that dictionary and has
// its own CRC32. To read a single page, fetch the header (for the dictionary)
// then GetRange the compressed page bytes.
//
// L1 blocks are sparse (only changed pages). L2 blocks are dense (all pages
// in the 4MB region that have data). Both use the same format.

var blockMagic = [4]byte{'L', 'H', 'B', 'K'}

const blockVersion = 1

type blockHeader struct {
	Magic      [4]byte
	Version    uint16
	BlockIdx   BlockIdx
	NumEntries uint32 // number of page entries
	DictSize   uint32 // size of zstd dictionary (0 = no dictionary)
	DictOffset uint64 // byte offset of dictionary
	DataOffset uint64 // byte offset of first compressed page
	IdxOffset  uint64 // byte offset of index entries
}

const blockHeaderSize = 4 + 2 + 8 + 4 + 4 + 8 + 8 + 8 // 46 bytes

type blockIndexEntry struct {
	PageOffset uint16 // offset within block (0..BlockPages-1)
	DataOffset uint64 // byte offset of compressed page in file
	DataLen    uint32 // compressed length
	CRC32      uint32 // CRC of compressed data
}

const blockIndexEntrySize = 2 + 8 + 4 + 4 // 18 bytes

// blockPage pairs a page offset within a block with its uncompressed data.
type blockPage struct {
	offset uint16 // 0..BlockPages-1
	data   []byte // PageSize bytes
}

// buildBlock serializes pages into the block file format with an optional
// shared zstd dictionary. Pages must belong to the same block address.
// Returns the serialized blob.
func buildBlock(blockIdx BlockIdx, pages []blockPage) ([]byte, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("no pages")
	}

	// Sort by page offset for binary search on read.
	sort.Slice(pages, func(i, j int) bool {
		return pages[i].offset < pages[j].offset
	})

	// Train a shared dictionary from the page data.
	var dictBytes []byte
	if len(pages) >= 4 {
		samples := make([][]byte, len(pages))
		for i, p := range pages {
			samples[i] = p.data
		}
		dictBytes = tryBuildDict(samples)
	}

	// Create encoder (with or without dictionary).
	var encOpts []zstd.EOption
	encOpts = append(encOpts,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if len(dictBytes) > 0 {
		encOpts = append(encOpts, zstd.WithEncoderDict(dictBytes))
	}
	enc, err := zstd.NewWriter(nil, encOpts...)
	if err != nil {
		return nil, fmt.Errorf("create encoder: %w", err)
	}
	defer util.SafeClose(enc, "zstd encoder")

	var buf bytes.Buffer

	// Reserve space for header.
	buf.Write(make([]byte, blockHeaderSize))

	// Write dictionary.
	dictOffset := uint64(buf.Len())
	if len(dictBytes) > 0 {
		buf.Write(dictBytes)
	}

	// Compress each page and record index entries.
	dataOffset := uint64(buf.Len())
	indexEntries := make([]blockIndexEntry, len(pages))
	for i, p := range pages {
		if len(p.data) != PageSize {
			return nil, fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(p.data))
		}
		compressed := enc.EncodeAll(p.data, nil)
		ie := blockIndexEntry{
			PageOffset: p.offset,
			DataOffset: uint64(buf.Len()),
			DataLen:    uint32(len(compressed)),
			CRC32:      crc32.ChecksumIEEE(compressed),
		}
		buf.Write(compressed)
		indexEntries[i] = ie
	}

	// Write index.
	idxOffset := uint64(buf.Len())
	for _, ie := range indexEntries {
		if err := binary.Write(&buf, binary.LittleEndian, ie); err != nil {
			return nil, fmt.Errorf("write index entry: %w", err)
		}
	}

	// Backfill header.
	hdr := blockHeader{
		Magic:      blockMagic,
		Version:    blockVersion,
		BlockIdx:   blockIdx,
		NumEntries: uint32(len(pages)),
		DictSize:   uint32(len(dictBytes)),
		DictOffset: dictOffset,
		DataOffset: dataOffset,
		IdxOffset:  idxOffset,
	}

	result := buf.Bytes()
	var hdrBuf bytes.Buffer
	if err := binary.Write(&hdrBuf, binary.LittleEndian, hdr); err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	copy(result[:blockHeaderSize], hdrBuf.Bytes())

	return result, nil
}

// parsedBlock holds a parsed block file's header, dictionary, and index.
type parsedBlock struct {
	data      []byte // full blob; nil for range-read mode
	header    blockHeader
	dictBytes []byte // shared dictionary; may be nil
	index     []blockIndexEntry
	store     loophole.ObjectStore
	key       string
}

// parseBlock parses a complete block blob.
func parseBlock(data []byte) (*parsedBlock, error) {
	if len(data) < blockHeaderSize {
		return nil, fmt.Errorf("block file too small: %d bytes", len(data))
	}

	var hdr blockHeader
	if err := binary.Read(bytes.NewReader(data[:blockHeaderSize]), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr.Magic != blockMagic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}

	// Extract dictionary.
	var dictBytes []byte
	if hdr.DictSize > 0 {
		dictEnd := hdr.DictOffset + uint64(hdr.DictSize)
		if dictEnd > uint64(len(data)) {
			return nil, fmt.Errorf("dictionary extends beyond data")
		}
		dictBytes = make([]byte, hdr.DictSize)
		copy(dictBytes, data[hdr.DictOffset:dictEnd])
	}

	// Parse index.
	idxEnd := hdr.IdxOffset + uint64(hdr.NumEntries)*blockIndexEntrySize
	if idxEnd > uint64(len(data)) {
		return nil, fmt.Errorf("index extends beyond data")
	}
	entries := make([]blockIndexEntry, hdr.NumEntries)
	r := bytes.NewReader(data[hdr.IdxOffset:idxEnd])
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedBlock{
		data:      data,
		header:    hdr,
		dictBytes: dictBytes,
		index:     entries,
	}, nil
}

// findPage searches for a page by its absolute page address within this block.
func (pb *parsedBlock) findPage(ctx context.Context, pageIdx PageIdx) ([]byte, bool, error) {
	// Convert absolute page address to block-relative offset.
	pageOffset := uint16(pageIdx % BlockPages)

	// Binary search the sorted index.
	i := sort.Search(len(pb.index), func(i int) bool {
		return pb.index[i].PageOffset >= pageOffset
	})
	if i >= len(pb.index) || pb.index[i].PageOffset != pageOffset {
		return nil, false, nil
	}

	ie := &pb.index[i]
	data, err := pb.decompressPage(ctx, ie, pageIdx)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// readAllPages reads and decompresses all pages in the block. Returns a map
// of page offset → decompressed data.
func (pb *parsedBlock) readAllPages(ctx context.Context, blockIdx BlockIdx) (map[uint16][]byte, error) {
	pages := make(map[uint16][]byte, len(pb.index))
	for i := range pb.index {
		ie := &pb.index[i]
		pageIdx := blockIdx.PageIdx(ie.PageOffset)
		data, err := pb.decompressPage(ctx, ie, pageIdx)
		if err != nil {
			return nil, err
		}
		pages[ie.PageOffset] = data
	}
	return pages, nil
}

// tryBuildDict attempts to build a zstd shared dictionary from samples.
// Returns nil if building fails (e.g. low-entropy data).
func tryBuildDict(samples [][]byte) []byte {
	d, err := dict.BuildZstdDict(samples, dict.Options{
		MaxDictSize: 32 * 1024,
		HashBytes:   6,
		ZstdLevel:   zstd.SpeedDefault,
	})
	if err != nil {
		return nil
	}
	return d
}

func (pb *parsedBlock) decompressPage(ctx context.Context, ie *blockIndexEntry, pageIdx PageIdx) ([]byte, error) {
	// Fetch compressed data.
	var compressed []byte
	if pb.data != nil {
		end := ie.DataOffset + uint64(ie.DataLen)
		if end > uint64(len(pb.data)) {
			return nil, fmt.Errorf("page data extends beyond blob: offset=%d len=%d size=%d", ie.DataOffset, ie.DataLen, len(pb.data))
		}
		compressed = pb.data[ie.DataOffset:end]
	} else {
		body, _, err := pb.store.GetRange(ctx, pb.key, int64(ie.DataOffset), int64(ie.DataLen))
		if err != nil {
			return nil, fmt.Errorf("range read page %d: %w", pageIdx, err)
		}
		compressed, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, fmt.Errorf("read page %d body: %w", pageIdx, err)
		}
	}

	// Verify CRC.
	if crc32.ChecksumIEEE(compressed) != ie.CRC32 {
		return nil, fmt.Errorf("CRC mismatch for page %d", pageIdx)
	}

	// Decompress with dictionary.
	var decOpts []zstd.DOption
	decOpts = append(decOpts, zstd.WithDecoderConcurrency(1))
	if len(pb.dictBytes) > 0 {
		decOpts = append(decOpts, zstd.WithDecoderDicts(pb.dictBytes))
	}
	dec, err := zstd.NewReader(nil, decOpts...)
	if err != nil {
		return nil, fmt.Errorf("create decoder: %w", err)
	}
	defer dec.Close()

	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress page %d: %w", pageIdx, err)
	}
	if len(decompressed) != PageSize {
		return nil, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}
	return decompressed, nil
}
