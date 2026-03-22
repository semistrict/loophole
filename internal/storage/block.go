package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/semistrict/loophole/internal/objstore"
)

// Block file format (L1 and L2):
//
//   [header][compressed page data...][index entries...]
//
// Each page is independently compressed with plain zstd (no dictionary) and
// has its own CRC32. DictSize in the header is always 0 (reserved for
// format compat). Compressed entries can be copied verbatim between blocks.
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

func encodeHeader(dst []byte, hdr blockHeader) {
	copy(dst[0:4], hdr.Magic[:])
	binary.LittleEndian.PutUint16(dst[4:6], hdr.Version)
	binary.LittleEndian.PutUint64(dst[6:14], uint64(hdr.BlockIdx))
	binary.LittleEndian.PutUint32(dst[14:18], hdr.NumEntries)
	binary.LittleEndian.PutUint32(dst[18:22], hdr.DictSize)
	binary.LittleEndian.PutUint64(dst[22:30], hdr.DictOffset)
	binary.LittleEndian.PutUint64(dst[30:38], hdr.DataOffset)
	binary.LittleEndian.PutUint64(dst[38:46], hdr.IdxOffset)
}

func encodeIndexEntry(dst []byte, ie blockIndexEntry) {
	binary.LittleEndian.PutUint16(dst[0:2], ie.PageOffset)
	binary.LittleEndian.PutUint64(dst[2:10], ie.DataOffset)
	binary.LittleEndian.PutUint32(dst[10:14], ie.DataLen)
	binary.LittleEndian.PutUint32(dst[14:18], ie.CRC32)
}

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

// compressedPage holds the result of compressing a single page.
type compressedPage struct {
	offset     uint16
	compressed []byte
	crc32      uint32
}

// buildBlock serializes pages into the block file format using plain zstd
// compression (no dictionary). Pages must belong to the same block address.
// Compression is parallelized across NumCPU goroutines.
// Returns the serialized blob.
func buildBlock(blockIdx BlockIdx, pages []blockPage, compress bool) ([]byte, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("no pages")
	}

	// Sort by page offset for binary search on read.
	sort.Slice(pages, func(i, j int) bool {
		return pages[i].offset < pages[j].offset
	})

	// Validate page sizes upfront.
	for _, p := range pages {
		if p.data != nil && len(p.data) != PageSize {
			return nil, fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(p.data))
		}
	}

	// Compress all pages in parallel using NumCPU goroutines.
	compressed := make([]compressedPage, len(pages))
	if compress {
		var wg sync.WaitGroup
		workers := runtime.NumCPU()
		if workers > len(pages) {
			workers = len(pages)
		}
		work := make(chan int, len(pages))
		for i := range pages {
			work <- i
		}
		close(work)

		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				enc := getZstdEncoder()
				defer putZstdEncoder(enc)
				for i := range work {
					if pages[i].data == nil {
						compressed[i] = compressedPage{offset: pages[i].offset}
						continue
					}
					c := enc.EncodeAll(pages[i].data, nil)
					compressed[i] = compressedPage{
						offset:     pages[i].offset,
						compressed: c,
						crc32:      crc32.ChecksumIEEE(c),
					}
				}
			}()
		}
		wg.Wait()
	} else {
		for i, p := range pages {
			if p.data == nil {
				compressed[i] = compressedPage{offset: p.offset}
				continue
			}
			compressed[i] = compressedPage{
				offset:     p.offset,
				compressed: p.data,
				crc32:      crc32.ChecksumIEEE(p.data),
			}
		}
	}

	// Compute exact output size and assemble directly into a right-sized slice.
	dataSize := 0
	for _, cp := range compressed {
		dataSize += len(cp.compressed)
	}
	totalSize := blockHeaderSize + dataSize + len(compressed)*blockIndexEntrySize
	result := make([]byte, totalSize)
	pos := blockHeaderSize // skip header, backfill later

	dictOffset := uint64(pos)
	dataOffset := uint64(pos)

	indexEntries := make([]blockIndexEntry, len(compressed))
	for i, cp := range compressed {
		indexEntries[i] = blockIndexEntry{
			PageOffset: cp.offset,
			DataOffset: uint64(pos),
			DataLen:    uint32(len(cp.compressed)),
			CRC32:      cp.crc32,
		}
		pos += copy(result[pos:], cp.compressed)
	}

	// Write index.
	idxOffset := uint64(pos)
	for _, ie := range indexEntries {
		encodeIndexEntry(result[pos:pos+blockIndexEntrySize], ie)
		pos += blockIndexEntrySize
	}

	// Backfill header.
	encodeHeader(result[:blockHeaderSize], blockHeader{
		Magic:      blockMagic,
		Version:    blockVersion,
		BlockIdx:   blockIdx,
		NumEntries: uint32(len(indexEntries)),
		DictSize:   0,
		DictOffset: dictOffset,
		DataOffset: dataOffset,
		IdxOffset:  idxOffset,
	})

	return result, nil
}

// parsedBlock holds a parsed block file's header, dictionary, and index.
type parsedBlock struct {
	data       []byte // full blob; nil for range-read mode
	header     blockHeader
	dictBytes  []byte // shared dictionary; may be nil
	index      []blockIndexEntry
	store      objstore.ObjectStore
	key        string
	compressed bool // true if pages are zstd-compressed
}

// parseBlock parses a complete block blob. If compressed is true, pages
// are zstd-compressed and will be decompressed on read.
func parseBlock(data []byte, compressed bool) (*parsedBlock, error) {
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
		data:       data,
		header:     hdr,
		dictBytes:  dictBytes,
		index:      entries,
		compressed: compressed,
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
	if ie.DataLen == 0 {
		return zeroPage[:], true, nil
	}
	data, err := pb.decompressPage(ctx, ie, pageIdx)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
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

	if !pb.compressed {
		if len(compressed) != PageSize {
			return nil, fmt.Errorf("uncompressed page %d size %d, expected %d", pageIdx, len(compressed), PageSize)
		}
		page := make([]byte, PageSize)
		copy(page, compressed)
		return page, nil
	}

	dec := getZstdDecoder()
	defer putZstdDecoder(dec)

	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress page %d: %w", pageIdx, err)
	}
	if len(decompressed) != PageSize {
		return nil, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}
	return decompressed, nil
}

// compressedBlockPage is a pre-compressed page entry that can be copied
// verbatim into the output block.
type compressedBlockPage struct {
	offset     uint16
	compressed []byte
	crc32      uint32
}

// compressedEntriesExcluding returns compressed entries for pages NOT in
// the exclude set. Requires pb.data to be non-nil (full blob mode).
func (pb *parsedBlock) compressedEntriesExcluding(exclude map[uint16]struct{}) []compressedBlockPage {
	entries := make([]compressedBlockPage, 0, len(pb.index))
	for _, ie := range pb.index {
		if _, skip := exclude[ie.PageOffset]; skip {
			continue
		}
		end := ie.DataOffset + uint64(ie.DataLen)
		entries = append(entries, compressedBlockPage{
			offset:     ie.PageOffset,
			compressed: pb.data[ie.DataOffset:end],
			crc32:      ie.CRC32,
		})
	}
	return entries
}

// compressedEntriesExcluding2 returns compressed entries for pages NOT in
// either exclude set. Avoids allocating a union map.
func (pb *parsedBlock) compressedEntriesExcluding2(excludeA, excludeB map[uint16]struct{}) []compressedBlockPage {
	entries := make([]compressedBlockPage, 0, len(pb.index))
	for _, ie := range pb.index {
		if _, skip := excludeA[ie.PageOffset]; skip {
			continue
		}
		if _, skip := excludeB[ie.PageOffset]; skip {
			continue
		}
		end := ie.DataOffset + uint64(ie.DataLen)
		entries = append(entries, compressedBlockPage{
			offset:     ie.PageOffset,
			compressed: pb.data[ie.DataOffset:end],
			crc32:      ie.CRC32,
		})
	}
	return entries
}

// patchBlock builds a block from a mix of pre-compressed entries (copied
// verbatim) and new uncompressed pages (compressed during build). Entries
// are sorted by page offset for binary search on read.
func patchBlock(blockIdx BlockIdx, existing []compressedBlockPage, newPages []blockPage, compress bool) ([]byte, error) {
	if len(existing) == 0 && len(newPages) == 0 {
		return nil, fmt.Errorf("no pages")
	}

	// Validate page sizes upfront.
	for _, p := range newPages {
		if p.data != nil && len(p.data) != PageSize {
			return nil, fmt.Errorf("page data must be %d bytes, got %d", PageSize, len(p.data))
		}
	}

	// Compress new pages in parallel using NumCPU goroutines.
	newCompressed := make([]compressedPage, len(newPages))
	if len(newPages) > 0 {
		if compress {
			var wg sync.WaitGroup
			workers := runtime.NumCPU()
			if workers > len(newPages) {
				workers = len(newPages)
			}
			work := make(chan int, len(newPages))
			for i := range newPages {
				work <- i
			}
			close(work)

			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					enc := getZstdEncoder()
					defer putZstdEncoder(enc)
					for i := range work {
						if newPages[i].data == nil {
							newCompressed[i] = compressedPage{offset: newPages[i].offset}
							continue
						}
						c := enc.EncodeAll(newPages[i].data, nil)
						newCompressed[i] = compressedPage{
							offset:     newPages[i].offset,
							compressed: c,
							crc32:      crc32.ChecksumIEEE(c),
						}
					}
				}()
			}
			wg.Wait()
		} else {
			for i, p := range newPages {
				if p.data == nil {
					newCompressed[i] = compressedPage{offset: p.offset}
					continue
				}
				newCompressed[i] = compressedPage{
					offset:     p.offset,
					compressed: p.data,
					crc32:      crc32.ChecksumIEEE(p.data),
				}
			}
		}
	}

	// Canonicalize by page offset before serializing so rebuilt blocks never
	// contain duplicate index entries. New pages always win over copied
	// entries from older blocks.
	existingByOffset := make(map[uint16]compressedBlockPage, len(existing))
	for _, e := range existing {
		existingByOffset[e.offset] = e
	}

	newByOffset := make(map[uint16]compressedPage, len(newCompressed))
	for _, cp := range newCompressed {
		newByOffset[cp.offset] = cp
		delete(existingByOffset, cp.offset)
	}

	offsets := make([]uint16, 0, len(existingByOffset)+len(newByOffset))
	for offset := range existingByOffset {
		offsets = append(offsets, offset)
	}
	for offset := range newByOffset {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	// Compute exact output size and assemble directly into a right-sized slice.
	dataSize := 0
	for _, offset := range offsets {
		if cp, ok := newByOffset[offset]; ok {
			dataSize += len(cp.compressed)
		} else {
			dataSize += len(existingByOffset[offset].compressed)
		}
	}
	totalSize := blockHeaderSize + dataSize + len(offsets)*blockIndexEntrySize
	result := make([]byte, totalSize)
	pos := blockHeaderSize // skip header, backfill later

	dictOffset := uint64(pos)
	dataOffset := uint64(pos)

	indexEntries := make([]blockIndexEntry, 0, len(offsets))
	for _, offset := range offsets {
		if cp, ok := newByOffset[offset]; ok {
			ie := blockIndexEntry{
				PageOffset: cp.offset,
				DataOffset: uint64(pos),
				DataLen:    uint32(len(cp.compressed)),
				CRC32:      cp.crc32,
			}
			pos += copy(result[pos:], cp.compressed)
			indexEntries = append(indexEntries, ie)
			continue
		}
		e := existingByOffset[offset]
		ie := blockIndexEntry{
			PageOffset: e.offset,
			DataOffset: uint64(pos),
			DataLen:    uint32(len(e.compressed)),
			CRC32:      e.crc32,
		}
		pos += copy(result[pos:], e.compressed)
		indexEntries = append(indexEntries, ie)
	}

	// Write index.
	idxOffset := uint64(pos)
	for _, ie := range indexEntries {
		encodeIndexEntry(result[pos:pos+blockIndexEntrySize], ie)
		pos += blockIndexEntrySize
	}

	// Backfill header.
	encodeHeader(result[:blockHeaderSize], blockHeader{
		Magic:      blockMagic,
		Version:    blockVersion,
		BlockIdx:   blockIdx,
		NumEntries: uint32(len(indexEntries)),
		DictSize:   0,
		DictOffset: dictOffset,
		DataOffset: dataOffset,
		IdxOffset:  idxOffset,
	})

	return result, nil
}
