package storage2

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/semistrict/loophole"
)

// L0 file format:
//   [header][compressed page data...][index entries...]
//
// Each page is independently zstd-compressed with its own CRC32.
// Tombstones are index entries with ValueLen=0.

var l0Magic = [4]byte{'L', 'H', 'L', '0'}

const l0Version = 1

type l0Header struct {
	Magic       [4]byte
	Version     uint16
	StartSeq    uint64
	EndSeq      uint64
	NumEntries  uint32
	IndexOffset uint64
}

const l0HeaderSize = 4 + 2 + 8 + 8 + 4 + 8 // 34 bytes

type l0IndexEntry struct {
	PageIdx     PageIdx
	ValueOffset uint64
	ValueLen    uint32 // 0 = tombstone
	CRC32       uint32
}

const l0IndexEntrySize = 8 + 8 + 4 + 4 // 24 bytes

// buildL0 serializes a frozen memtable's entries into the L0 file format.
// Returns the serialized bytes and the l0Entry metadata (including page index).
func buildL0(mt *memtable, layerID string, entries []sortedEntry, writeLeaseSeq uint64) ([]byte, l0Entry, error) {
	if len(entries) == 0 {
		return nil, l0Entry{}, fmt.Errorf("no entries")
	}

	encoder := getZstdEncoder()
	defer putZstdEncoder(encoder)

	var buf bytes.Buffer

	// Reserve space for header.
	buf.Write(make([]byte, l0HeaderSize))

	// Write compressed page data and build index.
	indexEntries := make([]l0IndexEntry, 0, len(entries))
	var pages []PageIdx
	var tombstones []PageIdx

	for _, e := range entries {
		ie := l0IndexEntry{
			PageIdx:     e.pageIdx,
			ValueOffset: uint64(buf.Len()),
		}

		if e.tombstone {
			ie.ValueLen = 0
			ie.CRC32 = 0
			tombstones = append(tombstones, e.pageIdx)
		} else {
			pageData, err := mt.readData(e.memEntry)
			if err != nil {
				return nil, l0Entry{}, fmt.Errorf("read page %d: %w", e.pageIdx, err)
			}
			compressed := encoder.EncodeAll(pageData, nil)
			ie.ValueLen = uint32(len(compressed))
			ie.CRC32 = crc32.ChecksumIEEE(compressed)
			buf.Write(compressed)
			pages = append(pages, e.pageIdx)
		}

		indexEntries = append(indexEntries, ie)
	}

	// Write index section.
	indexOffset := uint64(buf.Len())
	for _, ie := range indexEntries {
		if err := binary.Write(&buf, binary.LittleEndian, ie); err != nil {
			return nil, l0Entry{}, fmt.Errorf("write index entry: %w", err)
		}
	}

	// Backfill header.
	hdr := l0Header{
		Magic:       l0Magic,
		Version:     l0Version,
		StartSeq:    mt.startSeq,
		EndSeq:      mt.endSeq,
		NumEntries:  uint32(len(entries)),
		IndexOffset: indexOffset,
	}

	result := buf.Bytes()
	var hdrBuf bytes.Buffer
	if err := binary.Write(&hdrBuf, binary.LittleEndian, hdr); err != nil {
		return nil, l0Entry{}, fmt.Errorf("encode header: %w", err)
	}
	copy(result[:l0HeaderSize], hdrBuf.Bytes())

	key := fmt.Sprintf("layers/%s/l0/%016x-%016x-%016x", layerID, writeLeaseSeq, mt.startSeq, mt.endSeq)

	return result, l0Entry{
		Key:        key,
		Pages:      pages,
		Tombstones: tombstones,
		Size:       int64(len(result)),
	}, nil
}

// parsedL0 holds a parsed L0 file's index and optionally the full blob.
type parsedL0 struct {
	data  []byte // full blob; nil in index-only mode
	index []l0IndexEntry
	store loophole.ObjectStore // for range reads
	key   string
}

// parseL0 parses a complete L0 blob into its index entries.
func parseL0(data []byte) (*parsedL0, error) {
	if len(data) < l0HeaderSize {
		return nil, fmt.Errorf("L0 file too small: %d bytes", len(data))
	}

	var hdr l0Header
	if err := binary.Read(bytes.NewReader(data[:l0HeaderSize]), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr.Magic != l0Magic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}

	indexEnd := hdr.IndexOffset + uint64(hdr.NumEntries)*l0IndexEntrySize
	if indexEnd > uint64(len(data)) {
		return nil, fmt.Errorf("index extends beyond data")
	}

	entries := make([]l0IndexEntry, hdr.NumEntries)
	r := bytes.NewReader(data[hdr.IndexOffset:indexEnd])
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedL0{data: data, index: entries}, nil
}

// findPage searches the L0 index for a page, returning decompressed data.
func (p *parsedL0) findPage(ctx context.Context, pageIdx PageIdx) ([]byte, bool, error) {
	// Binary search for the entry.
	i := sort.Search(len(p.index), func(i int) bool {
		return p.index[i].PageIdx >= pageIdx
	})
	if i >= len(p.index) || p.index[i].PageIdx != pageIdx {
		return nil, false, nil
	}

	ie := &p.index[i]

	// Tombstone.
	if ie.ValueLen == 0 {
		return zeroPage[:], true, nil
	}

	data, err := readCompressedPage(ctx, p.data, p.store, p.key, ie.ValueOffset, ie.ValueLen, ie.CRC32, pageIdx)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// readCompressedPage fetches a compressed page from a blob or via S3 GetRange,
// verifies CRC, decompresses, and validates size.
func readCompressedPage(ctx context.Context, data []byte, store loophole.ObjectStore, key string, offset uint64, length uint32, crc uint32, pageIdx PageIdx) ([]byte, error) {
	var compressed []byte
	if data != nil {
		end := offset + uint64(length)
		if end > uint64(len(data)) {
			return nil, fmt.Errorf("value extends beyond data: offset=%d len=%d size=%d", offset, length, len(data))
		}
		compressed = data[offset:end]
	} else {
		body, _, err := store.GetRange(ctx, key, int64(offset), int64(length))
		if err != nil {
			return nil, fmt.Errorf("range read page %d: %w", pageIdx, err)
		}
		compressed, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, fmt.Errorf("read page %d: %w", pageIdx, err)
		}
	}

	if crc32.ChecksumIEEE(compressed) != crc {
		return nil, fmt.Errorf("CRC mismatch for page %d", pageIdx)
	}

	decoder := getZstdDecoder()
	defer putZstdDecoder(decoder)

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress page %d: %w", pageIdx, err)
	}
	if len(decompressed) != PageSize {
		return nil, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}
	return decompressed, nil
}
