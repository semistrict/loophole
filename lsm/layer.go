package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/semistrict/loophole"
)

// LayerMap tracks all layers for a timeline.
type LayerMap struct {
	// Sorted by StartSeq ascending.
	Deltas []DeltaLayerMeta
	Images []ImageLayerMeta
}

// DeltaLayerMeta is the metadata for a delta layer on S3.
type DeltaLayerMeta struct {
	StartSeq  uint64    `json:"start_seq"`
	EndSeq    uint64    `json:"end_seq"`
	PageRange [2]uint64 `json:"pages"` // [min, max] pageAddr
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
}

// ImageLayerMeta is the metadata for an image layer on S3.
type ImageLayerMeta struct {
	Seq       uint64    `json:"seq"`
	PageRange [2]uint64 `json:"pages"` // [min, max] pageAddr
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
}

// --- Delta layer S3 format ---

var deltaLayerMagic = [4]byte{'L', 'D', 'L', 'T'}

const deltaLayerVersion = 1

// deltaLayerHeader is the fixed-size header at the start of a delta layer.
type deltaLayerHeader struct {
	Magic       [4]byte
	Version     uint16
	StartSeq    uint64
	EndSeq      uint64
	PageRange   [2]uint64
	NumEntries  uint32
	IndexOffset uint64
}

const deltaLayerHeaderSize = 4 + 2 + 8 + 8 + 16 + 4 + 8 // 50 bytes

// deltaLayerIndexEntry is one entry in the delta layer's index section.
type deltaLayerIndexEntry struct {
	PageAddr    uint64
	Seq         uint64
	ValueOffset uint64
	ValueLen    uint32 // 0 = tombstone
	CRC32       uint32 // CRC of compressed page data
}

const deltaLayerIndexEntrySize = 8 + 8 + 8 + 4 + 4 // 32 bytes

// buildDeltaLayer serializes a frozen memLayer's entries into the delta layer
// S3 format. Returns the serialized bytes and the metadata.
func buildDeltaLayer(ml *MemLayer, entries []sortedEntry) ([]byte, DeltaLayerMeta, error) {
	if len(entries) == 0 {
		return nil, DeltaLayerMeta{}, fmt.Errorf("no entries")
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, DeltaLayerMeta{}, err
	}
	defer func() { _ = encoder.Close() }()

	var buf bytes.Buffer

	// Reserve space for header (written last with correct offsets).
	headerBytes := make([]byte, deltaLayerHeaderSize)
	buf.Write(headerBytes)

	// Write compressed page values and build index entries.
	indexEntries := make([]deltaLayerIndexEntry, 0, len(entries))

	minPage := entries[0].pageAddr
	maxPage := entries[0].pageAddr

	for _, e := range entries {
		if e.pageAddr < minPage {
			minPage = e.pageAddr
		}
		if e.pageAddr > maxPage {
			maxPage = e.pageAddr
		}

		ie := deltaLayerIndexEntry{
			PageAddr:    e.pageAddr,
			Seq:         e.seq,
			ValueOffset: uint64(buf.Len()),
		}

		if e.tombstone {
			ie.ValueLen = 0
			ie.CRC32 = 0
		} else {
			pageData, err := ml.readData(e.memEntry)
			if err != nil {
				return nil, DeltaLayerMeta{}, fmt.Errorf("read page %d: %w", e.pageAddr, err)
			}
			compressed := encoder.EncodeAll(pageData, nil)
			ie.ValueLen = uint32(len(compressed))
			ie.CRC32 = crc32.ChecksumIEEE(compressed)
			buf.Write(compressed)
		}

		indexEntries = append(indexEntries, ie)
	}

	// Write index section.
	indexOffset := uint64(buf.Len())
	for _, ie := range indexEntries {
		if err := binary.Write(&buf, binary.LittleEndian, ie); err != nil {
			return nil, DeltaLayerMeta{}, fmt.Errorf("write index entry: %w", err)
		}
	}

	// Write header.
	hdr := deltaLayerHeader{
		Magic:       deltaLayerMagic,
		Version:     deltaLayerVersion,
		StartSeq:    ml.startSeq,
		EndSeq:      ml.endSeq,
		PageRange:   [2]uint64{minPage, maxPage},
		NumEntries:  uint32(len(entries)),
		IndexOffset: indexOffset,
	}

	result := buf.Bytes()
	var hdrBuf bytes.Buffer
	if err := binary.Write(&hdrBuf, binary.LittleEndian, hdr); err != nil {
		return nil, DeltaLayerMeta{}, fmt.Errorf("encode header: %w", err)
	}
	copy(result[:deltaLayerHeaderSize], hdrBuf.Bytes())

	meta := DeltaLayerMeta{
		StartSeq:  hdr.StartSeq,
		EndSeq:    hdr.EndSeq,
		PageRange: hdr.PageRange,
		Key:       fmt.Sprintf("deltas/%016x-%016x", hdr.StartSeq, hdr.EndSeq),
		Size:      int64(len(result)),
	}

	return result, meta, nil
}

// parsedDeltaLayer holds a delta layer's parsed index and optionally the full
// blob. In index-only mode (data == nil), findPage uses GetRange to fetch
// individual compressed pages on demand.
type parsedDeltaLayer struct {
	data  []byte // full blob; nil in index-only mode
	index []deltaLayerIndexEntry
	store loophole.ObjectStore // for range reads; nil when data is set
	key   string               // S3 key for range reads
}

// parseDeltaLayer parses a complete delta layer blob into its index entries.
func parseDeltaLayer(data []byte) (*parsedDeltaLayer, error) {
	if len(data) < deltaLayerHeaderSize {
		return nil, fmt.Errorf("delta layer too small: %d bytes", len(data))
	}

	var hdr deltaLayerHeader
	if err := binary.Read(bytes.NewReader(data[:deltaLayerHeaderSize]), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr.Magic != deltaLayerMagic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}
	if hdr.Version != deltaLayerVersion {
		return nil, fmt.Errorf("unsupported version: %d", hdr.Version)
	}

	indexEnd := hdr.IndexOffset + uint64(hdr.NumEntries)*deltaLayerIndexEntrySize
	if indexEnd > uint64(len(data)) {
		return nil, fmt.Errorf("index extends beyond data: offset=%d entries=%d size=%d", hdr.IndexOffset, hdr.NumEntries, len(data))
	}

	entries := make([]deltaLayerIndexEntry, hdr.NumEntries)
	r := bytes.NewReader(data[hdr.IndexOffset:indexEnd])
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedDeltaLayer{data: data, index: entries}, nil
}

// findPage searches the index for a page, returning the decompressed data.
// Index entries are sorted by (pageAddr, seq). We want the entry with matching
// pageAddr and the highest seq < beforeSeq.
//
// In index-only mode (data == nil), compressed page data is fetched via
// GetRange on demand. In full-blob mode, data is read from the in-memory blob.
func (p *parsedDeltaLayer) findPage(ctx context.Context, pageAddr, beforeSeq uint64) ([]byte, bool, error) {
	// Binary search for the first entry with this pageAddr.
	i := sort.Search(len(p.index), func(i int) bool {
		return p.index[i].PageAddr >= pageAddr
	})

	// Scan entries with matching pageAddr, pick the highest seq < beforeSeq.
	bestIdx := -1
	for j := i; j < len(p.index) && p.index[j].PageAddr == pageAddr; j++ {
		if p.index[j].Seq < beforeSeq {
			bestIdx = j
		}
	}
	if bestIdx < 0 {
		return nil, false, nil
	}

	ie := &p.index[bestIdx]

	// Tombstone.
	if ie.ValueLen == 0 {
		return zeroPage[:], true, nil
	}

	var compressed []byte
	if p.data != nil {
		// Full-blob mode: read from in-memory data.
		end := ie.ValueOffset + uint64(ie.ValueLen)
		if end > uint64(len(p.data)) {
			return nil, false, fmt.Errorf("value extends beyond data: offset=%d len=%d size=%d", ie.ValueOffset, ie.ValueLen, len(p.data))
		}
		compressed = p.data[ie.ValueOffset:end]
	} else {
		// Index-only mode: fetch compressed page via GetRange.
		body, _, err := p.store.GetRange(ctx, p.key, int64(ie.ValueOffset), int64(ie.ValueLen))
		if err != nil {
			return nil, false, fmt.Errorf("range read page %d: %w", pageAddr, err)
		}
		compressed, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, false, fmt.Errorf("read page %d: %w", pageAddr, err)
		}
	}

	if crc32.ChecksumIEEE(compressed) != ie.CRC32 {
		return nil, false, fmt.Errorf("CRC mismatch for page %d seq %d", pageAddr, ie.Seq)
	}

	decoder := getZstdDecoder()
	defer putZstdDecoder(decoder)

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, false, fmt.Errorf("decompress page %d: %w", pageAddr, err)
	}
	if len(decompressed) != PageSize {
		return nil, false, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}

	return decompressed, true, nil
}

// parseDeltaIndex downloads only the header and index of a delta layer via
// two range reads, returning a parsedDeltaLayer in index-only mode (data=nil).
// Page data is fetched on demand by findPage via GetRange.
func parseDeltaIndex(ctx context.Context, store loophole.ObjectStore, key string) (*parsedDeltaLayer, error) {
	// 1. Read header.
	body, _, err := store.GetRange(ctx, key, 0, deltaLayerHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("read delta header: %w", err)
	}
	headerData, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, fmt.Errorf("read delta header bytes: %w", err)
	}

	var hdr deltaLayerHeader
	if err := binary.Read(bytes.NewReader(headerData), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("parse delta header: %w", err)
	}
	if hdr.Magic != deltaLayerMagic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}
	if hdr.Version != deltaLayerVersion {
		return nil, fmt.Errorf("unsupported version: %d", hdr.Version)
	}

	// 2. Read index section.
	indexSize := int64(hdr.NumEntries) * deltaLayerIndexEntrySize
	body, _, err = store.GetRange(ctx, key, int64(hdr.IndexOffset), indexSize)
	if err != nil {
		return nil, fmt.Errorf("read delta index: %w", err)
	}
	indexData, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, fmt.Errorf("read delta index bytes: %w", err)
	}

	entries := make([]deltaLayerIndexEntry, hdr.NumEntries)
	r := bytes.NewReader(indexData)
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedDeltaLayer{
		index: entries,
		store: store,
		key:   key,
	}, nil
}

// parseDeltaKey parses a delta layer key like "deltas/0000000000000000-0000000000000005"
// into a DeltaLayerMeta. PageRange is left as zero — it will be filled when
// the layer is actually downloaded and parsed.
func parseDeltaKey(key string, size int64) (DeltaLayerMeta, error) {
	base := path.Base(key)
	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 {
		return DeltaLayerMeta{}, fmt.Errorf("bad delta key: %s", key)
	}
	startSeq, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return DeltaLayerMeta{}, fmt.Errorf("parse start seq %q: %w", parts[0], err)
	}
	endSeq, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return DeltaLayerMeta{}, fmt.Errorf("parse end seq %q: %w", parts[1], err)
	}
	return DeltaLayerMeta{
		StartSeq:  startSeq,
		EndSeq:    endSeq,
		Key:       key,
		Size:      size,
		PageRange: [2]uint64{0, math.MaxUint64}, // unknown until downloaded
	}, nil
}

// parseImageKey parses an image layer key like "images/0000000000000005"
// into an ImageLayerMeta. PageRange is set to [0, MaxUint64] since it's
// unknown until the layer is downloaded and parsed.
func parseImageKey(key string, size int64) (ImageLayerMeta, error) {
	base := path.Base(key)
	seq, err := strconv.ParseUint(base, 16, 64)
	if err != nil {
		return ImageLayerMeta{}, fmt.Errorf("parse image seq %q: %w", base, err)
	}
	return ImageLayerMeta{
		Seq:       seq,
		Key:       key,
		Size:      size,
		PageRange: [2]uint64{0, math.MaxUint64},
	}, nil
}

// --- Image layer S3 format ---

var imageLayerMagic = [4]byte{'L', 'I', 'M', 'G'}

const imageLayerVersion = 1

// imageLayerHeader is the fixed-size header at the start of an image layer.
type imageLayerHeader struct {
	Magic       [4]byte
	Version     uint16
	Seq         uint64
	PageRange   [2]uint64
	NumPages    uint32
	IndexOffset uint64
}

const imageLayerHeaderSize = 4 + 2 + 8 + 16 + 4 + 8 // 42 bytes

// imageLayerIndexEntry is one entry in the image layer's index section.
type imageLayerIndexEntry struct {
	PageAddr    uint64
	ValueOffset uint64
	ValueLen    uint32
	CRC32       uint32
}

const imageLayerIndexEntrySize = 8 + 8 + 4 + 4 // 24 bytes

// imagePageInput is a page to include in an image layer.
type imagePageInput struct {
	PageAddr uint64
	Data     []byte // must be PageSize bytes
}

// buildImageLayer creates an image layer from a sorted set of pages.
func buildImageLayer(seq uint64, pages []imagePageInput) ([]byte, ImageLayerMeta, error) {
	if len(pages) == 0 {
		return nil, ImageLayerMeta{}, fmt.Errorf("no pages")
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, ImageLayerMeta{}, err
	}
	defer func() { _ = encoder.Close() }()

	var buf bytes.Buffer
	buf.Write(make([]byte, imageLayerHeaderSize)) // reserve header

	indexEntries := make([]imageLayerIndexEntry, 0, len(pages))
	minPage := pages[0].PageAddr
	maxPage := pages[0].PageAddr

	for _, p := range pages {
		if p.PageAddr < minPage {
			minPage = p.PageAddr
		}
		if p.PageAddr > maxPage {
			maxPage = p.PageAddr
		}

		compressed := encoder.EncodeAll(p.Data, nil)
		ie := imageLayerIndexEntry{
			PageAddr:    p.PageAddr,
			ValueOffset: uint64(buf.Len()),
			ValueLen:    uint32(len(compressed)),
			CRC32:       crc32.ChecksumIEEE(compressed),
		}
		buf.Write(compressed)
		indexEntries = append(indexEntries, ie)
	}

	indexOffset := uint64(buf.Len())
	for _, ie := range indexEntries {
		if err := binary.Write(&buf, binary.LittleEndian, ie); err != nil {
			return nil, ImageLayerMeta{}, fmt.Errorf("write index entry: %w", err)
		}
	}

	hdr := imageLayerHeader{
		Magic:       imageLayerMagic,
		Version:     imageLayerVersion,
		Seq:         seq,
		PageRange:   [2]uint64{minPage, maxPage},
		NumPages:    uint32(len(pages)),
		IndexOffset: indexOffset,
	}

	result := buf.Bytes()
	var hdrBuf bytes.Buffer
	if err := binary.Write(&hdrBuf, binary.LittleEndian, hdr); err != nil {
		return nil, ImageLayerMeta{}, fmt.Errorf("encode header: %w", err)
	}
	copy(result[:imageLayerHeaderSize], hdrBuf.Bytes())

	meta := ImageLayerMeta{
		Seq:       seq,
		PageRange: [2]uint64{minPage, maxPage},
		Key:       fmt.Sprintf("images/%016x", seq),
		Size:      int64(len(result)),
	}

	return result, meta, nil
}

// parsedImageLayer holds an image layer's parsed index and optionally the full
// blob. In index-only mode (data == nil), findPage uses GetRange to fetch
// individual compressed pages on demand.
type parsedImageLayer struct {
	data  []byte // full blob; nil in index-only mode
	index []imageLayerIndexEntry
	store loophole.ObjectStore // for range reads; nil when data is set
	key   string               // S3 key for range reads
}

func parseImageLayer(data []byte) (*parsedImageLayer, error) {
	if len(data) < imageLayerHeaderSize {
		return nil, fmt.Errorf("image layer too small: %d bytes", len(data))
	}

	var hdr imageLayerHeader
	if err := binary.Read(bytes.NewReader(data[:imageLayerHeaderSize]), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr.Magic != imageLayerMagic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}

	indexEnd := hdr.IndexOffset + uint64(hdr.NumPages)*imageLayerIndexEntrySize
	if indexEnd > uint64(len(data)) {
		return nil, fmt.Errorf("index extends beyond data")
	}

	entries := make([]imageLayerIndexEntry, hdr.NumPages)
	r := bytes.NewReader(data[hdr.IndexOffset:indexEnd])
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedImageLayer{data: data, index: entries}, nil
}

// findPage does a binary search for pageAddr in the image layer index.
// In index-only mode (data == nil), compressed page data is fetched via
// GetRange on demand.
func (p *parsedImageLayer) findPage(ctx context.Context, pageAddr uint64) ([]byte, bool, error) {
	i := sort.Search(len(p.index), func(i int) bool {
		return p.index[i].PageAddr >= pageAddr
	})
	if i >= len(p.index) || p.index[i].PageAddr != pageAddr {
		return nil, false, nil
	}

	ie := &p.index[i]

	var compressed []byte
	if p.data != nil {
		end := ie.ValueOffset + uint64(ie.ValueLen)
		if end > uint64(len(p.data)) {
			return nil, false, fmt.Errorf("value extends beyond data")
		}
		compressed = p.data[ie.ValueOffset:end]
	} else {
		body, _, err := p.store.GetRange(ctx, p.key, int64(ie.ValueOffset), int64(ie.ValueLen))
		if err != nil {
			return nil, false, fmt.Errorf("range read page %d: %w", pageAddr, err)
		}
		compressed, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, false, fmt.Errorf("read page %d: %w", pageAddr, err)
		}
	}

	if crc32.ChecksumIEEE(compressed) != ie.CRC32 {
		return nil, false, fmt.Errorf("CRC mismatch for page %d", pageAddr)
	}

	decoder := getZstdDecoder()
	defer putZstdDecoder(decoder)

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, false, fmt.Errorf("decompress page %d: %w", pageAddr, err)
	}
	if len(decompressed) != PageSize {
		return nil, false, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}

	return decompressed, true, nil
}

// parseImageIndex downloads only the header and index of an image layer via
// two range reads, returning a parsedImageLayer in index-only mode (data=nil).
func parseImageIndex(ctx context.Context, store loophole.ObjectStore, key string) (*parsedImageLayer, error) {
	// 1. Read header.
	body, _, err := store.GetRange(ctx, key, 0, imageLayerHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("read image header: %w", err)
	}
	headerData, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, fmt.Errorf("read image header bytes: %w", err)
	}

	var hdr imageLayerHeader
	if err := binary.Read(bytes.NewReader(headerData), binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("parse image header: %w", err)
	}
	if hdr.Magic != imageLayerMagic {
		return nil, fmt.Errorf("bad magic: %x", hdr.Magic)
	}

	// 2. Read index section.
	indexSize := int64(hdr.NumPages) * imageLayerIndexEntrySize
	body, _, err = store.GetRange(ctx, key, int64(hdr.IndexOffset), indexSize)
	if err != nil {
		return nil, fmt.Errorf("read image index: %w", err)
	}
	indexData, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, fmt.Errorf("read image index bytes: %w", err)
	}

	entries := make([]imageLayerIndexEntry, hdr.NumPages)
	r := bytes.NewReader(indexData)
	for i := range entries {
		if err := binary.Read(r, binary.LittleEndian, &entries[i]); err != nil {
			return nil, fmt.Errorf("read index entry %d: %w", i, err)
		}
	}

	return &parsedImageLayer{
		index: entries,
		store: store,
		key:   key,
	}, nil
}
