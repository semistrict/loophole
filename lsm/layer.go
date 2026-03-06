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

// ImageLayerMeta is the metadata for a segmented image layer on S3.
// Each segment is a self-contained S3 object covering PagesPerSegment
// contiguous page addresses.
type ImageLayerMeta struct {
	Seq       uint64        `json:"seq"`
	PageRange [2]uint64     `json:"pages"`    // [min, max] across all segments
	Segments  []SegmentMeta `json:"segments"` // sorted by SegIdx ascending
}

// SegmentMeta describes one segment within a segmented image layer.
type SegmentMeta struct {
	SegIdx   uint64 `json:"seg_idx"`   // segment index (pageAddr / PagesPerSegment)
	NumPages uint32 `json:"num_pages"` // number of pages with data
	Key      string `json:"key"`       // S3 key for this segment
	Size     int64  `json:"size"`      // compressed size in bytes
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

	encoder := getZstdEncoder()
	defer putZstdEncoder(encoder)

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
	if err := backfillHeader(result, hdr, deltaLayerHeaderSize); err != nil {
		return nil, DeltaLayerMeta{}, err
	}

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

// readCompressedPage fetches a compressed page from a layer (blob or S3 range
// read), verifies CRC, decompresses, and validates size. Shared by both delta
// and image layer findPage methods.
func readCompressedPage(ctx context.Context, data []byte, store loophole.ObjectStore, key string, offset uint64, length uint32, crc uint32, pageAddr uint64) ([]byte, error) {
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
			return nil, fmt.Errorf("range read page %d: %w", pageAddr, err)
		}
		compressed, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, fmt.Errorf("read page %d: %w", pageAddr, err)
		}
	}

	if crc32.ChecksumIEEE(compressed) != crc {
		return nil, fmt.Errorf("CRC mismatch for page %d", pageAddr)
	}

	decoder := getZstdDecoder()
	defer putZstdDecoder(decoder)

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress page %d: %w", pageAddr, err)
	}
	if len(decompressed) != PageSize {
		return nil, fmt.Errorf("decompressed size %d, expected %d", len(decompressed), PageSize)
	}
	return decompressed, nil
}

// backfillHeader encodes a fixed-size struct header and copies it into the
// reserved space at the start of result. Used by all layer build functions.
func backfillHeader(result []byte, hdr any, size int) error {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, hdr); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	copy(result[:size], buf.Bytes())
	return nil
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

	decompressed, err := readCompressedPage(ctx, p.data, p.store, p.key, ie.ValueOffset, ie.ValueLen, ie.CRC32, pageAddr)
	if err != nil {
		return nil, false, err
	}
	return decompressed, true, nil
}

// mergeDeltaLayers merges N parsed delta layers into a single delta layer.
// For each unique pageAddr, only the entry with the highest seq is kept.
// Tombstones are preserved. The merged delta's StartSeq = min of all inputs,
// EndSeq = max of all inputs.
func mergeDeltaLayers(ctx context.Context, layers []*parsedDeltaLayer, metas []DeltaLayerMeta) ([]byte, DeltaLayerMeta, error) {
	if len(layers) == 0 {
		return nil, DeltaLayerMeta{}, fmt.Errorf("no layers to merge")
	}

	// Collect all index entries, keeping track of which layer they came from.
	type sourceEntry struct {
		ie       deltaLayerIndexEntry
		layerIdx int
	}
	var all []sourceEntry
	for li, layer := range layers {
		for _, ie := range layer.index {
			all = append(all, sourceEntry{ie: ie, layerIdx: li})
		}
	}

	// For each unique pageAddr, keep the entry with the highest seq.
	best := make(map[uint64]sourceEntry)
	for _, se := range all {
		existing, ok := best[se.ie.PageAddr]
		if !ok || se.ie.Seq > existing.ie.Seq {
			best[se.ie.PageAddr] = se
		}
	}

	// Sort by pageAddr for deterministic output.
	addrs := make([]uint64, 0, len(best))
	for addr := range best {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })

	if len(addrs) == 0 {
		return nil, DeltaLayerMeta{}, fmt.Errorf("no entries after merge")
	}

	var buf bytes.Buffer
	buf.Write(make([]byte, deltaLayerHeaderSize)) // reserve header

	indexEntries := make([]deltaLayerIndexEntry, 0, len(addrs))
	minPage := addrs[0]
	maxPage := addrs[len(addrs)-1]

	for _, addr := range addrs {
		se := best[addr]
		srcLayer := layers[se.layerIdx]

		ie := deltaLayerIndexEntry{
			PageAddr:    addr,
			Seq:         se.ie.Seq,
			ValueOffset: uint64(buf.Len()),
		}

		if se.ie.ValueLen == 0 {
			// Tombstone: emit with ValueLen=0.
			ie.ValueLen = 0
			ie.CRC32 = 0
		} else {
			// Read compressed data from source layer and copy through.
			// CRC validates the compressed bytes; no need to decompress/recompress.
			var srcCompressed []byte
			if srcLayer.data != nil {
				end := se.ie.ValueOffset + uint64(se.ie.ValueLen)
				if end > uint64(len(srcLayer.data)) {
					return nil, DeltaLayerMeta{}, fmt.Errorf("source value extends beyond data for page %d", addr)
				}
				srcCompressed = srcLayer.data[se.ie.ValueOffset:end]
			} else {
				body, _, err := srcLayer.store.GetRange(ctx, srcLayer.key, int64(se.ie.ValueOffset), int64(se.ie.ValueLen))
				if err != nil {
					return nil, DeltaLayerMeta{}, fmt.Errorf("range read page %d: %w", addr, err)
				}
				srcCompressed, err = io.ReadAll(body)
				_ = body.Close()
				if err != nil {
					return nil, DeltaLayerMeta{}, fmt.Errorf("read page %d: %w", addr, err)
				}
			}

			if crc32.ChecksumIEEE(srcCompressed) != se.ie.CRC32 {
				return nil, DeltaLayerMeta{}, fmt.Errorf("CRC mismatch for page %d seq %d", addr, se.ie.Seq)
			}

			ie.ValueLen = se.ie.ValueLen
			ie.CRC32 = se.ie.CRC32
			buf.Write(srcCompressed)
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

	// Compute seq range from metas.
	startSeq := metas[0].StartSeq
	endSeq := metas[0].EndSeq
	for _, m := range metas[1:] {
		if m.StartSeq < startSeq {
			startSeq = m.StartSeq
		}
		if m.EndSeq > endSeq {
			endSeq = m.EndSeq
		}
	}

	// Write header.
	hdr := deltaLayerHeader{
		Magic:       deltaLayerMagic,
		Version:     deltaLayerVersion,
		StartSeq:    startSeq,
		EndSeq:      endSeq,
		PageRange:   [2]uint64{minPage, maxPage},
		NumEntries:  uint32(len(indexEntries)),
		IndexOffset: indexOffset,
	}

	result := buf.Bytes()
	if err := backfillHeader(result, hdr, deltaLayerHeaderSize); err != nil {
		return nil, DeltaLayerMeta{}, err
	}

	meta := DeltaLayerMeta{
		StartSeq:  startSeq,
		EndSeq:    endSeq,
		PageRange: [2]uint64{minPage, maxPage},
		Key:       fmt.Sprintf("deltas/%016x-%016x", startSeq, endSeq),
		Size:      int64(len(result)),
	}

	return result, meta, nil
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

// imageSegmentKey returns the S3 key for an image segment.
func imageSegmentKey(seq, segIdx uint64) string {
	return fmt.Sprintf("images/%016x/%016x", seq, segIdx)
}

// TotalSize returns the sum of compressed sizes across all segments.
func (im *ImageLayerMeta) TotalSize() int64 {
	var total int64
	for _, seg := range im.Segments {
		total += seg.Size
	}
	return total
}

// LogKey returns a human-readable key for logging (not a real S3 key).
func (im *ImageLayerMeta) LogKey() string {
	return fmt.Sprintf("images/%016x", im.Seq)
}

// FindSegment returns the segment covering the given pageAddr, or nil.
func (im *ImageLayerMeta) FindSegment(pageAddr uint64) *SegmentMeta {
	segIdx := pageAddr / PagesPerSegment
	for i := range im.Segments {
		if im.Segments[i].SegIdx == segIdx {
			return &im.Segments[i]
		}
	}
	return nil
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

// buildImageSegmentBlob creates a single image segment blob from a set of pages.
// All pages must belong to the same segment index.
func buildImageSegmentBlob(seq uint64, pages []imagePageInput) ([]byte, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("no pages")
	}

	encoder := getZstdEncoder()
	defer putZstdEncoder(encoder)

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
			return nil, fmt.Errorf("write index entry: %w", err)
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
	if err := backfillHeader(result, hdr, imageLayerHeaderSize); err != nil {
		return nil, err
	}

	return result, nil
}

// ImageSegmentBlob pairs a segment index with its serialized blob.
type ImageSegmentBlob struct {
	SegIdx uint64
	Data   []byte
}

// buildImageSegments groups pages by segment index and builds one blob per
// segment. Returns the per-segment blobs and the aggregate ImageLayerMeta.
func buildImageSegments(seq uint64, pages []imagePageInput) ([]ImageSegmentBlob, ImageLayerMeta, error) {
	if len(pages) == 0 {
		return nil, ImageLayerMeta{}, fmt.Errorf("no pages")
	}

	// Group pages by segment index.
	groups := make(map[uint64][]imagePageInput)
	for _, p := range pages {
		segIdx := p.PageAddr / PagesPerSegment
		groups[segIdx] = append(groups[segIdx], p)
	}

	// Sort segment indices for deterministic output.
	segIdxs := make([]uint64, 0, len(groups))
	for idx := range groups {
		segIdxs = append(segIdxs, idx)
	}
	sort.Slice(segIdxs, func(i, j int) bool { return segIdxs[i] < segIdxs[j] })

	var segments []SegmentMeta
	var blobs []ImageSegmentBlob
	minPage := pages[0].PageAddr
	maxPage := pages[0].PageAddr

	for _, segIdx := range segIdxs {
		segPages := groups[segIdx]
		// Sort pages within segment by PageAddr.
		sort.Slice(segPages, func(i, j int) bool { return segPages[i].PageAddr < segPages[j].PageAddr })

		blob, err := buildImageSegmentBlob(seq, segPages)
		if err != nil {
			return nil, ImageLayerMeta{}, fmt.Errorf("build segment %d: %w", segIdx, err)
		}

		key := imageSegmentKey(seq, segIdx)
		segments = append(segments, SegmentMeta{
			SegIdx:   segIdx,
			NumPages: uint32(len(segPages)),
			Key:      key,
			Size:     int64(len(blob)),
		})
		blobs = append(blobs, ImageSegmentBlob{SegIdx: segIdx, Data: blob})

		if segPages[0].PageAddr < minPage {
			minPage = segPages[0].PageAddr
		}
		if segPages[len(segPages)-1].PageAddr > maxPage {
			maxPage = segPages[len(segPages)-1].PageAddr
		}
	}

	meta := ImageLayerMeta{
		Seq:       seq,
		PageRange: [2]uint64{minPage, maxPage},
		Segments:  segments,
	}

	return blobs, meta, nil
}

// parsedImageLayer holds a parsed image segment's index and full blob.
// Each segment is small (~4MB) so we always download the full blob.
type parsedImageLayer struct {
	data  []byte
	index []imageLayerIndexEntry
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

// findPage does a binary search for pageAddr in the image segment index.
func (p *parsedImageLayer) findPage(pageAddr uint64) ([]byte, bool, error) {
	i := sort.Search(len(p.index), func(i int) bool {
		return p.index[i].PageAddr >= pageAddr
	})
	if i >= len(p.index) || p.index[i].PageAddr != pageAddr {
		return nil, false, nil
	}

	ie := &p.index[i]

	decompressed, err := readCompressedPage(context.Background(), p.data, nil, "", ie.ValueOffset, ie.ValueLen, ie.CRC32, pageAddr)
	if err != nil {
		return nil, false, err
	}
	return decompressed, true, nil
}
