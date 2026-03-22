package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/semistrict/loophole/internal/blob"
)

// --- Block parse + page lookup benchmarks ---

// buildTestBlock creates a block with n random pages for benchmarking.
func buildTestBlock(b *testing.B, rng *rand.Rand, numPages int, compressed bool) []byte {
	b.Helper()
	pages := make([]blockPage, numPages)
	for i := range pages {
		data := make([]byte, PageSize)
		randomPage(rng, data)
		pages[i] = blockPage{
			offset: uint16(i * (BlockPages / numPages)), // spread pages across block
			data:   data,
		}
	}
	blob, err := buildBlock(0, pages, compressed)
	if err != nil {
		b.Fatal(err)
	}
	return blob
}

func BenchmarkParseBlock(b *testing.B) {
	rng := rand.New(rand.NewPCG(42, 0))
	for _, numPages := range []int{1, 16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("pages=%d", numPages), func(b *testing.B) {
			blob := buildTestBlock(b, rng, numPages, true)
			b.SetBytes(int64(len(blob)))
			b.ResetTimer()
			for range b.N {
				_, err := parseBlock(blob, true)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFindPage(b *testing.B) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(42, 0))

	for _, numPages := range []int{1, 16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("pages=%d", numPages), func(b *testing.B) {
			blob := buildTestBlock(b, rng, numPages, true)
			pb, err := parseBlock(blob, true)
			if err != nil {
				b.Fatal(err)
			}
			// Pick a page that exists in the block.
			targetPage := PageIdx(pb.index[numPages/2].PageOffset)

			b.ResetTimer()
			for range b.N {
				data, found, err := pb.findPage(ctx, targetPage)
				if err != nil {
					b.Fatal(err)
				}
				if !found || len(data) != PageSize {
					b.Fatal("page not found")
				}
			}
		})
	}
}

func BenchmarkFindPageMiss(b *testing.B) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(42, 0))

	// Sparse block: only 16 pages out of 1024 possible offsets.
	blob := buildTestBlock(b, rng, 16, true)
	pb, err := parseBlock(blob, true)
	if err != nil {
		b.Fatal(err)
	}
	// Target a page offset that doesn't exist.
	missPage := PageIdx(BlockPages - 1)
	b.ResetTimer()
	for range b.N {
		_, found, err := pb.findPage(ctx, missPage)
		if err != nil {
			b.Fatal(err)
		}
		if found {
			b.Fatal("should not find page")
		}
	}
}

// --- blockRangeMap.Find benchmarks ---

func BenchmarkBlockRangeMapFind(b *testing.B) {
	for _, numRanges := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("ranges=%d", numRanges), func(b *testing.B) {
			ranges := make([]blockRange, numRanges)
			for i := range ranges {
				start := BlockIdx(i * 2)
				ranges[i] = blockRange{
					Start:         start,
					End:           start + 1,
					Layer:         fmt.Sprintf("layer-%d", i),
					WriteLeaseSeq: uint64(i),
				}
			}
			m := newBlockRangeMap(ranges)

			rng := rand.New(rand.NewPCG(99, 0))
			maxBlock := uint64(numRanges * 2)

			b.ResetTimer()
			for range b.N {
				block := BlockIdx(rng.Uint64() % maxBlock)
				m.Find(block)
			}
		})
	}
}

// --- boundedCache benchmarks ---

func BenchmarkBlockCacheHit(b *testing.B) {
	var cache boundedCache[*parsedBlock]
	cache.init(256)

	// Pre-populate with 256 entries.
	pb := &parsedBlock{}
	for i := range 256 {
		cache.put(fmt.Sprintf("key-%d", i), pb)
	}

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("key-%d", i%256)
		cache.get(key)
	}
}

func BenchmarkBlockCacheMiss(b *testing.B) {
	var cache boundedCache[*parsedBlock]
	cache.init(256)

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("miss-%d", i)
		cache.get(key)
	}
}

// --- End-to-end read from flushed L1 at different block densities ---

func BenchmarkReadL1Sparse(b *testing.B) {
	// Write only a few pages per block, then read them back from L1.
	store := blob.New(blob.NewMemDriver())
	ctx := context.Background()

	m := newBenchManager(b, store, Config{FlushThreshold: 64 * PageSize})
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: BlockSize * 4})
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(42, 0))
	page := make([]byte, PageSize)

	// Write 4 pages spread across 4 blocks — sparse L1.
	offsets := []uint64{0, BlockSize, 2 * BlockSize, 3 * BlockSize}
	for _, off := range offsets {
		randomPage(rng, page)
		if err := v.Write(page, off); err != nil {
			b.Fatal(err)
		}
	}
	if err := v.Flush(); err != nil {
		b.Fatal(err)
	}

	// Warm the block cache.
	buf := make([]byte, PageSize)
	for _, off := range offsets {
		if _, err := v.Read(ctx, buf, off); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(PageSize)
	b.ResetTimer()
	for i := range b.N {
		off := offsets[i%len(offsets)]
		if _, err := v.Read(ctx, buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadL1Dense(b *testing.B) {
	// Write all pages in a block, flush to L1, then read back.
	store := blob.New(blob.NewMemDriver())
	ctx := context.Background()

	m := newBenchManager(b, store, Config{FlushThreshold: BlockSize + PageSize})
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: BlockSize})
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(42, 0))
	page := make([]byte, PageSize)
	for pg := uint64(0); pg < BlockPages; pg++ {
		randomPage(rng, page)
		if err := v.Write(page, pg*PageSize); err != nil {
			b.Fatal(err)
		}
	}
	if err := v.Flush(); err != nil {
		b.Fatal(err)
	}

	// Warm the block cache.
	buf := make([]byte, PageSize)
	for pg := uint64(0); pg < BlockPages; pg++ {
		if _, err := v.Read(ctx, buf, pg*PageSize); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(PageSize)
	b.ResetTimer()
	readRng := rand.New(rand.NewPCG(99, 0))
	for range b.N {
		off := (readRng.Uint64() % BlockPages) * PageSize
		if _, err := v.Read(ctx, buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Multi-layer read: reads that must fall through L1 miss to L2 ---

func BenchmarkReadL1MissFallsToL2(b *testing.B) {
	store := blob.New(blob.NewMemDriver())
	ctx := context.Background()

	m := newBenchManager(b, store, Config{FlushThreshold: BlockSize + PageSize})
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: BlockSize})
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(42, 0))
	page := make([]byte, PageSize)

	// Fill the full block and flush twice so pages land in L2.
	for pass := range 2 {
		for pg := uint64(0); pg < BlockPages; pg++ {
			randomPage(rng, page)
			if err := v.Write(page, pg*PageSize); err != nil {
				b.Fatal(err)
			}
		}
		if err := v.Flush(); err != nil {
			b.Fatalf("flush %d: %v", pass, err)
		}
	}

	// Now write 1 page to create a sparse L1 that covers only page 0.
	randomPage(rng, page)
	if err := v.Write(page, 0); err != nil {
		b.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		b.Fatal(err)
	}

	// Reading page 500 should miss L1 (only has page 0) and fall through to L2.
	targetOff := uint64(500) * PageSize
	buf := make([]byte, PageSize)

	// Warm.
	if _, err := v.Read(ctx, buf, targetOff); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(PageSize)
	b.ResetTimer()
	for range b.N {
		if _, err := v.Read(ctx, buf, targetOff); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Clone read: read from a snapshot (cloned) volume ---

func BenchmarkReadAfterClone(b *testing.B) {
	store := blob.New(blob.NewMemDriver())
	ctx := context.Background()

	cfg := Config{FlushThreshold: BlockSize + PageSize}
	m := newBenchManager(b, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: BlockSize})
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(42, 0))
	page := make([]byte, PageSize)
	for pg := uint64(0); pg < BlockPages; pg++ {
		randomPage(rng, page)
		if err := v.Write(page, pg*PageSize); err != nil {
			b.Fatal(err)
		}
	}
	if err := v.Flush(); err != nil {
		b.Fatal(err)
	}

	if err := checkpointAndClone(b, v, "snap"); err != nil {
		b.Fatal(err)
	}

	// Open clone on a separate manager (same store), mirroring production.
	m2 := &Manager{BlobStore: store, config: cfg, fs: NewSimLocalFS()}
	b.Cleanup(func() { _ = m2.Close() })
	clone, err := m2.OpenVolume("snap")
	if err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, PageSize)
	// Warm.
	for pg := uint64(0); pg < BlockPages; pg++ {
		if _, err := clone.Read(ctx, buf, pg*PageSize); err != nil {
			b.Fatal(err)
		}
	}

	readRng := rand.New(rand.NewPCG(99, 0))
	b.SetBytes(PageSize)
	b.ResetTimer()
	for range b.N {
		off := (readRng.Uint64() % BlockPages) * PageSize
		if _, err := clone.Read(ctx, buf, off); err != nil {
			b.Fatal(err)
		}
	}
}
