package storage

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/semistrict/loophole/objstore"
)

const (
	benchVolumeSize = 64 * 1024 * 1024 // 64 MiB
	fuseMaxWrite    = 1024 * 1024      // 1 MiB
)

func setupBenchVolume(b *testing.B, flush bool) (*Volume, *objstore.MemStore) {
	b.Helper()
	store := objstore.NewMemStore()
	m := newBenchManager(b, store, Config{FlushThreshold: benchVolumeSize + PageSize})
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: benchVolumeSize})
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(42, 0))
	page := make([]byte, PageSize)
	totalPages := benchVolumeSize / PageSize
	for pg := uint64(0); pg < uint64(totalPages); pg++ {
		randomPage(rng, page)
		if err := v.Write(page, pg*PageSize); err != nil {
			b.Fatal(err)
		}
	}

	if flush {
		if err := v.Flush(); err != nil {
			b.Fatal(err)
		}
		buf := make([]byte, PageSize)
		ctx := context.Background()
		for pg := uint64(0); pg < uint64(totalPages); pg++ {
			if _, err := v.Read(ctx, buf, pg*PageSize); err != nil {
				b.Fatal(err)
			}
		}
	}

	return v, store
}

// --- Sequential 1 MiB reads ---

func BenchmarkFuseSeqRead1MiB(b *testing.B) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		flush bool
	}{
		{"dirtyPages", false},
		{"flushed", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			v, _ := setupBenchVolume(b, tc.flush)
			maxOff := benchVolumeSize - fuseMaxWrite

			b.Run("copy", func(b *testing.B) {
				buf := make([]byte, fuseMaxWrite)
				b.SetBytes(fuseMaxWrite)
				b.ResetTimer()
				for i := range b.N {
					off := uint64(i*fuseMaxWrite) % uint64(maxOff)
					if _, err := v.Read(ctx, buf, off); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("zerocopy", func(b *testing.B) {
				b.SetBytes(fuseMaxWrite)
				b.ResetTimer()
				for i := range b.N {
					off := uint64(i*fuseMaxWrite) % uint64(maxOff)
					_, cleanup, err := v.ReadPages(ctx, off, fuseMaxWrite)
					if err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		})
	}
}

// --- Random 4K reads ---

func BenchmarkFuseRandRead4K(b *testing.B) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		flush bool
	}{
		{"dirtyPages", false},
		{"flushed", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			v, _ := setupBenchVolume(b, tc.flush)
			totalPages := uint64(benchVolumeSize / PageSize)

			b.Run("copy", func(b *testing.B) {
				buf := make([]byte, PageSize)
				rng := rand.New(rand.NewPCG(99, 0))
				b.SetBytes(PageSize)
				b.ResetTimer()
				for range b.N {
					off := (rng.Uint64() % totalPages) * PageSize
					if _, err := v.Read(ctx, buf, off); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("zerocopy", func(b *testing.B) {
				rng := rand.New(rand.NewPCG(99, 0))
				b.SetBytes(PageSize)
				b.ResetTimer()
				for range b.N {
					off := (rng.Uint64() % totalPages) * PageSize
					_, cleanup, err := v.ReadPages(ctx, off, PageSize)
					if err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		})
	}
}

// --- Sequential 1 MiB writes ---

func BenchmarkFuseSeqWrite1MiB(b *testing.B) {
	b.Run("dirtyPages", func(b *testing.B) {
		v, _ := setupBenchVolume(b, false)
		rng := rand.New(rand.NewPCG(77, 0))
		data := make([]byte, fuseMaxWrite)
		randomPage(rng, data)
		maxOff := benchVolumeSize - fuseMaxWrite
		b.SetBytes(fuseMaxWrite)
		b.ResetTimer()
		for i := range b.N {
			off := uint64(i*fuseMaxWrite) % uint64(maxOff)
			if err := v.Write(data, off); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Random 4K writes ---

func BenchmarkFuseRandWrite4K(b *testing.B) {
	b.Run("dirtyPages", func(b *testing.B) {
		v, _ := setupBenchVolume(b, false)
		rng := rand.New(rand.NewPCG(88, 0))
		data := make([]byte, PageSize)
		totalPages := uint64(benchVolumeSize / PageSize)
		b.SetBytes(PageSize)
		b.ResetTimer()
		for range b.N {
			randomPage(rng, data)
			off := (rng.Uint64() % totalPages) * PageSize
			if err := v.Write(data, off); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFuseRandOverwriteHotset4K emulates buffered FUSE writes landing on
// a small hot set of pages. This is the allocator-churn pattern the dirty page
// pool is meant to improve: each write stages a fresh 4 KiB page while
// overwriting and retiring a recently written dirty page from the same working
// set. Throughput may move only slightly; allocs/op is the more important
// signal here.
func BenchmarkFuseRandOverwriteHotset4K(b *testing.B) {
	b.Run("dirtyPages", func(b *testing.B) {
		v, _ := setupBenchVolume(b, false)
		rng := rand.New(rand.NewPCG(1234, 0))
		data := make([]byte, PageSize)
		const hotPages = 256

		b.ReportAllocs()
		b.SetBytes(PageSize)
		b.ResetTimer()
		for range b.N {
			randomPage(rng, data)
			off := (rng.Uint64() % hotPages) * PageSize
			if err := v.Write(data, off); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Mixed workload ---

func BenchmarkFuseMixed(b *testing.B) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		flush bool
	}{
		{"dirtyPages", false},
		{"flushed", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			v, _ := setupBenchVolume(b, tc.flush)
			totalPages := uint64(benchVolumeSize / PageSize)
			maxOff128K := uint64(benchVolumeSize - 128*1024)

			b.Run("copy", func(b *testing.B) {
				rng := rand.New(rand.NewPCG(55, 0))
				readBuf4K := make([]byte, PageSize)
				readBuf128K := make([]byte, 128*1024)
				writeBuf := make([]byte, PageSize)
				b.SetBytes(PageSize)
				b.ResetTimer()
				for range b.N {
					r := rng.Uint32() % 100
					switch {
					case r < 70:
						off := (rng.Uint64() % totalPages) * PageSize
						if _, err := v.Read(ctx, readBuf4K, off); err != nil {
							b.Fatal(err)
						}
					case r < 90:
						randomPage(rng, writeBuf)
						off := (rng.Uint64() % totalPages) * PageSize
						if err := v.Write(writeBuf, off); err != nil {
							b.Fatal(err)
						}
					default:
						off := rng.Uint64() % maxOff128K
						off = off &^ (PageSize - 1)
						if _, err := v.Read(ctx, readBuf128K, off); err != nil {
							b.Fatal(err)
						}
					}
				}
			})

			b.Run("zerocopy", func(b *testing.B) {
				rng := rand.New(rand.NewPCG(55, 0))
				writeBuf := make([]byte, PageSize)
				b.SetBytes(PageSize)
				b.ResetTimer()
				for range b.N {
					r := rng.Uint32() % 100
					switch {
					case r < 70:
						off := (rng.Uint64() % totalPages) * PageSize
						_, cleanup, err := v.ReadPages(ctx, off, PageSize)
						if err != nil {
							b.Fatal(err)
						}
						cleanup()
					case r < 90:
						randomPage(rng, writeBuf)
						off := (rng.Uint64() % totalPages) * PageSize
						if err := v.Write(writeBuf, off); err != nil {
							b.Fatal(err)
						}
					default:
						off := rng.Uint64() % maxOff128K
						off = off &^ (PageSize - 1)
						_, cleanup, err := v.ReadPages(ctx, off, 128*1024)
						if err != nil {
							b.Fatal(err)
						}
						cleanup()
					}
				}
			})
		})
	}
}
