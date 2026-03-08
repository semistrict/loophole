package storage2

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole"
)

func randomPage(rng *rand.Rand, buf []byte) {
	for i := 0; i < len(buf); i += 4 {
		v := rng.Uint32()
		buf[i] = byte(v)
		buf[i+1] = byte(v >> 8)
		buf[i+2] = byte(v >> 16)
		buf[i+3] = byte(v >> 24)
	}
}

func newBenchManager(b *testing.B, store *loophole.MemStore, config Config) *Manager {
	b.Helper()
	cacheDir := b.TempDir()
	dc, err := NewPageCache(filepath.Join(cacheDir, "diskcache"))
	if err != nil {
		b.Fatalf("create page cache: %v", err)
	}
	m := NewVolumeManager(store, cacheDir, config, NewSimLocalFS(), dc)
	b.Cleanup(func() {
		_ = m.Close(b.Context())
		_ = dc.Close()
	})
	return m
}

var defaultBenchConfig = Config{
	FlushThreshold:  64 * PageSize,
	MaxFrozenTables: 2,
}

// BenchmarkS3Ops measures S3 operation counts for various workloads.
// Wall-clock times are meaningless (MemStore); only the s3-*/op metrics matter.
func BenchmarkS3Ops(b *testing.B) {
	b.Run("Write64Flush", func(b *testing.B) {
		store := loophole.NewMemStore()
		m := newBenchManager(b, store, defaultBenchConfig)
		ctx := context.Background()

		v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewPCG(1, 0))
		page := make([]byte, PageSize)

		store.ResetCounts()
		for range b.N {
			for pg := 0; pg < 64; pg++ {
				randomPage(rng, page)
				if err := v.Write(page, uint64(pg)*PageSize); err != nil {
					b.Fatal(err)
				}
			}
			if err := v.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		reportS3Counts(b, store, b.N)
	})

	b.Run("Read256Pages", func(b *testing.B) {
		store := loophole.NewMemStore()
		m := newBenchManager(b, store, defaultBenchConfig)
		ctx := context.Background()

		v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewPCG(1, 0))
		page := make([]byte, PageSize)
		for pg := uint64(0); pg < 256; pg++ {
			randomPage(rng, page)
			if err := v.Write(page, pg*PageSize); err != nil {
				b.Fatal(err)
			}
		}
		if err := v.Flush(); err != nil {
			b.Fatal(err)
		}

		buf := make([]byte, PageSize)
		store.ResetCounts()
		for range b.N {
			for pg := uint64(0); pg < 256; pg++ {
				if _, err := v.Read(ctx, buf, pg*PageSize); err != nil {
					b.Fatal(err)
				}
			}
		}
		reportS3Counts(b, store, b.N)
	})

	b.Run("Snapshot", func(b *testing.B) {
		store := loophole.NewMemStore()
		m := newBenchManager(b, store, defaultBenchConfig)
		ctx := context.Background()

		v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewPCG(1, 0))
		page := make([]byte, PageSize)
		for pg := uint64(0); pg < 256; pg++ {
			randomPage(rng, page)
			if err := v.Write(page, pg*PageSize); err != nil {
				b.Fatal(err)
			}
		}
		if err := v.Flush(); err != nil {
			b.Fatal(err)
		}

		store.ResetCounts()
		for i := range b.N {
			if err := v.Snapshot(fmt.Sprintf("snap-%d", i)); err != nil {
				b.Fatal(err)
			}
		}
		reportS3Counts(b, store, b.N)
	})

	b.Run("Snapshot100", func(b *testing.B) {
		for range b.N {
			store := loophole.NewMemStore()
			m := newBenchManager(b, store, defaultBenchConfig)
			ctx := context.Background()

			v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
			if err != nil {
				b.Fatal(err)
			}

			rng := rand.New(rand.NewPCG(1, 0))
			page := make([]byte, PageSize)
			for pg := uint64(0); pg < 256; pg++ {
				randomPage(rng, page)
				if err := v.Write(page, pg*PageSize); err != nil {
					b.Fatal(err)
				}
			}
			if err := v.Flush(); err != nil {
				b.Fatal(err)
			}

			store.ResetCounts()
			for i := range 100 {
				if err := v.Snapshot(fmt.Sprintf("snap-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			reportS3Counts(b, store, 100)
		}
	})

	b.Run("Compact", func(b *testing.B) {
		for range b.N {
			store := loophole.NewMemStore()
			m := newBenchManager(b, store, Config{
				FlushThreshold:  4 * PageSize,
				MaxFrozenTables: 2,
			})
			ctx := context.Background()

			v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
			if err != nil {
				b.Fatal(err)
			}
			vol := v.(*volume)

			rng := rand.New(rand.NewPCG(1, 0))
			page := make([]byte, PageSize)
			for pg := 0; pg < 128; pg++ {
				randomPage(rng, page)
				if err := v.Write(page, uint64(pg)*PageSize); err != nil {
					b.Fatal(err)
				}
			}
			if err := v.Flush(); err != nil {
				b.Fatal(err)
			}

			store.ResetCounts()
			if err := vol.layer.CompactL0(ctx); err != nil {
				b.Fatal(err)
			}
			reportS3Counts(b, store, 1)
		}
	})
}

var s3OpNames = []struct {
	name string
	op   loophole.OpType
}{
	{"Get", loophole.OpGet},
	{"Put", loophole.OpPutReader},
	{"PutIfNX", loophole.OpPutIfNotExists},
	{"CAS", loophole.OpPutBytesCAS},
	{"Delete", loophole.OpDeleteObject},
	{"List", loophole.OpListKeys},
}

func reportS3Counts(b *testing.B, store *loophole.MemStore, n int) {
	b.Helper()
	for _, o := range s3OpNames {
		b.ReportMetric(float64(store.Count(o.op))/float64(n), "s3-"+o.name+"/op")
	}
	b.ReportMetric(float64(store.BytesRx())/float64(n), "s3-rx-bytes/op")
}

// BenchmarkReadFewFromLargeLayer writes 1024 pages into a single delta layer,
// then reads only 4 pages from a fresh manager (no in-memory cache).
// This is the scenario where range reads win: instead of downloading the
// entire ~4MB layer, we fetch just the index + 4 compressed pages.
func BenchmarkReadFewFromLargeLayer(b *testing.B) {
	store := loophole.NewMemStore()
	ctx := context.Background()

	const totalPages = 1024
	const readPages = 4

	// Write the data once.
	m := newBenchManager(b, store, defaultBenchConfig)
	v, err := m.NewVolume(ctx, "vol", totalPages*PageSize, "")
	if err != nil {
		b.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 0))
	page := make([]byte, PageSize)
	for pg := 0; pg < totalPages; pg++ {
		randomPage(rng, page)
		if err := v.Write(page, uint64(pg)*PageSize); err != nil {
			b.Fatal(err)
		}
	}
	if err := v.Flush(); err != nil {
		b.Fatal(err)
	}

	readAddrs := [readPages]uint64{0, 256, 512, 768}
	buf := make([]byte, PageSize)

	store.ResetCounts()
	b.ResetTimer()
	for range b.N {
		// Fresh manager each iteration — no cached layers.
		m2 := newBenchManager(b, store, defaultBenchConfig)
		v2, err := m2.OpenVolume(ctx, "vol")
		if err != nil {
			b.Fatal(err)
		}
		for _, addr := range readAddrs {
			if _, err := v2.Read(ctx, buf, addr*PageSize); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	reportS3Counts(b, store, b.N)
}
