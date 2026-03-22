package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/semistrict/loophole/internal/blob"
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

func newBenchManager(b *testing.B, store *blob.Store, config Config) *Manager {
	b.Helper()
	if _, _, err := FormatVolumeSet(context.Background(), store); err != nil {
		b.Fatal(err)
	}
	m := &Manager{BlobStore: store, config: config, fs: NewSimLocalFS()}
	b.Cleanup(func() {
		_ = m.Close()
	})
	return m
}

var defaultBenchConfig = Config{
	FlushThreshold: 64 * PageSize,
}

// BenchmarkS3Ops measures S3 operation counts for various workloads.
// Wall-clock times are meaningless (MemDriver); only the s3-*/op metrics matter.
func BenchmarkS3Ops(b *testing.B) {
	b.Run("Write64Flush", func(b *testing.B) {
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		m := newBenchManager(b, store, defaultBenchConfig)

		v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize})
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewPCG(1, 0))
		page := make([]byte, PageSize)

		mem.ResetCounts()
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
		reportS3Counts(b, mem, b.N)
	})

	b.Run("Read256Pages", func(b *testing.B) {
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		m := newBenchManager(b, store, defaultBenchConfig)
		ctx := context.Background()

		v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize})
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
		mem.ResetCounts()
		for range b.N {
			for pg := uint64(0); pg < 256; pg++ {
				if _, err := v.Read(ctx, buf, pg*PageSize); err != nil {
					b.Fatal(err)
				}
			}
		}
		reportS3Counts(b, mem, b.N)
	})

	b.Run("Snapshot", func(b *testing.B) {
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		m := newBenchManager(b, store, defaultBenchConfig)

		v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize})
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

		mem.ResetCounts()
		for i := range b.N {
			if err := checkpointAndClone(b, v, fmt.Sprintf("snap-%d", i)); err != nil {
				b.Fatal(err)
			}
		}
		reportS3Counts(b, mem, b.N)
	})

	b.Run("Snapshot100", func(b *testing.B) {
		for range b.N {
			mem := blob.NewMemDriver()
			store := blob.New(mem)
			m := newBenchManager(b, store, defaultBenchConfig)

			v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize})
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

			mem.ResetCounts()
			for i := range 100 {
				if err := checkpointAndClone(b, v, fmt.Sprintf("snap-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			reportS3Counts(b, mem, 100)
		}
	})

}

var s3OpNames = []struct {
	name string
	op   blob.OpType
}{
	{"Get", blob.OpGet},
	{"Put", blob.OpPut},
	{"Delete", blob.OpDelete},
	{"List", blob.OpList},
}

func reportS3Counts(b *testing.B, drv *blob.MemDriver, n int) {
	b.Helper()
	for _, o := range s3OpNames {
		b.ReportMetric(float64(drv.Count(o.op))/float64(n), "s3-"+o.name+"/op")
	}
	b.ReportMetric(float64(drv.BytesRx())/float64(n), "s3-rx-bytes/op")
}

// BenchmarkWritePage measures the cost of writing a single sub-page chunk
// into the dirty page layer. This is the hot path for every FUSE write.
func BenchmarkWritePage(b *testing.B) {
	for _, chunkSize := range []int{512, 1024, 4096, 16384, 32768, PageSize, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024} {
		name := fmt.Sprintf("chunk=%d", chunkSize)
		b.Run(name, func(b *testing.B) {
			store := blob.New(blob.NewMemDriver())
			config := Config{
				FlushThreshold: 512 * 1024 * 1024, // huge threshold to avoid flush during bench
			}
			m := newBenchManager(b, store, config)

			v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024 * 1024})
			if err != nil {
				b.Fatal(err)
			}

			rng := rand.New(rand.NewPCG(1, 0))
			chunk := make([]byte, chunkSize)
			randomPage(rng, chunk)

			// Total addressable pages.
			numPages := 1024 * 1024 * 1024 / PageSize

			b.SetBytes(int64(chunkSize))
			b.ResetTimer()
			for i := range b.N {
				pageIdx := i % numPages
				offset := uint64(pageIdx) * PageSize
				if err := v.Write(chunk, offset); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadFewFromLargeLayer writes 1024 pages into a single delta layer,
// then reads only 4 pages from a fresh manager (no in-memory cache).
// This is the scenario where range reads win: instead of downloading the
// entire ~4MB layer, we fetch just the index + 4 compressed pages.
func BenchmarkReadFewFromLargeLayer(b *testing.B) {
	mem := blob.NewMemDriver()
	store := blob.New(mem)
	ctx := context.Background()

	const totalPages = 1024
	const readPages = 4

	// Write the data once.
	m := newBenchManager(b, store, defaultBenchConfig)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: totalPages * PageSize})
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

	mem.ResetCounts()
	b.ResetTimer()
	for range b.N {
		// Fresh manager each iteration — no cached layers.
		m2 := newBenchManager(b, store, defaultBenchConfig)
		v2, err := m2.OpenVolume("vol")
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
	reportS3Counts(b, mem, b.N)
}
