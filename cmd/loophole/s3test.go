package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/semistrict/loophole/internal/util"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/objstore"
)

func s3testCmd() *cobra.Command {
	var count int
	var parallelOnly bool
	cmd := &cobra.Command{
		Use:   "s3test",
		Short: "Benchmark S3 latency and throughput",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := env.DefaultDir()
			inst, err := resolveProfile(dir)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			store, err := objstore.NewS3Store(ctx, inst)
			if err != nil {
				return err
			}
			s := store.At("_s3test")

			fmt.Printf("S3 benchmark: %s (bucket=%s, profile=%s)\n", inst.Endpoint, inst.Bucket, inst.ProfileName)
			fmt.Printf("Iterations per test: %d\n\n", count)

			if !parallelOnly {
				// Part 1: Latency and single-stream throughput
				fmt.Println("=== Single-stream latency & throughput ===")
				sizes := []struct {
					name string
					size int
				}{
					{"1KB", 1024},
					{"64KB", 64 * 1024},
					{"256KB", 256 * 1024},
					{"1MB", 1 << 20},
					{"4MB", 4 << 20},
				}
				for _, sz := range sizes {
					if err := benchSize(ctx, s, sz.name, sz.size, count); err != nil {
						return err
					}
				}
				fmt.Println()
			}

			// Part 2: Parallel PUT throughput
			fmt.Println("=== Parallel PUT throughput (4MB objects) ===")
			for _, workers := range []int{1, 2, 4, 8} {
				if err := benchParallelPut(ctx, s, 4<<20, workers, count); err != nil {
					return err
				}
			}

			return nil
		},
	}
	cmd.Flags().IntVarP(&count, "count", "n", 5, "iterations per test")
	cmd.Flags().BoolVar(&parallelOnly, "parallel", false, "run only parallel PUT tests")
	return cmd
}

func benchSize(ctx context.Context, store objstore.ObjectStore, label string, size, count int) error {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return err
	}

	key := fmt.Sprintf("bench-%s", label)

	// PUT
	putDurs := make([]time.Duration, count)
	for i := range count {
		start := time.Now()
		if err := store.PutReader(ctx, key, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("PUT %s iter %d: %w", label, i, err)
		}
		putDurs[i] = time.Since(start)
	}

	// GET
	getDurs := make([]time.Duration, count)
	for i := range count {
		start := time.Now()
		rc, _, err := store.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("GET %s iter %d: %w", label, i, err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			util.SafeClose(rc, "close GET body on read error")
			return fmt.Errorf("GET read %s iter %d: %w", label, i, err)
		}
		util.SafeClose(rc, "close GET body")
		getDurs[i] = time.Since(start)
	}

	// Cleanup
	if err := store.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("DELETE %s: %w", label, err)
	}

	putP50, putP99 := percentiles(putDurs)
	getP50, getP99 := percentiles(getDurs)

	sizeMB := float64(size) / (1 << 20)
	putThroughput := sizeMB / putP50.Seconds()
	getThroughput := sizeMB / getP50.Seconds()

	fmt.Printf("%-6s  PUT p50=%-8s p99=%-8s (%5.1f MB/s)  GET p50=%-8s p99=%-8s (%5.1f MB/s)\n",
		label,
		putP50.Round(time.Millisecond), putP99.Round(time.Millisecond), putThroughput,
		getP50.Round(time.Millisecond), getP99.Round(time.Millisecond), getThroughput,
	)
	return nil
}

func benchParallelPut(ctx context.Context, store objstore.ObjectStore, size, workers, totalPuts int) error {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return err
	}

	// Each worker gets its own key to avoid contention.
	keys := make([]string, workers)
	for i := range workers {
		keys[i] = fmt.Sprintf("bench-parallel-%d", i)
	}

	// Total work = totalPuts per worker.
	totalOps := workers * totalPuts
	start := time.Now()

	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range totalPuts {
				if err := store.PutReader(ctx, keys[w], bytes.NewReader(data)); err != nil {
					errs[w] = fmt.Errorf("worker %d iter %d: %w", w, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	// Cleanup
	for _, key := range keys {
		if err := store.DeleteObject(ctx, key); err != nil {
			return fmt.Errorf("DELETE %s: %w", key, err)
		}
	}

	sizeMB := float64(size) / (1 << 20)
	totalMB := sizeMB * float64(totalOps)
	throughput := totalMB / elapsed.Seconds()
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	fmt.Printf("workers=%-2d  %d PUTs in %-8s  %5.1f MB/s  %4.1f ops/s\n",
		workers, totalOps, elapsed.Round(time.Millisecond), throughput, opsPerSec)
	return nil
}

func percentiles(durs []time.Duration) (p50, p99 time.Duration) {
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	p50 = durs[len(durs)/2]
	p99 = durs[int(float64(len(durs))*0.99)]
	return
}
