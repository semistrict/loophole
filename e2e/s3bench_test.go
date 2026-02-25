package e2e

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/semistrict/loophole"
)

// TestS3Bench measures raw S3 upload throughput and object creation speed.
func TestS3Bench(t *testing.T) {
	skipE2E(t)
	ctx := t.Context()

	inst := uniqueInstance(t)
	store, err := loophole.NewS3Store(ctx, inst, defaultS3Options())
	if err != nil {
		t.Fatal(err)
	}
	base := store.At("bench")

	// --- Object creation latency: 100 small (1KB) objects sequentially ---
	t.Run("CreateLatency", func(t *testing.T) {
		data := make([]byte, 1024)
		rand.Read(data)

		start := time.Now()
		const n = 100
		for i := range n {
			key := fmt.Sprintf("small/%04d", i)
			if err := base.PutReader(ctx, key, bytes.NewReader(data)); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%d objects in %v = %.1f objects/sec, avg latency %.1fms",
			n, elapsed, float64(n)/elapsed.Seconds(), float64(elapsed.Milliseconds())/float64(n))
	})

	// --- Upload throughput: 20 concurrent 4MB uploads ---
	t.Run("UploadThroughput", func(t *testing.T) {
		const blockSize = 4 * 1024 * 1024
		const concurrent = 20
		const totalBlocks = 40

		data := make([]byte, blockSize)
		rand.Read(data)

		start := time.Now()
		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrent)
		var uploaded atomic.Int64

		for i := range totalBlocks {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				key := fmt.Sprintf("big/%04d", i)
				if err := base.PutReader(ctx, key, bytes.NewReader(data)); err != nil {
					t.Errorf("put %d: %v", i, err)
					return
				}
				uploaded.Add(1)
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)

		totalBytes := uploaded.Load() * blockSize
		t.Logf("%d x 4MB blocks, %d concurrent: %v elapsed, %.1f MB/s",
			uploaded.Load(), concurrent, elapsed,
			float64(totalBytes)/elapsed.Seconds()/1024/1024)
	})

	// --- Single object latency: 10 sequential 4MB uploads ---
	t.Run("SingleUploadLatency", func(t *testing.T) {
		const blockSize = 4 * 1024 * 1024
		data := make([]byte, blockSize)
		rand.Read(data)

		const n = 10
		start := time.Now()
		for i := range n {
			key := fmt.Sprintf("seq/%04d", i)
			if err := base.PutReader(ctx, key, bytes.NewReader(data)); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%d x 4MB sequential: %v elapsed, avg %.0fms per upload, %.1f MB/s",
			n, elapsed, float64(elapsed.Milliseconds())/float64(n),
			float64(n*blockSize)/elapsed.Seconds()/1024/1024)
	})
}
