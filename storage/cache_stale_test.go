package storage

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole/objstore"
)

// TestStalePageCacheAfterFlush verifies that flushing a dirty pages to S3
// invalidates page cache entries for affected pages.
//
// Without flush-time invalidation, the sequence:
//  1. Write A to page 0, flush → A in S3 delta
//  2. Read page 0 → readPage fetches A from S3, caches A
//  3. Write B to page 0, flush → B in S3 delta (newer)
//  4. Read page 0 → readPage checks: dirty pages (empty), frozen (empty),
//     page cache (stale A!) → returns A instead of B
//
// The page cache must be invalidated at flush time (step 3) so that
// step 4 misses the cache and finds B in the S3 delta layers.
func TestStalePageCacheAfterFlush(t *testing.T) {
	cfg := testConfig
	cfg.FlushThreshold = 256 * PageSize // high so flushes are manual only

	m := newTestManager(t, objstore.NewMemStore(), cfg)
	ctx := t.Context()

	vol, err := m.NewVolume(CreateParams{Volume: "stale-flush"})
	if err != nil {
		t.Fatal(err)
	}

	for iter := range 50 {
		valA := byte(iter%127 + 1)
		valB := byte((iter+64)%127 + 1)
		if valB == valA {
			valB = valA + 1
		}

		// Step 1: Write A, flush to S3.
		dataA := bytes.Repeat([]byte{valA}, PageSize)
		if err := vol.Write(dataA, 0); err != nil {
			t.Fatal(err)
		}
		if err := vol.Flush(); err != nil {
			t.Fatal(err)
		}

		// Step 2: Read page 0 — populates page cache with A.
		buf := make([]byte, PageSize)
		if _, err := vol.Read(ctx, buf, 0); err != nil {
			t.Fatal(err)
		}
		if buf[0] != valA {
			t.Fatalf("iter %d: pre-read got 0x%02x, want 0x%02x", iter, buf[0], valA)
		}

		// Step 3: Write B, flush to S3.
		dataB := bytes.Repeat([]byte{valB}, PageSize)
		if err := vol.Write(dataB, 0); err != nil {
			t.Fatal(err)
		}
		if err := vol.Flush(); err != nil {
			t.Fatal(err)
		}

		// Step 4: Read page 0 — must see B, not stale A from cache.
		if _, err := vol.Read(ctx, buf, 0); err != nil {
			t.Fatal(err)
		}
		if buf[0] != valB {
			t.Fatalf("iter %d: post-flush read got 0x%02x (stale A=0x%02x), want 0x%02x (B)",
				iter, buf[0], valA, valB)
		}
	}
}
