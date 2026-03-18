package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/semistrict/loophole/objstore"
)

// TestDeepCloneChainReadCost creates a chain of 1000 snapshots and verifies
// that reading from the leaf doesn't require O(n) S3 gets.
func TestDeepCloneChainReadCost(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  time.Hour,
	}
	ctx := t.Context()
	m := newTestManager(t, store, cfg)

	// Create root volume and write data.
	v, err := m.NewVolume(CreateParams{Volume: "root", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	page[0] = 0xDE
	page[1] = 0xAD
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Build a chain of 1000 clones: root → c0 → c1 → ... → c999.
	// Each iteration: clone from prev, close prev, open clone.
	const chainLen = 1000
	prev := v
	for i := range chainLen {
		name := fmt.Sprintf("c%d", i)
		if err := prev.Clone(name); err != nil {
			t.Fatalf("clone %d: %v", i, err)
		}
		if err := prev.ReleaseRef(); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
		next, err := m.OpenVolume(name)
		if err != nil {
			t.Fatalf("open clone %d: %v", i, err)
		}
		prev = next
	}
	// Release the leaf too.
	if err := prev.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	// Open the leaf on a fresh manager so everything is loaded from S3.
	m2 := newTestManager(t, store, cfg)
	leaf, err := m2.OpenVolume(fmt.Sprintf("c%d", chainLen-1))
	if err != nil {
		t.Fatal(err)
	}

	// Log total S3 ops for chain construction + open.
	if debugCountersEnabled() {
		t.Logf("S3 ops for chain construction + open: Get=%d PutCAS=%d PutReader=%d PutIfNX=%d List=%d Delete=%d",
			store.Count(objstore.OpGet), store.Count(objstore.OpPutBytesCAS),
			store.Count(objstore.OpPutReader), store.Count(objstore.OpPutIfNotExists),
			store.Count(objstore.OpListKeys), store.Count(objstore.OpDeleteObject))
	}

	// Reset S3 counters, then read.
	store.ResetCounts()

	buf := make([]byte, PageSize)
	if _, err := leaf.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xDE || buf[1] != 0xAD {
		t.Fatalf("expected 0xDEAD, got 0x%02X%02X", buf[0], buf[1])
	}

	gets := store.Count(objstore.OpGet)
	if debugCountersEnabled() {
		t.Logf("S3 gets for one read across %d-deep chain: %d", chainLen, gets)
	}
	if gets > 20 {
		t.Fatalf("expected few S3 gets for a read, got %d (chain depth %d)", gets, chainLen)
	}

	// Second read of the same page should hit layer caches — zero S3 gets.
	store.ResetCounts()
	if _, err := leaf.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	gets2 := store.Count(objstore.OpGet)
	if debugCountersEnabled() {
		t.Logf("S3 gets for second read: %d", gets2)
	}
	if gets2 != 0 {
		t.Fatalf("expected 0 S3 gets for cached read, got %d", gets2)
	}
}
