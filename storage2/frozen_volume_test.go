package storage2

import (
	"testing"
	"time"

	"github.com/semistrict/loophole"
)

// TestFrozenZygoteCloneS3Ops creates a volume, writes data, flushes, freezes it,
// then clones from the frozen zygote on a fresh manager. It logs every S3
// operation so we can see the exact cost of a clone-from-frozen-zygote.
func TestFrozenZygoteCloneS3Ops(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   time.Hour,
	}
	ctx := t.Context()

	// Phase 1: Create a zygote volume, write some data, flush, freeze.
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(ctx, loophole.CreateParams{Volume: "zygote", Size: 16 * 1024 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple pages to simulate a rootfs with some data.
	page := make([]byte, PageSize)
	for i := range 100 {
		page[0] = byte(i)
		if err := v.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := v.Freeze(); err != nil {
		t.Fatal(err)
	}
	if err := v.ReleaseRef(); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	t.Logf("Setup complete. S3 keys in store: %d", len(store.Keys("")))
	t.Logf("Setup ops: Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNX=%d List=%d Delete=%d",
		store.Count(loophole.OpGet), store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS), store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists), store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))

	// Phase 2: Fresh manager — simulate a different node opening the frozen
	// zygote and cloning from it. This is the path the cf-demo daemon takes.
	store.ResetCounts()

	m2 := newTestManager(t, store, cfg)

	// Open the frozen zygote — should detect frozen meta, use frozenVolume.
	zygote, err := m2.OpenVolume(ctx, "zygote")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("--- After OpenVolume(zygote) ---")
	t.Logf("  Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNX=%d List=%d Delete=%d",
		store.Count(loophole.OpGet), store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS), store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists), store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))
	t.Logf("  BytesRx=%d BytesTx=%d", store.BytesRx(), store.BytesTx())

	if !zygote.ReadOnly() {
		t.Fatal("expected zygote to be read-only")
	}

	// Reset counters — now measure JUST the clone.
	store.ResetCounts()

	clone, err := zygote.Clone("my-sandbox")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("--- After Clone(my-sandbox) ---")
	t.Logf("  Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNX=%d List=%d Delete=%d",
		store.Count(loophole.OpGet), store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS), store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists), store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))
	t.Logf("  BytesRx=%d BytesTx=%d", store.BytesRx(), store.BytesTx())

	// Verify clone has the data.
	buf := make([]byte, PageSize)
	store.ResetCounts()

	for _, pageIdx := range []int{0, 50, 99} {
		if _, err := clone.Read(ctx, buf, uint64(pageIdx)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", pageIdx, err)
		}
		if buf[0] != byte(pageIdx) {
			t.Fatalf("page %d: expected 0x%02x, got 0x%02x", pageIdx, byte(pageIdx), buf[0])
		}
	}

	t.Logf("--- After reading 3 pages from clone ---")
	t.Logf("  Get=%d PutBytes=%d PutBytesCAS=%d PutReader=%d PutIfNX=%d List=%d Delete=%d",
		store.Count(loophole.OpGet), store.Count(loophole.OpPutBytes),
		store.Count(loophole.OpPutBytesCAS), store.Count(loophole.OpPutReader),
		store.Count(loophole.OpPutIfNotExists), store.Count(loophole.OpListKeys),
		store.Count(loophole.OpDeleteObject))
	t.Logf("  BytesRx=%d BytesTx=%d", store.BytesRx(), store.BytesTx())

	// Verify clone is writable.
	page[0] = 0xFF
	if err := clone.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xFF {
		t.Fatalf("clone write: expected 0xFF, got 0x%02x", buf[0])
	}

	// Verify zygote is unchanged.
	if _, err := zygote.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x00 {
		t.Fatalf("zygote after clone write: expected 0x00, got 0x%02x", buf[0])
	}
}
