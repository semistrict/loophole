package storage2

import (
	"bytes"
	"sync"
	"testing"

	"github.com/semistrict/loophole"
)

// TestConcurrentPartialPageWrites verifies that concurrent sub-page writes
// targeting the same 64KB page don't lose data. This reproduces the NBD bug
// where the kernel sends 4KB writes concurrently and the read-modify-write
// cycle in Timeline.Write races, causing lost updates.
func TestConcurrentPartialPageWrites(t *testing.T) {
	cfg := testConfig
	cfg.FlushThreshold = 256 * PageSize

	m := newTestManager(t, loophole.NewMemStore(), cfg)
	ctx := t.Context()

	vol, err := m.NewVolume(ctx, loophole.CreateParams{Volume: "race"})
	if err != nil {
		t.Fatal(err)
	}

	// Write 16 concurrent 4KB chunks into the same 64KB page (page 0).
	// Each chunk fills its 4KB region with a distinct byte value.
	const chunkSize = 4096
	const nChunks = PageSize / chunkSize // 16

	var wg sync.WaitGroup
	errs := make([]error, nChunks)
	for i := range nChunks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
			offset := uint64(i) * chunkSize
			errs[i] = vol.Write(data, offset)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}

	buf := make([]byte, PageSize)
	n, err := vol.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("short read: %d", n)
	}

	for i := range nChunks {
		start := i * chunkSize
		end := start + chunkSize
		expected := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
		if !bytes.Equal(buf[start:end], expected) {
			t.Errorf("chunk %d (offset %d): got 0x%02x, want 0x%02x",
				i, start, buf[start], byte(i+1))
		}
		_ = end
	}
}

// TestConcurrentWriteAndPunchHole verifies that a PunchHole tombstone for a
// full page doesn't clobber a concurrent sub-page write to the same page.
//
// This reproduces an NBD bug: with multi-connection NBD, the kernel can send
// a TRIM (PunchHole) for a 64KB page on one connection and a 4KB WRITE to the
// same page on another connection concurrently. Because PunchHole's full-page
// tombstone path didn't acquire the per-page lock, the tombstone could overwrite
// the write, causing reads to return zeros where data was expected.
func TestConcurrentWriteAndPunchHole(t *testing.T) {
	cfg := testConfig
	// Use a very large threshold so auto-flush never triggers. This prevents
	// delta layer creation, which avoids zstd decompression. The zstd decoder
	// pool is global; if TestSimulation (synctest) runs first, its channels
	// get tainted by the synctest bubble, causing "receive on synctest channel
	// from outside bubble" when this non-synctest test uses zstd.
	cfg.FlushThreshold = 1 << 62

	m := newTestManager(t, loophole.NewMemStore(), cfg)
	ctx := t.Context()

	vol, err := m.NewVolume(ctx, loophole.CreateParams{Volume: "punch-race"})
	if err != nil {
		t.Fatal(err)
	}

	// Target page 0 (offset 0..PageSize-1).
	// First seed the page with known data so we can detect clobbering.
	seed := bytes.Repeat([]byte{0xAA}, PageSize)
	if err := vol.Write(seed, 0); err != nil {
		t.Fatal(err)
	}

	// Run many iterations: concurrent PunchHole (full page) + Write (sub-page).
	// After both complete, the page must contain either:
	//   a) all zeros (PunchHole won) with the write's chunk on top, OR
	//   b) the write's chunk at offset 0 with zeros elsewhere
	// In either case the write's 4KB chunk must be present — a tombstone
	// must not silently discard a concurrent write.
	const chunkSize = 4096
	const iterations = 500

	for iter := range iterations {
		// Re-seed the full page.
		if err := vol.Write(seed, 0); err != nil {
			t.Fatal(err)
		}

		writeData := bytes.Repeat([]byte{byte(iter%254 + 1)}, chunkSize)
		var wg sync.WaitGroup
		var writeErr, punchErr error

		wg.Add(2)
		go func() {
			defer wg.Done()
			// PunchHole the full page (offset 0, length PageSize).
			punchErr = vol.PunchHole(0, PageSize)
		}()
		go func() {
			defer wg.Done()
			// Write 4KB at offset 0 of the same page.
			writeErr = vol.Write(writeData, 0)
		}()
		wg.Wait()

		if writeErr != nil {
			t.Fatalf("iter %d: write: %v", iter, writeErr)
		}
		if punchErr != nil {
			t.Fatalf("iter %d: punch: %v", iter, punchErr)
		}

		// Read back the page.
		buf := make([]byte, PageSize)
		n, err := vol.Read(ctx, buf, 0)
		if err != nil {
			t.Fatalf("iter %d: read: %v", iter, err)
		}
		if n != PageSize {
			t.Fatalf("iter %d: short read: %d", iter, n)
		}

		// The 4KB write and the full-page PunchHole raced. One of two
		// valid outcomes:
		//   1. PunchHole finalized last → entire page is zeros
		//   2. Write finalized last    → first 4KB = writeData, rest = zeros
		//
		// What must NOT happen: the first 4KB is zeros while the rest
		// has stale 0xAA data (tombstone clobbered write but didn't
		// actually zero the whole page), or the write's data is missing
		// while the seed data remains (tombstone won partially).

		first4k := buf[:chunkSize]
		rest := buf[chunkSize:]
		restAllZero := bytes.Equal(rest, make([]byte, len(rest)))

		if bytes.Equal(first4k, writeData) && restAllZero {
			// Outcome 2: write after punch. ✓
			continue
		}
		if bytes.Equal(buf, make([]byte, PageSize)) {
			// Outcome 1: punch after write. ✓
			continue
		}

		// Any other state is a bug.
		t.Fatalf("iter %d: corrupt page state\n  first4k[0]=0x%02x (want 0x%02x or 0x00)\n  rest all-zero=%v\n  rest[0]=0x%02x",
			iter, first4k[0], writeData[0], restAllZero, rest[0])
	}
}
