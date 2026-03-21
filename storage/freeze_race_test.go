package storage

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

// TestFreezeMemtableRaceWithPeriodicFlush reproduces a panic where the
// periodic flush goroutine freezes a memtable into the frozen slot between
// the write path's flushFrozenTables() call and its ly.mu.Lock() in
// freezememtable(). The sequence is:
//
//  1. Write goroutine fills memtable, calls freezememtable()
//  2. freezememtable() sees ly.frozen != nil, calls flushFrozenTables() which drains it
//  3. flushFrozenTables() returns — frozen slot is now empty
//  4. Before the write goroutine acquires ly.mu, the periodic flush goroutine
//     calls freezememtable(), acquires ly.mu first, freezes a new memtable
//  5. Write goroutine acquires ly.mu, finds frozen slot occupied → panic
//
// We reproduce this by using a small flush threshold with periodic flush
// enabled and slow S3 uploads to keep the frozen slot occupied longer.
func TestFreezeMemtableRaceWithPeriodicFlush(t *testing.T) {
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize, // very low: 2 pages triggers freeze
		FlushInterval:  time.Millisecond,
	}

	// Slow down S3 uploads just enough to keep the frozen slot occupied,
	// widening the race window between flushFrozenTables() returning
	// and ly.mu.Lock() in freezememtable().
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Delay: time.Millisecond,
	})

	m := &Manager{ObjectStore: store, config: cfg}
	t.Cleanup(func() { _ = m.Close() })

	const numPages = 200
	v, err := m.NewVolume(CreateParams{Volume: "race-test", Size: uint64(numPages+10) * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	// Hammer writes. With FlushThreshold=2*PageSize and 50ms upload delay,
	// the write path and periodic flush will race on freezememtable.
	page := make([]byte, PageSize)
	for i := range numPages {
		page[0] = byte(i)
		err := v.Write(page, uint64(i%50)*PageSize)
		if err != nil {
			// Write errors from backpressure are acceptable — the panic is not.
			t.Logf("write %d: %v", i, err)
		}
	}

	store.ClearAllFaults()
	if err := v.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
}

// TestFreezeRaceDataIntegrity verifies that no pages are silently lost when
// the write path and periodic flush race on freezememtable. Each page is
// written with a unique byte pattern; after flushing we read every page
// back and verify the contents.
func TestFreezeRaceDataIntegrity(t *testing.T) {
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  time.Millisecond,
	}

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Delay: time.Millisecond,
	})

	m := &Manager{ObjectStore: store, config: cfg}
	t.Cleanup(func() { _ = m.Close() })

	const numPages = 100
	v, err := m.NewVolume(CreateParams{Volume: "integrity-test", Size: uint64(numPages+10) * PageSize})
	require.NoError(t, err)

	// Write each page with a unique pattern. Later pages to the same offset
	// overwrite earlier ones, so track the last value written per offset.
	lastWritten := make(map[uint64]byte)
	for i := range numPages {
		page := bytes.Repeat([]byte{byte(i + 1)}, PageSize)
		offset := uint64(i%50) * PageSize
		err := v.Write(page, offset)
		if err != nil {
			t.Logf("write %d: %v", i, err)
			continue
		}
		lastWritten[offset] = byte(i + 1)
	}

	store.ClearAllFaults()
	require.NoError(t, v.Flush())

	// Read back every written page and verify contents.
	buf := make([]byte, PageSize)
	for offset, expected := range lastWritten {
		n, err := v.Read(t.Context(), buf, offset)
		require.NoError(t, err)
		require.Equal(t, PageSize, n)
		want := bytes.Repeat([]byte{expected}, PageSize)
		require.Equal(t, want, buf, "data mismatch at offset %d", offset)
	}
}

// TestConcurrentFreezeMemtable exercises the retry path in freezememtable
// where multiple goroutines race to freeze simultaneously. With a tiny
// memtable and no periodic flush, concurrent writers force repeated
// freeze+flush cycles that stress the lock handoff.
func TestConcurrentFreezeMemtable(t *testing.T) {
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1, // no periodic flush — writers do all freezing
	}

	m := &Manager{ObjectStore: store, config: cfg}
	t.Cleanup(func() { _ = m.Close() })

	const numWriters = 4
	const writesPerWriter = 50
	totalPages := numWriters * writesPerWriter

	v, err := m.NewVolume(CreateParams{Volume: "concurrent-freeze", Size: uint64(totalPages+10) * PageSize})
	require.NoError(t, err)

	// Each writer gets its own page range so we can verify all data.
	var wg sync.WaitGroup
	for w := range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range writesPerWriter {
				pageIdx := w*writesPerWriter + i
				page := bytes.Repeat([]byte{byte(pageIdx%255 + 1)}, PageSize)
				err := v.Write(page, uint64(pageIdx)*PageSize)
				if err != nil {
					t.Errorf("writer %d write %d: %v", w, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	require.NoError(t, v.Flush())

	// Verify all pages.
	buf := make([]byte, PageSize)
	for i := range totalPages {
		n, err := v.Read(t.Context(), buf, uint64(i)*PageSize)
		require.NoError(t, err)
		require.Equal(t, PageSize, n)
		want := bytes.Repeat([]byte{byte(i%255 + 1)}, PageSize)
		require.Equal(t, want, buf, "data mismatch at page %d", i)
	}
}
