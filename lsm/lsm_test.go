package lsm

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/semistrict/loophole"
)

// newTestManager creates a Manager and registers cleanup to close it when the test finishes.
func newTestManager(t *testing.T, store loophole.ObjectStore, config Config) *Manager {
	t.Helper()
	m := NewManager(store, t.TempDir(), config, nil, nil, nil)
	t.Cleanup(func() { m.Close(t.Context()) })
	return m
}

var testConfig = Config{
	FlushThreshold:  16 * PageSize,
	MaxFrozenLayers: 2,
	PageCacheBytes:  16 * PageSize,
}

// testManager creates a Manager backed by a MemStore for testing.
// The manager is automatically closed when the test finishes.
func testManager(t *testing.T) *Manager {
	t.Helper()
	return newTestManager(t, loophole.NewMemStore(), testConfig)
}

func TestMemLayerPutGet(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemLayer(OSLocalFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ml.cleanup()

	// Write a page.
	var page [PageSize]byte
	for i := range page {
		page[i] = 0xAB
	}
	if err := ml.put(42, 0, page[:]); err != nil {
		t.Fatal(err)
	}

	// Read it back.
	entry, ok := ml.get(42)
	if !ok {
		t.Fatal("page 42 not found")
	}
	data, err := ml.readData(entry)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, page[:]) {
		t.Fatal("page data mismatch")
	}

	// Non-existent page.
	_, ok = ml.get(99)
	if ok {
		t.Fatal("page 99 should not exist")
	}
}

func TestMemLayerTombstone(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemLayer(OSLocalFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ml.cleanup()

	// Write then tombstone.
	page := make([]byte, PageSize)
	page[0] = 0xFF
	if err := ml.put(10, 0, page); err != nil {
		t.Fatal(err)
	}
	if err := ml.putTombstone(10, 1); err != nil {
		t.Fatal(err)
	}

	entry, ok := ml.get(10)
	if !ok {
		t.Fatal("page 10 not found")
	}
	if !entry.tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestMemLayerFreeze(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemLayer(OSLocalFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ml.cleanup()

	page := make([]byte, PageSize)
	if err := ml.put(1, 0, page); err != nil {
		t.Fatal(err)
	}
	ml.freeze(1)

	// Writes should fail after freeze.
	if err := ml.put(2, 1, page); err == nil {
		t.Fatal("expected error writing to frozen memlayer")
	}
}

func TestDeltaLayerRoundtrip(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemLayer(OSLocalFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ml.cleanup()

	// Write several pages.
	pages := make(map[uint64][]byte)
	for i := uint64(0); i < 10; i++ {
		page := make([]byte, PageSize)
		rand.Read(page)
		pages[i] = page
		if err := ml.put(i, i, page); err != nil {
			t.Fatal(err)
		}
	}
	// Add a tombstone.
	if err := ml.putTombstone(5, 10); err != nil {
		t.Fatal(err)
	}
	ml.freeze(11)

	entries := ml.entries()
	data, meta, err := buildDeltaLayer(ml, entries)
	if err != nil {
		t.Fatal(err)
	}
	if meta.StartSeq != 0 {
		t.Fatalf("expected StartSeq=0, got %d", meta.StartSeq)
	}
	if meta.EndSeq != 11 {
		t.Fatalf("expected EndSeq=11, got %d", meta.EndSeq)
	}

	// Parse it back.
	parsed, err := parseDeltaLayer(data)
	if err != nil {
		t.Fatal(err)
	}

	// Read each page back.
	for addr, expected := range pages {
		if addr == 5 {
			// Tombstoned — should read as zeros.
			got, found, err := parsed.findPage(t.Context(), addr, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("page %d not found", addr)
			}
			if !bytes.Equal(got, make([]byte, PageSize)) {
				t.Fatalf("tombstoned page %d should be zeros", addr)
			}
			continue
		}
		got, found, err := parsed.findPage(t.Context(), addr, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("page %d not found", addr)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("page %d data mismatch", addr)
		}
	}

	// Non-existent page.
	_, found, err := parsed.findPage(t.Context(), 999, 100)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("page 999 should not be found")
	}
}

func TestWriteFlushRead(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write data across multiple pages.
	data := make([]byte, PageSize*3)
	rand.Read(data)
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Read back before flush — should come from memLayer.
	buf := make([]byte, len(data))
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("read %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch before flush")
	}

	// Flush to S3.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Read back after flush — should come from delta layer.
	buf2 := make([]byte, len(data))
	n, err = v.Read(ctx, buf2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("read %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(buf2, data) {
		t.Fatal("data mismatch after flush")
	}
}

func TestWriteFlushReopenRead(t *testing.T) {
	store := loophole.NewMemStore()
	cacheDir := t.TempDir()
	config := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Write and flush with first manager.
	m1 := NewManager(store, cacheDir, config, nil, nil, nil)
	t.Cleanup(func() { m1.Close(t.Context()) })
	v1, err := m1.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize*2)
	rand.Read(data)
	if err := v1.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen with a new manager (simulating process restart).
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	n, err := v2.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("read %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch after reopen")
	}

	if err := m2.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCloseFlushesWithoutExplicitFlush(t *testing.T) {
	store := loophole.NewMemStore()
	config := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Write data and close WITHOUT calling Flush.
	m1 := NewManager(store, t.TempDir(), config, nil, nil, nil)
	v1, err := m1.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize*2)
	rand.Read(data)
	if err := v1.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	// No explicit Flush — Close must flush dirty data.
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify data survived.
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	n, err := v2.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("read %d bytes, expected %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch after close without explicit flush")
	}
}

func TestPartialPageWrite(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write at an offset within a page.
	data := []byte("hello world")
	offset := uint64(100)
	if err := v.Write(ctx, data, offset); err != nil {
		t.Fatal(err)
	}

	// Read the full page — should be zeros except where we wrote.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}

	// Bytes before our write should be zero.
	for i := 0; i < int(offset); i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d should be 0, got %d", i, buf[i])
		}
	}

	// Our written data.
	if !bytes.Equal(buf[offset:offset+uint64(len(data))], data) {
		t.Fatal("written data mismatch")
	}

	// Bytes after our write should be zero.
	for i := int(offset) + len(data); i < PageSize; i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d should be 0, got %d", i, buf[i])
		}
	}
}

func TestPunchHole(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write two pages.
	data := make([]byte, PageSize*2)
	rand.Read(data)
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole covering the first page exactly.
	if err := v.PunchHole(ctx, 0, PageSize); err != nil {
		t.Fatal(err)
	}

	// First page should be zeros.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if !bytes.Equal(buf, make([]byte, PageSize)) {
		t.Fatal("punched page should be zeros")
	}

	// Second page should be untouched.
	n, err = v.Read(ctx, buf, PageSize)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if !bytes.Equal(buf, data[PageSize:]) {
		t.Fatal("second page should be untouched")
	}
}

func TestPunchHoleSubPage(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write one full page of 'X'.
	data := bytes.Repeat([]byte("X"), PageSize)
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole covering only the first 4096 bytes (sub-page).
	if err := v.PunchHole(ctx, 0, 4096); err != nil {
		t.Fatal(err)
	}

	// Read back full page.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}

	// First 4096 bytes should be zeros.
	if !bytes.Equal(buf[:4096], make([]byte, 4096)) {
		t.Fatalf("first 4096 bytes should be zeros after sub-page punch, got first byte: 0x%x", buf[0])
	}
	// Rest should still be 'X'.
	if !bytes.Equal(buf[4096:], bytes.Repeat([]byte("X"), PageSize-4096)) {
		t.Fatal("remaining bytes should be untouched")
	}
}

func TestPunchHoleFlushBetweenWriteAndPunch(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write one page of 'Y'.
	data := bytes.Repeat([]byte("Y"), PageSize)
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Flush write to S3.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Punch hole.
	if err := v.PunchHole(ctx, 0, PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush tombstone to S3.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Read back: should be zeros.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if !bytes.Equal(buf, make([]byte, PageSize)) {
		t.Fatalf("punched page should be zeros, got first byte: 0x%x", buf[0])
	}
}

func TestPunchHoleAfterFlush(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write one page of 'X'.
	data := bytes.Repeat([]byte("X"), PageSize)
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole.
	if err := v.PunchHole(ctx, 0, PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush to S3 (moves tombstone from memLayer to delta layer).
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Read back: should be zeros.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if !bytes.Equal(buf, make([]byte, PageSize)) {
		t.Fatalf("punched page should be zeros after flush, got first byte: %x", buf[0])
	}
}

func TestSnapshotThenFlush(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write 5 pages to fill the memLayer with some data.
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		if err := v.Write(ctx, data, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot freezes the memLayer.
	if err := v.Snapshot(ctx, "snap"); err != nil {
		t.Fatal(err)
	}

	// Flush should succeed — the frozen memLayer's ephemeral file should be readable.
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("flush after snapshot failed: %v", err)
	}

	// Read back to verify data.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if !bytes.Equal(buf, bytes.Repeat([]byte{'A'}, PageSize)) {
		t.Fatalf("unexpected first byte: 0x%x", buf[0])
	}
}

func TestPageCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.cache")
	pc, err := NewPageCache(path, PageSize*3) // room for 3 pages
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	// Put and get.
	data := make([]byte, PageSize)
	data[0] = 0x42
	pc.Put("tl1", 0, data)

	got := pc.Get("tl1", 0)
	if got == nil {
		t.Fatal("expected cached page")
	}
	if got[0] != 0x42 {
		t.Fatalf("expected 0x42, got 0x%02x", got[0])
	}

	// Different timeline should miss.
	if pc.Get("tl2", 0) != nil {
		t.Fatal("expected cache miss for different timeline")
	}

	// Fill up and evict — with random eviction, we can't predict which page
	// gets evicted, but after inserting 4 pages into a 3-slot cache, exactly
	// one of the first three must be gone.
	pc.Put("tl1", 1, data)
	pc.Put("tl1", 2, data)
	pc.Put("tl1", 3, data) // should evict one random page

	// Page 3 must be present (just inserted).
	if pc.Get("tl1", 3) == nil {
		t.Fatal("page 3 should be cached")
	}

	// Exactly one of 0,1,2 should have been evicted.
	evicted := 0
	for _, addr := range []uint64{0, 1, 2} {
		if pc.Get("tl1", addr) == nil {
			evicted++
		}
	}
	if evicted != 1 {
		t.Fatalf("expected exactly 1 eviction, got %d", evicted)
	}
}

func TestParseDeltaKey(t *testing.T) {
	meta, err := parseDeltaKey("deltas/0000000000000000-000000000000000a", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if meta.StartSeq != 0 {
		t.Fatalf("expected StartSeq=0, got %d", meta.StartSeq)
	}
	if meta.EndSeq != 10 {
		t.Fatalf("expected EndSeq=10, got %d", meta.EndSeq)
	}
	if meta.Size != 1234 {
		t.Fatalf("expected Size=1234, got %d", meta.Size)
	}
}

func TestMultipleFlushes(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write, flush, write more, flush again.
	data1 := make([]byte, PageSize)
	rand.Read(data1)
	if err := v.Write(ctx, data1, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, PageSize)
	rand.Read(data2)
	if err := v.Write(ctx, data2, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Read both pages.
	buf := make([]byte, PageSize*2)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize*2 {
		t.Fatalf("read %d bytes, expected %d", n, PageSize*2)
	}
	if !bytes.Equal(buf[:PageSize], data1) {
		t.Fatal("page 0 mismatch")
	}
	if !bytes.Equal(buf[PageSize:], data2) {
		t.Fatal("page 1 mismatch")
	}
}

func TestSnapshot(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write data to parent.
	data := make([]byte, PageSize)
	data[0] = 0xAA
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Snapshot.
	if err := v.Snapshot(ctx, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Write more to parent after snapshot.
	data[0] = 0xBB
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Parent should see the new data.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xBB {
		t.Fatalf("parent: expected 0xBB, got 0x%02x", buf[0])
	}
}

func TestClone(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write data.
	data := make([]byte, PageSize)
	data[0] = 0xCC
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Clone.
	clone, err := v.Clone(ctx, "clone1")
	if err != nil {
		t.Fatal(err)
	}

	// Clone should see parent's data via ancestor.
	buf := make([]byte, PageSize)
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("clone: expected 0xCC, got 0x%02x", buf[0])
	}

	// Write to clone should not affect parent.
	data[0] = 0xDD
	if err := clone.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// Verify parent unchanged.
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("parent after clone write: expected 0xCC, got 0x%02x", buf[0])
	}

	// Verify clone has new data.
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xDD {
		t.Fatalf("clone: expected 0xDD, got 0x%02x", buf[0])
	}
}

func TestImageLayerRoundtrip(t *testing.T) {
	pages := make([]imagePageInput, 5)
	for i := range pages {
		pages[i].PageAddr = uint64(i * 3) // non-contiguous
		pages[i].Data = make([]byte, PageSize)
		rand.Read(pages[i].Data)
	}

	data, meta, err := buildImageLayer(42, pages)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Seq != 42 {
		t.Fatalf("expected Seq=42, got %d", meta.Seq)
	}
	if meta.PageRange[0] != 0 || meta.PageRange[1] != 12 {
		t.Fatalf("expected PageRange=[0,12], got %v", meta.PageRange)
	}

	parsed, err := parseImageLayer(data)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pages {
		got, found, err := parsed.findPage(t.Context(), p.PageAddr)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("page %d not found", p.PageAddr)
		}
		if !bytes.Equal(got, p.Data) {
			t.Fatalf("page %d data mismatch", p.PageAddr)
		}
	}

	// Non-existent page.
	_, found, err := parsed.findPage(t.Context(), 999)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("page 999 should not exist")
	}
}

func TestCompaction(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write and flush multiple times to create several delta layers.
	data1 := make([]byte, PageSize)
	data1[0] = 0x11
	if err := v.Write(ctx, data1, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, PageSize)
	data2[0] = 0x22
	if err := v.Write(ctx, data2, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Overwrite page 0.
	data3 := make([]byte, PageSize)
	data3[0] = 0x33
	if err := v.Write(ctx, data3, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have 3 delta layers.
	vol.mu.RLock()
	numDeltas := len(vol.timeline.layers.Deltas)
	vol.mu.RUnlock()
	if numDeltas != 3 {
		t.Fatalf("expected 3 delta layers, got %d", numDeltas)
	}

	// Compact.
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have 0 deltas and 1 image.
	vol.mu.RLock()
	numDeltas = len(vol.timeline.layers.Deltas)
	numImages := len(vol.timeline.layers.Images)
	vol.mu.RUnlock()
	if numDeltas != 0 {
		t.Fatalf("expected 0 delta layers after compaction, got %d", numDeltas)
	}
	if numImages != 1 {
		t.Fatalf("expected 1 image layer after compaction, got %d", numImages)
	}

	// Data should be correct: page 0 = 0x33, page 1 = 0x22.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x33 {
		t.Fatalf("page 0: expected 0x33, got 0x%02x", buf[0])
	}

	if _, err := v.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x22 {
		t.Fatalf("page 1: expected 0x22, got 0x%02x", buf[0])
	}
}

func TestOverwriteAfterFlush(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write page 0, flush, overwrite page 0, read.
	data1 := make([]byte, PageSize)
	data1[0] = 0x11
	if err := v.Write(ctx, data1, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, PageSize)
	data2[0] = 0x22
	if err := v.Write(ctx, data2, 0); err != nil {
		t.Fatal(err)
	}

	// Should read the overwritten data (from memLayer, not delta layer).
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if buf[0] != 0x22 {
		t.Fatalf("expected 0x22, got 0x%02x", buf[0])
	}
}

// TestSnapshotReadFromDifferentNode opens the snapshot volume fresh
// (simulating a different node) and verifies data is readable via ancestor.
func TestSnapshotReadFromDifferentNode(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Node A: create volume, write data, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0xAA
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := v.Snapshot(ctx, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Node B: open the snapshot and read data via ancestor chain.
	mB := newTestManager(t, store, cfg)
	snapVol, err := mB.OpenVolume(ctx, "snap1")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, PageSize)
	if _, err := snapVol.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xAA {
		t.Fatalf("snapshot on node B: expected 0xAA, got 0x%02x", buf[0])
	}
}

// TestSnapshotOfSnapshotRead creates a snapshot of a snapshot and verifies
// data is readable through a 2-level ancestor chain.
func TestSnapshotOfSnapshotRead(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	// Create volume, write, flush.
	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0x11
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Snapshot parent -> snap1
	if err := v.Snapshot(ctx, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Open snap1 as writable clone so we can write to it.
	snap1, err := v.Clone(ctx, "snap1-clone")
	if err != nil {
		t.Fatal(err)
	}

	// Write to snap1-clone, flush.
	data[0] = 0x22
	if err := snap1.Write(ctx, data, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := snap1.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Snapshot snap1-clone -> snap2
	if err := snap1.Snapshot(ctx, "snap2"); err != nil {
		t.Fatal(err)
	}

	// Open snap2 on a fresh manager (different node).
	m2 := newTestManager(t, store, cfg)
	snap2, err := m2.OpenVolume(ctx, "snap2")
	if err != nil {
		t.Fatal(err)
	}

	// snap2 should see page 0 = 0x11 (from grandparent) via ancestor chain.
	buf := make([]byte, PageSize)
	if _, err := snap2.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x11 {
		t.Fatalf("snap2 page 0: expected 0x11, got 0x%02x", buf[0])
	}

	// snap2 should see page 1 = 0x22 (from snap1-clone).
	if _, err := snap2.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x22 {
		t.Fatalf("snap2 page 1: expected 0x22, got 0x%02x", buf[0])
	}
}

// TestCompactThenSnapshotRead tests that compaction on a parent timeline
// doesn't break reads from child snapshots.
func TestCompactThenSnapshotRead(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple pages and flush multiple times.
	for i := byte(0); i < 5; i++ {
		data := make([]byte, PageSize)
		data[0] = i + 1
		if err := v.Write(ctx, data, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot before compaction.
	if err := v.Snapshot(ctx, "snap-pre"); err != nil {
		t.Fatal(err)
	}

	// Compact the parent.
	vol := v.(*volume)
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Snapshot after compaction.
	if err := v.Snapshot(ctx, "snap-post"); err != nil {
		t.Fatal(err)
	}

	// Open both snapshots on a fresh manager.
	m2 := newTestManager(t, store, cfg)

	snapPre, err := m2.OpenVolume(ctx, "snap-pre")
	if err != nil {
		t.Fatal(err)
	}
	snapPost, err := m2.OpenVolume(ctx, "snap-post")
	if err != nil {
		t.Fatal(err)
	}

	// Both snapshots should see all 5 pages.
	for i := byte(0); i < 5; i++ {
		buf := make([]byte, PageSize)
		if _, err := snapPre.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("snap-pre page %d read: %v", i, err)
		}
		if buf[0] != i+1 {
			t.Fatalf("snap-pre page %d: expected 0x%02x, got 0x%02x", i, i+1, buf[0])
		}

		if _, err := snapPost.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("snap-post page %d read: %v", i, err)
		}
		if buf[0] != i+1 {
			t.Fatalf("snap-post page %d: expected 0x%02x, got 0x%02x", i, i+1, buf[0])
		}
	}
}

// TestCloneOfSnapshotFromDifferentNode verifies clone of snapshot works
// when opened on a different node.
func TestCloneOfSnapshotFromDifferentNode(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Node A: create, write, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0xCC
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Snapshot(ctx, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Node B: open snap1, clone it, read from clone.
	mB := newTestManager(t, store, cfg)
	snapVol, err := mB.OpenVolume(ctx, "snap1")
	if err != nil {
		t.Fatal(err)
	}

	cloneVol, err := snapVol.Clone(ctx, "snap1-clone")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, PageSize)
	if _, err := cloneVol.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("clone of snap: expected 0xCC, got 0x%02x", buf[0])
	}
}

// TestCompactParentThenReadChild verifies that compacting the parent
// doesn't break existing child reads when the child is opened later.
func TestCompactParentThenReadChild(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write pages 0-2, flush.
	for i := byte(0); i < 3; i++ {
		data := make([]byte, PageSize)
		data[0] = 0x10 + i
		if err := v.Write(ctx, data, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Create snapshot (child).
	if err := v.Snapshot(ctx, "child-snap"); err != nil {
		t.Fatal(err)
	}

	// Write more to parent, flush.
	data := make([]byte, PageSize)
	data[0] = 0xFF
	if err := v.Write(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Compact parent — replaces delta layers with image layer.
	vol := v.(*volume)
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Open child snapshot on fresh manager — ancestor will load compacted parent.
	m2 := newTestManager(t, store, cfg)
	child, err := m2.OpenVolume(ctx, "child-snap")
	if err != nil {
		t.Fatal(err)
	}

	// Child should see the ORIGINAL data (before post-snapshot writes),
	// because ancestorSeq limits what's visible.
	buf := make([]byte, PageSize)
	if _, err := child.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x10 {
		t.Fatalf("child page 0: expected 0x10, got 0x%02x", buf[0])
	}
	if _, err := child.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x11 {
		t.Fatalf("child page 1: expected 0x11, got 0x%02x", buf[0])
	}
}

// --- Layer parsing corruption tests ---

// buildTestDeltaLayer creates a valid delta layer blob for corruption tests.
func buildTestDeltaLayer(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	ml, err := newMemLayer(OSLocalFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ml.cleanup()

	page := make([]byte, PageSize)
	page[0] = 0xAB
	if err := ml.put(0, 0, page); err != nil {
		t.Fatal(err)
	}
	ml.freeze(1)

	data, _, err := buildDeltaLayer(ml, ml.entries())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// buildTestImageLayer creates a valid image layer blob for corruption tests.
func buildTestImageLayer(t *testing.T) []byte {
	t.Helper()
	page := make([]byte, PageSize)
	page[0] = 0xCD
	data, _, err := buildImageLayer(1, []imagePageInput{{PageAddr: 0, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseDeltaLayerCorruption(t *testing.T) {
	t.Run("TooSmall", func(t *testing.T) {
		_, err := parseDeltaLayer(make([]byte, 10))
		if err == nil || !strings.Contains(err.Error(), "too small") {
			t.Fatalf("expected 'too small' error, got: %v", err)
		}
	})

	t.Run("BadMagic", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		data[0] = 'X' // corrupt magic
		_, err := parseDeltaLayer(data)
		if err == nil || !strings.Contains(err.Error(), "bad magic") {
			t.Fatalf("expected 'bad magic' error, got: %v", err)
		}
	})

	t.Run("BadVersion", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		binary.LittleEndian.PutUint16(data[4:6], 99) // corrupt version
		_, err := parseDeltaLayer(data)
		if err == nil || !strings.Contains(err.Error(), "unsupported version") {
			t.Fatalf("expected 'unsupported version' error, got: %v", err)
		}
	})

	t.Run("IndexBeyondData", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		// NumEntries is at offset 4+2+8+8+16 = 38
		binary.LittleEndian.PutUint32(data[38:42], 999999)
		_, err := parseDeltaLayer(data)
		if err == nil || !strings.Contains(err.Error(), "index extends beyond") {
			t.Fatalf("expected 'index extends beyond' error, got: %v", err)
		}
	})
}

func TestParseImageLayerCorruption(t *testing.T) {
	t.Run("TooSmall", func(t *testing.T) {
		_, err := parseImageLayer(make([]byte, 10))
		if err == nil || !strings.Contains(err.Error(), "too small") {
			t.Fatalf("expected 'too small' error, got: %v", err)
		}
	})

	t.Run("BadMagic", func(t *testing.T) {
		data := buildTestImageLayer(t)
		data[0] = 'X'
		_, err := parseImageLayer(data)
		if err == nil || !strings.Contains(err.Error(), "bad magic") {
			t.Fatalf("expected 'bad magic' error, got: %v", err)
		}
	})

	t.Run("IndexBeyondData", func(t *testing.T) {
		data := buildTestImageLayer(t)
		// NumPages is at offset 4+2+8+16 = 30
		binary.LittleEndian.PutUint32(data[30:34], 999999)
		_, err := parseImageLayer(data)
		if err == nil || !strings.Contains(err.Error(), "index extends beyond") {
			t.Fatalf("expected 'index extends beyond' error, got: %v", err)
		}
	})
}

func TestParseDeltaKeyErrors(t *testing.T) {
	t.Run("BadFormat", func(t *testing.T) {
		_, err := parseDeltaKey("deltas/nohyphen", 0)
		if err == nil {
			t.Fatal("expected error for bad format")
		}
	})

	t.Run("BadStartSeq", func(t *testing.T) {
		_, err := parseDeltaKey("deltas/zzzz-0000000000000000", 0)
		if err == nil {
			t.Fatal("expected error for bad start seq")
		}
	})

	t.Run("BadEndSeq", func(t *testing.T) {
		_, err := parseDeltaKey("deltas/0000000000000000-zzzz", 0)
		if err == nil {
			t.Fatal("expected error for bad end seq")
		}
	})
}

// --- findPage corruption tests ---

func TestDeltaFindPageCorruption(t *testing.T) {
	t.Run("CRCMismatch", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		parsed, err := parseDeltaLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		// Flip a byte in the compressed data region (right after the header).
		parsed.data[deltaLayerHeaderSize] ^= 0xFF
		_, _, err = parsed.findPage(t.Context(), 0, 100)
		if err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
			t.Fatalf("expected 'CRC mismatch' error, got: %v", err)
		}
	})

	t.Run("ValueBeyondData", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		parsed, err := parseDeltaLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		parsed.index[0].ValueLen = 999999
		_, _, err = parsed.findPage(t.Context(), 0, 100)
		if err == nil || !strings.Contains(err.Error(), "value extends beyond") {
			t.Fatalf("expected 'value extends beyond' error, got: %v", err)
		}
	})

	t.Run("BadDecompressedSize", func(t *testing.T) {
		data := buildTestDeltaLayer(t)
		parsed, err := parseDeltaLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		// Replace compressed data with zstd of wrong-size payload.
		enc, _ := zstd.NewWriter(nil)
		wrongSize := enc.EncodeAll(make([]byte, 100), nil)
		enc.Close()

		ie := &parsed.index[0]
		// Write the wrong-size compressed data at the original offset.
		copy(parsed.data[ie.ValueOffset:], wrongSize)
		ie.ValueLen = uint32(len(wrongSize))
		// Fix CRC to pass the CRC check.
		ie.CRC32 = crc32.ChecksumIEEE(wrongSize)

		_, _, err = parsed.findPage(t.Context(), 0, 100)
		if err == nil || !strings.Contains(err.Error(), "decompressed size") {
			t.Fatalf("expected 'decompressed size' error, got: %v", err)
		}
	})
}

func TestImageFindPageCorruption(t *testing.T) {
	t.Run("CRCMismatch", func(t *testing.T) {
		data := buildTestImageLayer(t)
		parsed, err := parseImageLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		parsed.data[imageLayerHeaderSize] ^= 0xFF
		_, _, err = parsed.findPage(t.Context(), 0)
		if err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
			t.Fatalf("expected 'CRC mismatch' error, got: %v", err)
		}
	})

	t.Run("ValueBeyondData", func(t *testing.T) {
		data := buildTestImageLayer(t)
		parsed, err := parseImageLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		parsed.index[0].ValueLen = 999999
		_, _, err = parsed.findPage(t.Context(), 0)
		if err == nil || !strings.Contains(err.Error(), "value extends beyond") {
			t.Fatalf("expected 'value extends beyond' error, got: %v", err)
		}
	})

	t.Run("BadDecompressedSize", func(t *testing.T) {
		data := buildTestImageLayer(t)
		parsed, err := parseImageLayer(data)
		if err != nil {
			t.Fatal(err)
		}
		enc, _ := zstd.NewWriter(nil)
		wrongSize := enc.EncodeAll(make([]byte, 100), nil)
		enc.Close()

		ie := &parsed.index[0]
		copy(parsed.data[ie.ValueOffset:], wrongSize)
		ie.ValueLen = uint32(len(wrongSize))
		ie.CRC32 = crc32.ChecksumIEEE(wrongSize)

		_, _, err = parsed.findPage(t.Context(), 0)
		if err == nil || !strings.Contains(err.Error(), "decompressed size") {
			t.Fatalf("expected 'decompressed size' error, got: %v", err)
		}
	})
}

// --- Volume read-only guard tests ---

func TestVolumeReadOnlyGuards(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	src, err := m.NewVolume(ctx, "src", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data then freeze.
	page := make([]byte, PageSize)
	page[0] = 0x42
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Freeze(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("Write", func(t *testing.T) {
		err := v.Write(ctx, page, 0)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("expected read-only error, got: %v", err)
		}
	})

	t.Run("PunchHole", func(t *testing.T) {
		err := v.PunchHole(ctx, 0, PageSize)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("expected read-only error, got: %v", err)
		}
	})

	t.Run("CopyFrom", func(t *testing.T) {
		_, err := v.CopyFrom(ctx, src, 0, 0, PageSize)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("expected read-only error, got: %v", err)
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		err := v.Snapshot(ctx, "snap")
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("expected read-only error, got: %v", err)
		}
	})

	t.Run("FreezeIdempotent", func(t *testing.T) {
		if err := v.Freeze(ctx); err != nil {
			t.Fatalf("freeze on already-frozen volume should be nil, got: %v", err)
		}
	})

	t.Run("ReadStillWorks", func(t *testing.T) {
		buf := make([]byte, PageSize)
		if _, err := v.Read(ctx, buf, 0); err != nil {
			t.Fatal(err)
		}
		if buf[0] != 0x42 {
			t.Fatalf("expected 0x42, got 0x%02x", buf[0])
		}
	})
}

// --- Manager edge case tests ---

func TestNewVolumeDuplicateName(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "vol", 1024*1024); err != nil {
		t.Fatal(err)
	}
	_, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestNewVolumeDefaultSize(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.Size() != DefaultVolumeSize {
		t.Fatalf("expected default size %d, got %d", DefaultVolumeSize, v.Size())
	}
}

func TestOpenVolumeCacheHit(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v1, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := m.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	// Should be the exact same pointer.
	if v1 != v2 {
		t.Fatal("OpenVolume should return cached volume")
	}
}

func TestOpenVolumeNotFound(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	_, err := m.OpenVolume(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestDeleteVolumeWhileOpen(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "vol", 1024*1024); err != nil {
		t.Fatal(err)
	}
	err := m.DeleteVolume(ctx, "vol")
	if err == nil || !strings.Contains(err.Error(), "is open") {
		t.Fatalf("expected 'is open' error, got: %v", err)
	}
}

func TestDeleteVolumeThenList(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	// Close the volume so it can be deleted.
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	if err := m.DeleteVolume(ctx, "vol"); err != nil {
		t.Fatal(err)
	}

	names, err := m.ListAllVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name == "vol" {
			t.Fatal("deleted volume should not appear in ListAllVolumes")
		}
	}
}

func TestVolumesAndGetVolume(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "a", 1024*1024); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "b", 1024*1024); err != nil {
		t.Fatal(err)
	}

	names := m.Volumes()
	sort.Strings(names)
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("expected [a, b], got %v", names)
	}

	if m.GetVolume("a") == nil {
		t.Fatal("GetVolume('a') should not be nil")
	}
	if m.GetVolume("nonexistent") != nil {
		t.Fatal("GetVolume('nonexistent') should be nil")
	}
}

func TestManagerClose(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "v1", 1024*1024); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "v2", 1024*1024); err != nil {
		t.Fatal(err)
	}

	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes()) != 0 {
		t.Fatal("Volumes() should be empty after Close")
	}
}

// --- Compact edge case tests ---

func TestCompactAllZeroPages(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write pages, then punch all of them.
	page := make([]byte, PageSize)
	page[0] = 0xFF
	for i := 0; i < 4; i++ {
		if err := v.Write(ctx, page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := v.PunchHole(ctx, uint64(i)*PageSize, PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Compact should take the all-zero path.
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	vol.mu.RLock()
	numDeltas := len(vol.timeline.layers.Deltas)
	numImages := len(vol.timeline.layers.Images)
	vol.mu.RUnlock()

	if numDeltas != 0 {
		t.Fatalf("expected 0 deltas after compact, got %d", numDeltas)
	}
	if numImages != 0 {
		t.Fatalf("expected 0 images after all-zero compact, got %d", numImages)
	}

	// Read should return zeros.
	buf := make([]byte, PageSize)
	for i := 0; i < 4; i++ {
		if _, err := v.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, make([]byte, PageSize)) {
			t.Fatalf("page %d should be zeros after all-zero compact", i)
		}
	}
}

func TestCompactWithBranchPoints(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write pages and flush.
	page := make([]byte, PageSize)
	page[0] = 0x11
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Create snapshot (branch point).
	if err := v.Snapshot(ctx, "snap"); err != nil {
		t.Fatal(err)
	}

	// Write more after branch point.
	page[0] = 0x22
	if err := v.Write(ctx, page, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	vol.mu.RLock()
	numDeltasBefore := len(vol.timeline.layers.Deltas)
	vol.mu.RUnlock()

	// Compact — should only compact deltas before the branch seq.
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Should still have some deltas (those after branch point).
	vol.mu.RLock()
	numDeltasAfter := len(vol.timeline.layers.Deltas)
	vol.mu.RUnlock()

	if numDeltasAfter >= numDeltasBefore {
		t.Fatalf("compact should have removed some deltas: before=%d after=%d", numDeltasBefore, numDeltasAfter)
	}

	// Data should still be readable from parent.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x22 {
		t.Fatalf("parent page 1: expected 0x22, got 0x%02x", buf[0])
	}

	// Open snapshot on fresh manager and verify.
	store := vol.manager.store
	m2 := newTestManager(t, store, m.config)
	snap, err := m2.OpenVolume(ctx, "snap")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snap.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x11 {
		t.Fatalf("snapshot page 0: expected 0x11, got 0x%02x", buf[0])
	}
}

func TestCompactNoDeltas(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// No writes, just compact — should be a no-op.
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}
}

// --- Flush S3 failure tests ---

func TestFlushPutReaderFail(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write data.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Arm fault on PutReader.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated S3 PUT failure"),
	})

	// Flush should fail.
	err = v.Flush(ctx)
	if err == nil {
		t.Fatal("expected flush to fail with PutReader fault")
	}

	// Clear fault and retry — data in frozen layer should still be flushable.
	store.ClearAllFaults()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Read data back.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xAA {
		t.Fatalf("expected 0xAA, got 0x%02x", buf[0])
	}
}

func TestFlushSaveLayerMapFail(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write data.
	page := make([]byte, PageSize)
	page[0] = 0xBB
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Arm a key-specific fault on PutReader for layers.json only.
	// The delta upload uses "deltas/..." which won't match this key.
	layersKey := "timelines/" + vol.timeline.id + "/layers.json"
	store.SetFault(loophole.OpPutReader, layersKey, loophole.Fault{
		Err: fmt.Errorf("simulated layers.json write failure"),
	})

	// Flush should fail on saveLayerMap (delta upload succeeds, but layers.json fails).
	err = v.Flush(ctx)
	if err == nil {
		t.Fatal("expected flush to fail on saveLayerMap")
	}

	// Clear fault and retry.
	store.ClearAllFaults()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Read data back — should be correct.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xBB {
		t.Fatalf("expected 0xBB, got 0x%02x", buf[0])
	}
}

// --- loadLayerMap fallback tests ---

func TestLoadLayerMapFromListing(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Create, write, flush, compact (to create an image layer).
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Write another page and flush so we have both delta and image layers.
	page2 := make([]byte, PageSize)
	page2[0] = 0xDD
	if err := v.Write(ctx, page2, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Find and delete layers.json from the store.
	tlID := vol.timeline.id
	layersKey := "timelines/" + tlID + "/layers.json"
	if err := store.DeleteObject(ctx, layersKey); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen on new manager — should fall back to S3 listing.
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatalf("open after layers.json delete: %v", err)
	}

	// Verify data from image layer.
	buf := make([]byte, PageSize)
	if _, err := v2.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("expected 0xCC from image layer after fallback, got 0x%02x", buf[0])
	}

	// Verify data from delta layer.
	buf2 := make([]byte, PageSize)
	if _, err := v2.Read(ctx, buf2, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf2[0] != 0xDD {
		t.Fatalf("expected 0xDD from delta layer after fallback, got 0x%02x", buf2[0])
	}
}

func TestLoadLayerMapCorruptJSON(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	// Create, write, flush.
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	vol := v.(*volume)
	tlID := vol.timeline.id
	layersKey := "timelines/" + tlID + "/layers.json"
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Write corrupt bytes to layers.json.
	store.PutBytes(ctx, layersKey, []byte("{corrupt json!!!"))

	// Reopen — should fail with a parse error, NOT silently fall back to listing.
	m2 := newTestManager(t, store, cfg)
	_, err = m2.OpenVolume(ctx, "vol")
	if err == nil {
		t.Fatal("expected error opening volume with corrupt layers.json")
	}
	if !strings.Contains(err.Error(), "layers.json") {
		t.Fatalf("expected error mentioning layers.json, got: %v", err)
	}
}

func TestListAllVolumes(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "alpha", 1024*1024); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "beta", 1024*1024); err != nil {
		t.Fatal(err)
	}

	names, err := m.ListAllVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("expected [alpha, beta], got %v", names)
	}
}

// --- Config defaults ---

func TestConfigDefaults(t *testing.T) {
	store := loophole.NewMemStore()
	m := NewManager(store, t.TempDir(), Config{}, nil, nil, nil)
	t.Cleanup(func() { m.Close(t.Context()) })
	if m.config.FlushThreshold != DefaultFlushThreshold {
		t.Fatalf("expected FlushThreshold=%d, got %d", DefaultFlushThreshold, m.config.FlushThreshold)
	}
	if m.config.MaxFrozenLayers != DefaultMaxFrozenLayers {
		t.Fatalf("expected MaxFrozenLayers=%d, got %d", DefaultMaxFrozenLayers, m.config.MaxFrozenLayers)
	}
	if m.config.PageCacheBytes != DefaultPageCacheSize {
		t.Fatalf("expected PageCacheBytes=%d, got %d", DefaultPageCacheSize, m.config.PageCacheBytes)
	}
}

// --- AcquireRef / ReleaseRef ---

func TestAcquireRefAndRelease(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// AcquireRef on a live volume should succeed.
	if err := v.AcquireRef(); err != nil {
		t.Fatalf("AcquireRef on live volume: %v", err)
	}

	// Now refs=2. Release once → refs=1 (no destroy).
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	// Volume should still be usable.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatalf("read after first ReleaseRef: %v", err)
	}

	// Release again → refs=0 → destroy.
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	// Volume should now be closed — AcquireRef should fail.
	if err := v.AcquireRef(); err == nil {
		t.Fatal("expected error on AcquireRef after close")
	}
}

// --- PageCache edge cases ---

func TestPageCacheMinSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.cache")
	// maxBytes < PageSize should clamp to 1 page.
	pc, err := NewPageCache(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if pc.maxPages != 1 {
		t.Fatalf("expected maxPages=1, got %d", pc.maxPages)
	}

	// Should still work — insert and retrieve.
	data := make([]byte, PageSize)
	data[0] = 0x42
	pc.Put("tl", 0, data)

	got := pc.Get("tl", 0)
	if got == nil || got[0] != 0x42 {
		t.Fatal("expected cached page")
	}
}

func TestPageCachePutUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.cache")
	pc, err := NewPageCache(path, PageSize*2)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	data1 := make([]byte, PageSize)
	data1[0] = 0x11
	pc.Put("tl", 0, data1)

	// Update same key with new data.
	data2 := make([]byte, PageSize)
	data2[0] = 0x22
	pc.Put("tl", 0, data2)

	got := pc.Get("tl", 0)
	if got == nil || got[0] != 0x22 {
		t.Fatalf("expected updated value 0x22, got 0x%02x", got[0])
	}
}

func TestPageCacheEviction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.cache")
	pc, err := NewPageCache(path, PageSize*4) // 4 slots
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	data := make([]byte, PageSize)
	// Fill all 4 slots.
	for i := range 4 {
		data[0] = byte(i)
		pc.Put("tl", uint64(i), data)
	}
	// All 4 should be present.
	for i := range 4 {
		if pc.Get("tl", uint64(i)) == nil {
			t.Fatalf("page %d should be cached", i)
		}
	}

	// Insert one more — exactly one of the 4 should be evicted.
	data[0] = 0xFF
	pc.Put("tl", 100, data)

	if pc.Get("tl", 100) == nil {
		t.Fatal("newly inserted page should be cached")
	}

	evicted := 0
	for i := range 4 {
		if pc.Get("tl", uint64(i)) == nil {
			evicted++
		}
	}
	if evicted != 1 {
		t.Fatalf("expected exactly 1 eviction, got %d", evicted)
	}
}

func TestPageCacheDeleteThenReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.cache")
	pc, err := NewPageCache(path, PageSize*2) // 2 slots
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	data := make([]byte, PageSize)
	data[0] = 0xAA
	pc.Put("tl", 0, data)
	data[0] = 0xBB
	pc.Put("tl", 1, data)

	// Delete page 0 — should free a slot.
	pc.Delete("tl", 0)
	if pc.Get("tl", 0) != nil {
		t.Fatal("deleted page should not be cached")
	}

	// Insert page 2 — should use the freed slot, no eviction needed.
	data[0] = 0xCC
	pc.Put("tl", 2, data)

	// Both page 1 and page 2 should be present.
	got1 := pc.Get("tl", 1)
	if got1 == nil || got1[0] != 0xBB {
		t.Fatal("page 1 should still be cached with value 0xBB")
	}
	got2 := pc.Get("tl", 2)
	if got2 == nil || got2[0] != 0xCC {
		t.Fatal("page 2 should be cached with value 0xCC")
	}
}

func TestPageCacheRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.cache")

	// First "process": write data.
	pc1, err := NewPageCache(path, PageSize*4)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, PageSize)
	data[0] = 0xAA
	pc1.Put("tl", 0, data)
	data[0] = 0xBB
	pc1.Put("tl", 1, data)

	// Sanity: data is readable.
	if got := pc1.Get("tl", 0); got == nil || got[0] != 0xAA {
		t.Fatal("expected 0xAA before close")
	}
	pc1.Close()

	// Second "process": reopen at the same path — should start empty.
	pc2, err := NewPageCache(path, PageSize*4)
	if err != nil {
		t.Fatal(err)
	}
	defer pc2.Close()

	if got := pc2.Get("tl", 0); got != nil {
		t.Fatalf("expected cache miss after restart, got data[0]=0x%02x", got[0])
	}
	if got := pc2.Get("tl", 1); got != nil {
		t.Fatalf("expected cache miss after restart, got data[0]=0x%02x", got[0])
	}
}

// --- openVolume concurrent race ---

func TestOpenVolumeConcurrentRace(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	if _, err := m.NewVolume(ctx, "vol", 1024*1024); err != nil {
		t.Fatal(err)
	}
	// Close it so we can reopen concurrently.
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Race two goroutines opening the same volume.
	errs := make(chan error, 2)
	vols := make(chan loophole.Volume, 2)
	for range 2 {
		go func() {
			v, err := m.OpenVolume(ctx, "vol")
			errs <- err
			vols <- v
		}()
	}

	var results []loophole.Volume
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		results = append(results, <-vols)
	}

	// Both should get the same volume object.
	if results[0] != results[1] {
		t.Fatal("concurrent OpenVolume should return the same volume")
	}
}

// ==========================================================================
// S3 fault-injection tests
// ==========================================================================

// testStoreManager creates a Manager with an explicit MemStore returned for fault injection.
func testStoreManager(t *testing.T) (*loophole.MemStore, *Manager) {
	t.Helper()
	store := loophole.NewMemStore()
	return store, newTestManager(t, store, testConfig)
}

// TestNewVolumeMetaFail tests that a PutIfNotExists failure on timeline meta.json
// propagates an error from NewVolume.
func TestNewVolumeMetaFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	store.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
		Err: fmt.Errorf("simulated meta.json failure"),
	})
	_, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err == nil || !strings.Contains(err.Error(), "simulated meta.json failure") {
		t.Fatalf("expected meta.json failure, got: %v", err)
	}
}

// TestNewVolumeRefFail tests that a PutIfNotExists failure on the volume ref
// propagates an error from NewVolume (timeline meta.json succeeds, volume ref fails).
func TestNewVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	// Key-specific fault: only the volume ref write (volumes/vol) fails,
	// not the timeline meta.json write (timelines/<uuid>/meta.json).
	store.SetFault(loophole.OpPutIfNotExists, "volumes/vol", loophole.Fault{
		Err: fmt.Errorf("simulated volume ref failure"),
	})

	_, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err == nil || !strings.Contains(err.Error(), "simulated volume ref failure") {
		t.Fatalf("expected volume ref failure, got: %v", err)
	}
}

// TestDeleteVolumeS3Fail tests that a DeleteObject failure on the volume ref
// propagates an error from DeleteVolume.
func TestDeleteVolumeS3Fail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	// Close the volume so it can be deleted.
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	store.SetFault(loophole.OpDeleteObject, "", loophole.Fault{
		Err: fmt.Errorf("simulated delete failure"),
	})
	err = m.DeleteVolume(ctx, "vol")
	if err == nil || !strings.Contains(err.Error(), "simulated delete failure") {
		t.Fatalf("expected delete failure, got: %v", err)
	}

	// Clear fault — delete should work.
	store.ClearAllFaults()
	if err := m.DeleteVolume(ctx, "vol"); err != nil {
		t.Fatalf("retry delete failed: %v", err)
	}
}

// TestListAllVolumesFail tests that a ListKeys failure propagates from ListAllVolumes.
func TestListAllVolumesFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	store.SetFault(loophole.OpListKeys, "", loophole.Fault{
		Err: fmt.Errorf("simulated list failure"),
	})
	_, err := m.ListAllVolumes(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated list failure") {
		t.Fatalf("expected list failure, got: %v", err)
	}
}

// TestOpenVolumeRefFail tests that a Get failure when reading the volume ref
// propagates from OpenVolume.
func TestOpenVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	// Create a volume successfully.
	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	// Close so it's not cached.
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault on Get — reading the volume ref fails.
	store.SetFault(loophole.OpGet, "", loophole.Fault{
		Err: fmt.Errorf("simulated get failure"),
	})
	_, err = m.OpenVolume(ctx, "vol")
	if err == nil || !strings.Contains(err.Error(), "simulated get failure") {
		t.Fatalf("expected get failure on OpenVolume, got: %v", err)
	}
}

// TestOpenTimelineMetaFail tests that a Get failure when reading timeline meta.json
// propagates from OpenVolume (volume ref reads fine, but timeline meta fails).
func TestOpenTimelineMetaFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)
	tlID := vol.timeline.id

	// Close so it's not cached.
	if err := v.ReleaseRef(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault on Get for the timeline meta.json specifically.
	metaKey := "timelines/" + tlID + "/meta.json"
	store.SetFault(loophole.OpGet, metaKey, loophole.Fault{
		Err: fmt.Errorf("simulated timeline meta failure"),
	})
	_, err = m.OpenVolume(ctx, "vol")
	if err == nil || !strings.Contains(err.Error(), "simulated timeline meta failure") {
		t.Fatalf("expected timeline meta failure, got: %v", err)
	}
}

// TestWritePartialPageReadFail tests that a read failure during read-modify-write
// (partial page write) propagates from Write.
func TestWritePartialPageReadFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Write a full page first, then flush so readPage goes to S3.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Now fault Get — a partial page write needs to read the existing page.
	store.SetFault(loophole.OpGet, "", loophole.Fault{
		Err: fmt.Errorf("simulated read failure"),
	})

	// Write 1 byte at offset 1 — partial page, triggers read-modify-write.
	err = v.Write(ctx, []byte{0xBB}, 1)
	if err == nil || !strings.Contains(err.Error(), "simulated read failure") {
		t.Fatalf("expected read failure on partial write, got: %v", err)
	}

	// Clear fault — full page writes should still work (no read needed).
	store.ClearAllFaults()
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatalf("full page write after clearing fault: %v", err)
	}
}

// TestGetDeltaLayerFail tests that a Get failure when downloading a delta layer
// propagates through readPage.
func TestGetDeltaLayerFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Open on a fresh manager (empty local cache) so reads must go to S3.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}

	// Fault Get — getDeltaLayer will fail.
	vol2 := v2.(*volume)
	deltaKey := "timelines/" + vol2.timeline.id + "/" + vol2.timeline.layers.Deltas[0].Key
	store.SetFault(loophole.OpGet, deltaKey, loophole.Fault{
		Err: fmt.Errorf("simulated delta download failure"),
	})

	buf := make([]byte, PageSize)
	_, err = v2.Read(ctx, buf, 0)
	if err == nil || !strings.Contains(err.Error(), "simulated delta download failure") {
		t.Fatalf("expected delta download failure, got: %v", err)
	}

	// Clear fault — read should work.
	store.ClearAllFaults()
	_, err = v2.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xDD {
		t.Fatalf("expected 0xDD, got 0x%02x", buf[0])
	}
}

// TestGetImageLayerFail tests that a Get failure on an image layer propagates.
func TestGetImageLayerFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write, flush, compact to create an image layer.
	page := make([]byte, PageSize)
	page[0] = 0xEE
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Open on fresh manager.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	vol2 := v2.(*volume)

	// Fault Get on image layer key.
	imageKey := "timelines/" + vol2.timeline.id + "/" + vol2.timeline.layers.Images[0].Key
	store.SetFault(loophole.OpGet, imageKey, loophole.Fault{
		Err: fmt.Errorf("simulated image download failure"),
	})

	buf := make([]byte, PageSize)
	_, err = v2.Read(ctx, buf, 0)
	if err == nil || !strings.Contains(err.Error(), "simulated image download failure") {
		t.Fatalf("expected image download failure, got: %v", err)
	}

	// Clear and retry.
	store.ClearAllFaults()
	_, err = v2.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xEE {
		t.Fatalf("expected 0xEE, got 0x%02x", buf[0])
	}
}

// TestCompactFlushFail tests that a flush failure at the start of Compact propagates.
func TestCompactFlushFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write to memLayer (don't flush) so Compact's Flush has work to do.
	page := make([]byte, PageSize)
	page[0] = 0x11
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Fault PutReader — flush inside Compact will fail.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated compact flush failure"),
	})
	err = vol.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated compact flush failure") {
		t.Fatalf("expected compact flush failure, got: %v", err)
	}

	// Clear and compact should succeed.
	store.ClearAllFaults()
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatalf("compact retry: %v", err)
	}
}

// TestCompactGetDeltaFail tests that a delta layer download failure during page
// collection in Compact propagates.
func TestCompactGetDeltaFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x22
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Open on a fresh manager so delta layers aren't in memory cache.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	vol2 := v2.(*volume)

	// Fault Get on delta layer — compact's getDeltaLayer will fail.
	deltaKey := "timelines/" + vol2.timeline.id + "/" + vol2.timeline.layers.Deltas[0].Key
	store.SetFault(loophole.OpGet, deltaKey, loophole.Fault{
		Err: fmt.Errorf("simulated compact delta get failure"),
	})
	err = vol2.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated compact delta get failure") {
		t.Fatalf("expected compact delta get failure, got: %v", err)
	}

	// Clear and retry.
	store.ClearAllFaults()
	if err := vol2.timeline.Compact(ctx); err != nil {
		t.Fatalf("compact retry: %v", err)
	}
}

// TestCompactImageUploadFail tests that a PutReader failure on image layer upload
// during Compact propagates.
func TestCompactImageUploadFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	page := make([]byte, PageSize)
	page[0] = 0x33
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Arm a fault on PutReader — Compact's flush is a no-op (already flushed),
	// so PutReader only fires for the image upload.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated image upload failure"),
	})
	// But compact reads meta.json first (which uses Get, not PutReader), and then
	// the flush is a no-op (already flushed). So PutReader only fires for image upload.
	err = vol.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated image upload failure") {
		t.Fatalf("expected image upload failure, got: %v", err)
	}

	// Clear and retry.
	store.ClearAllFaults()
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatalf("compact retry: %v", err)
	}

	// Data still readable.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x33 {
		t.Fatalf("expected 0x33, got 0x%02x", buf[0])
	}
}

// TestCompactSaveLayerMapFail tests that a saveLayerMap failure during Compact
// correctly reverts in-memory state.
func TestCompactSaveLayerMapFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	page := make([]byte, PageSize)
	page[0] = 0x44
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Arm fault on layers.json only.
	layersKey := "timelines/" + vol.timeline.id + "/layers.json"
	store.SetFault(loophole.OpPutReader, layersKey, loophole.Fault{
		Err: fmt.Errorf("simulated compact saveLayerMap failure"),
	})
	err = vol.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated compact saveLayerMap failure") {
		t.Fatalf("expected saveLayerMap failure, got: %v", err)
	}

	// In-memory state should be reverted — deltas should still be present.
	vol.timeline.mu.RLock()
	numDeltas := len(vol.timeline.layers.Deltas)
	vol.timeline.mu.RUnlock()
	if numDeltas == 0 {
		t.Fatal("deltas should be restored after saveLayerMap failure")
	}

	// Clear and retry.
	store.ClearAllFaults()
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatalf("compact retry: %v", err)
	}

	// Data still readable.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0x44 {
		t.Fatalf("expected 0x44, got 0x%02x", buf[0])
	}
}

// TestCompactAllZeroSaveLayerMapFail tests that saveLayerMap failure in the
// all-zero compact path correctly reverts.
func TestCompactAllZeroSaveLayerMapFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write pages then punch all → all zeros.
	page := make([]byte, PageSize)
	page[0] = 0xFF
	for i := 0; i < 4; i++ {
		if err := v.Write(ctx, page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := v.PunchHole(ctx, uint64(i)*PageSize, PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	vol.timeline.mu.RLock()
	numDeltasBefore := len(vol.timeline.layers.Deltas)
	vol.timeline.mu.RUnlock()

	// Arm fault on layers.json.
	layersKey := "timelines/" + vol.timeline.id + "/layers.json"
	store.SetFault(loophole.OpPutReader, layersKey, loophole.Fault{
		Err: fmt.Errorf("simulated all-zero saveLayerMap failure"),
	})
	err = vol.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated all-zero saveLayerMap failure") {
		t.Fatalf("expected all-zero saveLayerMap failure, got: %v", err)
	}

	// In-memory state should be reverted.
	vol.timeline.mu.RLock()
	numDeltasAfter := len(vol.timeline.layers.Deltas)
	vol.timeline.mu.RUnlock()
	if numDeltasAfter != numDeltasBefore {
		t.Fatalf("deltas should be restored: before=%d after=%d", numDeltasBefore, numDeltasAfter)
	}

	// Clear and compact successfully.
	store.ClearAllFaults()
	if err := vol.timeline.Compact(ctx); err != nil {
		t.Fatalf("compact retry: %v", err)
	}
}

// TestSnapshotCreateChildFail tests that a PutIfNotExists failure during
// createChild propagates from Snapshot.
func TestSnapshotCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x55
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault PutIfNotExists — createChild writes child meta.json.
	store.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
		Err: fmt.Errorf("simulated createChild failure"),
	})
	err = v.Snapshot(ctx, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated createChild failure") {
		t.Fatalf("expected createChild failure, got: %v", err)
	}

	// Clear — snapshot should work.
	store.ClearAllFaults()
	if err := v.Snapshot(ctx, "snap"); err != nil {
		t.Fatalf("snapshot retry: %v", err)
	}
}

// TestSnapshotPutVolumeRefFail tests that a PutIfNotExists failure on the
// snapshot's volume ref propagates from Snapshot.
func TestSnapshotPutVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x66
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Key-specific fault: only the snapshot volume ref write (volumes/snap) fails,
	// not the child timeline meta.json write.
	store.SetFault(loophole.OpPutIfNotExists, "volumes/snap", loophole.Fault{
		Err: fmt.Errorf("simulated putVolumeRef failure"),
	})

	err = v.Snapshot(ctx, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated putVolumeRef failure") {
		t.Fatalf("expected putVolumeRef failure, got: %v", err)
	}
}

// TestCreateChildUpdateParentMetaFail tests that a PutBytesCAS failure when
// updating the parent's meta.json during createChild propagates.
func TestCreateChildUpdateParentMetaFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	page := make([]byte, PageSize)
	page[0] = 0x77
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault PutBytesCAS on the parent's meta.json (used by ModifyJSON).
	parentMetaKey := "timelines/" + vol.timeline.id + "/meta.json"
	store.SetFault(loophole.OpPutBytesCAS, parentMetaKey, loophole.Fault{
		Err: fmt.Errorf("simulated parent meta update failure"),
	})

	err = v.Snapshot(ctx, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated parent meta update failure") {
		t.Fatalf("expected parent meta update failure, got: %v", err)
	}
}

// TestCloneCreateChildFail tests that createChild failure propagates from Clone.
func TestCloneCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x88
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault PutIfNotExists for createChild.
	store.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
		Err: fmt.Errorf("simulated clone createChild failure"),
	})
	_, err = v.Clone(ctx, "clone")
	if err == nil || !strings.Contains(err.Error(), "simulated clone createChild failure") {
		t.Fatalf("expected clone createChild failure, got: %v", err)
	}
}

// TestClonePutVolumeRefFail tests that putVolumeRef failure propagates from Clone.
func TestClonePutVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x99
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Key-specific fault: only the clone volume ref write (volumes/clone) fails.
	store.SetFault(loophole.OpPutIfNotExists, "volumes/clone", loophole.Fault{
		Err: fmt.Errorf("simulated clone putVolumeRef failure"),
	})

	_, err = v.Clone(ctx, "clone")
	if err == nil || !strings.Contains(err.Error(), "simulated clone putVolumeRef failure") {
		t.Fatalf("expected clone putVolumeRef failure, got: %v", err)
	}
}

// TestFreezeFlushFail tests that a flush failure inside Freeze propagates.
func TestFreezeFlushFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated freeze flush failure"),
	})
	err = v.Freeze(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated freeze flush failure") {
		t.Fatalf("expected freeze flush failure, got: %v", err)
	}

	// Verify volume is NOT read-only (freeze failed).
	if v.ReadOnly() {
		t.Fatal("volume should not be read-only after failed freeze")
	}

	// Clear and freeze should work.
	store.ClearAllFaults()
	if err := v.Freeze(ctx); err != nil {
		t.Fatalf("freeze retry: %v", err)
	}
	if !v.ReadOnly() {
		t.Fatal("volume should be read-only after successful freeze")
	}
}

// TestOpenAncestorFail tests that an ancestor timeline open failure propagates.
func TestOpenAncestorFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	// Create parent, write, flush, snapshot.
	v, err := m.NewVolume(ctx, "parent", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)
	parentTlID := vol.timeline.id

	page := make([]byte, PageSize)
	page[0] = 0xAB
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Snapshot(ctx, "snap"); err != nil {
		t.Fatal(err)
	}

	// Close everything.
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault Get on the parent's meta.json. When opening the snapshot,
	// it reads the child meta.json first (which has ancestor = parentTlID),
	// then tries to open the ancestor and reads parent's meta.json.
	parentMetaKey := "timelines/" + parentTlID + "/meta.json"
	store.SetFault(loophole.OpGet, parentMetaKey, loophole.Fault{
		Err: fmt.Errorf("simulated ancestor open failure"),
	})

	m2 := newTestManager(t, store, m.config)
	_, err = m2.OpenVolume(ctx, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated ancestor open failure") {
		t.Fatalf("expected ancestor open failure, got: %v", err)
	}
}

// TestLoadLayerMapListKeysFail tests that a ListKeys failure in loadLayerMap
// fallback path results in an empty layer map (no error).
func TestLoadLayerMapListKeysFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// Write, flush, then delete layers.json to force fallback.
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	tlID := vol.timeline.id
	layersKey := "timelines/" + tlID + "/layers.json"
	if err := store.DeleteObject(ctx, layersKey); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault ListKeys so the fallback also fails.
	store.SetFault(loophole.OpListKeys, "", loophole.Fault{
		Err: fmt.Errorf("simulated list keys failure"),
	})

	// Opening should still work — loadLayerMap treats list failure as fresh timeline.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatalf("expected open to succeed with empty layers, got: %v", err)
	}

	// Read should return zeros (no layers).
	buf := make([]byte, PageSize)
	if _, err := v2.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0 {
		t.Fatalf("expected zeros with failed listing, got 0x%02x", buf[0])
	}
}

// TestCompactMetaReadFail tests that a meta.json read failure in Compact
// (reading branch points) propagates.
func TestCompactMetaReadFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize)
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	page := make([]byte, PageSize)
	page[0] = 0x77
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Fault Get on meta.json for this timeline.
	metaKey := "timelines/" + vol.timeline.id + "/meta.json"
	store.SetFault(loophole.OpGet, metaKey, loophole.Fault{
		Err: fmt.Errorf("simulated compact meta read failure"),
	})
	err = vol.timeline.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "simulated compact meta read failure") {
		t.Fatalf("expected compact meta read failure, got: %v", err)
	}
}

// TestWriteBackpressurePreservesData verifies that when a backpressure flush
// fails (S3 fault), the write still lands in the memLayer. This is critical
// because callers (like the simulation oracle) record writes unconditionally
// and expect data to be in the memLayer even when Write returns an error from
// an internal flush.
func TestWriteBackpressurePreservesData(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize, // freeze after every write
		MaxFrozenLayers: 1,
		PageCacheBytes:  PageSize,
		FlushInterval:   -1, // disable periodic flush
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Write page 0 — succeeds and gets flushed.
	page0 := make([]byte, PageSize)
	for i := range page0 {
		page0[i] = 0xAA
	}
	if err := v.Write(ctx, page0, 0); err != nil {
		t.Fatalf("write page 0: %v", err)
	}

	// Step 2: Write page 1 — this triggers freeze+flush; inject fault so
	// the flush fails. The data should still be in a frozen layer.
	page1 := make([]byte, PageSize)
	for i := range page1 {
		page1[i] = 0xBB
	}
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated S3 fault"),
	})
	// Write returns error from auto-flush, but data should be in frozen layer.
	_ = v.Write(ctx, page1, PageSize)

	// Step 3: Write page 2 — frozen layers are at capacity, so backpressure
	// flush kicks in. With the fault still active, this flush fails. The bug
	// was that this prevented the write from entering the memLayer at all.
	page2 := make([]byte, PageSize)
	for i := range page2 {
		page2[i] = 0xCC
	}
	_ = v.Write(ctx, page2, 2*PageSize)

	// Step 4: Clear faults and flush everything.
	store.ClearAllFaults()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Step 5: Verify all three pages are readable with correct data.
	buf := make([]byte, PageSize)

	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatalf("read page 0: %v", err)
	}
	if !bytes.Equal(buf, page0) {
		t.Fatalf("page 0: expected 0xAA, got %x...", buf[:4])
	}

	if _, err := v.Read(ctx, buf, PageSize); err != nil {
		t.Fatalf("read page 1: %v", err)
	}
	if !bytes.Equal(buf, page1) {
		t.Fatalf("page 1: expected 0xBB, got %x...", buf[:4])
	}

	if _, err := v.Read(ctx, buf, 2*PageSize); err != nil {
		t.Fatalf("read page 2: %v", err)
	}
	if !bytes.Equal(buf, page2) {
		t.Fatalf("page 2: expected 0xCC, got %x...", buf[:4])
	}
}
