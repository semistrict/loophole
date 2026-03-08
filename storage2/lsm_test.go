package storage2

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/metrics"
)

func promCounterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.(prometheus.Metric).Write(&m)
	return m.GetCounter().GetValue()
}

// testManager creates a Manager backed by a MemStore for testing.
// The manager is automatically closed when the test finishes.
func testManager(t *testing.T) *Manager {
	t.Helper()
	return newTestManager(t, loophole.NewMemStore(), testConfig)
}

func testStoreManager(t *testing.T) (*loophole.MemStore, *Manager) {
	t.Helper()
	store := loophole.NewMemStore()
	return store, newTestManager(t, store, testConfig)
}

func TestMemLayerPutGet(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemtable(dir, 0, 1024)
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
	ml, err := newMemtable(dir, 0, 1024)
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
	ml, err := newMemtable(dir, 0, 1024)
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

func TestWriteFlushRead(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	// Write and flush with first manager.
	dc1, err := NewPageCache(filepath.Join(cacheDir, "diskcache-1"))
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewVolumeManager(store, cacheDir, config, nil, dc1)
	t.Cleanup(func() { m1.Close(t.Context()) })
	t.Cleanup(func() { dc1.Close() })
	v1, err := m1.NewVolume(ctx, "test", 1024*1024, "")
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

func TestPartialPageWrite(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "parent", 1024*1024, "")
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

func TestMultipleFlushes(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "parent", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "parent", 1024*1024, "")
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

func TestOverwriteAfterFlush(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	// Node A: create volume, write data, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(ctx, "parent", 1024*1024, "")
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	// Create volume, write, flush.
	v, err := m.NewVolume(ctx, "parent", 1024*1024, "")
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(ctx, "parent", 1024*1024, "")
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
	if err := vol.layer.CompactL0(ctx); err != nil {
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	// Node A: create, write, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(ctx, "parent", 1024*1024, "")
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
func TestVolumeReadOnlyGuards(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}
	src, err := m.NewVolume(ctx, "src", 1024*1024, "")
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

	if _, err := m.NewVolume(ctx, "vol", 1024*1024, ""); err != nil {
		t.Fatal(err)
	}
	_, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestNewVolumeDefaultSize(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 0, "")
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

	v1, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	if _, err := m.NewVolume(ctx, "vol", 1024*1024, ""); err != nil {
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
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	if _, err := m.NewVolume(ctx, "a", 1024*1024, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "b", 1024*1024, ""); err != nil {
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

	if _, err := m.NewVolume(ctx, "v1", 1024*1024, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "v2", 1024*1024, ""); err != nil {
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

func TestCompactNoDeltas(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 256*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}
	vol := v.(*volume)

	// No writes, just compact — should be a no-op.
	if err := vol.layer.CompactL0(ctx); err != nil {
		t.Fatal(err)
	}
}

// --- Flush S3 failure tests ---

func TestFlushPutReaderFail(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

func TestLoadLayerMapFromListing(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	// Create, write, flush, compact (to create an image layer).
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(ctx, "vol", 1024*1024, "")
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
	if err := vol.layer.CompactL0(ctx); err != nil {
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
	tlID := vol.layer.id
	layersKey := "layers/" + tlID + "/layers.json"
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

func TestListAllVolumes(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(ctx, "alpha", 1024*1024, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.NewVolume(ctx, "beta", 1024*1024, ""); err != nil {
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

func TestAcquireRefAndRelease(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

func TestNewVolumeMetaFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	store.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
		Err: fmt.Errorf("simulated meta.json failure"),
	})
	_, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	_, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err == nil || !strings.Contains(err.Error(), "simulated volume ref failure") {
		t.Fatalf("expected volume ref failure, got: %v", err)
	}
}

// TestDeleteVolumeS3Fail tests that a DeleteObject failure on the volume ref
// propagates an error from DeleteVolume.
func TestDeleteVolumeS3Fail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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
	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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
func TestWritePartialPageReadFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write a full page first, then flush so the data is on S3.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Close the first manager to release the lease.
	m.Close(ctx)

	// Open on a fresh manager (empty local cache) so reads must go to S3.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}

	// Now fault Get — a partial page write needs to read the existing page.
	store.SetFault(loophole.OpGet, "", loophole.Fault{
		Err: fmt.Errorf("simulated read failure"),
	})

	// Write 1 byte at offset 1 — partial page, triggers read-modify-write.
	err = v2.Write(ctx, []byte{0xBB}, 1)
	if err == nil || !strings.Contains(err.Error(), "simulated read failure") {
		t.Fatalf("expected read failure on partial write, got: %v", err)
	}

	// Clear fault — full page writes should still work (no read needed).
	store.ClearAllFaults()
	if err := v2.Write(ctx, page, 0); err != nil {
		t.Fatalf("full page write after clearing fault: %v", err)
	}
}

// TestGetDeltaLayerFail tests that a Get failure when downloading a delta layer
// propagates through readPage.
func TestSnapshotCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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
func TestCloneCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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
func TestWriteBackpressurePreservesData(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize, // freeze after every write
		MaxFrozenTables: 1,
		FlushInterval:   -1, // disable periodic flush
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	if n, err := v.Read(ctx, buf, 0); err != nil || n != PageSize {
		t.Fatalf("read page 0: n=%d err=%v", n, err)
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

// TestCopyFromAutoFlushFault verifies that CopyFrom data is readable even
// when an auto-flush fails mid-copy due to an S3 fault.
//
// This reproduces a bug where CopyFrom's Write succeeds (data enters the
// memLayer) but auto-flush fails and returns an error. CopyFrom returns
// `copied` without counting the last page, so callers (and the simulation
// oracle) miss it. But the data IS in the memLayer and survives a later Flush.
func TestCopyFromAutoFlushFault(t *testing.T) {
	store := loophole.NewMemStore()

	// Very low flush threshold: 2 pages triggers auto-flush.
	cfg := Config{
		FlushThreshold:  2 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	// Create source volume with 4 pages of known data.
	src, err := m.NewVolume(ctx, "src", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}
	srcPages := make([][]byte, 4)
	for i := range srcPages {
		srcPages[i] = bytes.Repeat([]byte{byte(0xA0 + i)}, PageSize)
		if err := src.Write(ctx, srcPages[i], uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Create destination volume.
	dst, err := m.NewVolume(ctx, "dst", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault so auto-flush during CopyFrom fails.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// CopyFrom 4 pages. The first 1-2 writes go into memLayer without
	// triggering flush. When the memLayer crosses FlushThreshold, the
	// auto-flush fires and hits the PUT fault. CopyFrom returns an error
	// with `copied` not counting the page whose Write triggered the flush.
	copied, err := dst.CopyFrom(ctx, src, 0, 0, 4*PageSize)
	if err == nil {
		t.Fatal("expected CopyFrom to fail due to S3 fault")
	}

	// BUG: CopyFrom reports fewer bytes than actually written to memLayer.
	// The Write that triggered auto-flush DID put the page in the memLayer
	// (the put() call succeeds before the flush), but copied doesn't count it.
	//
	// We expect copied to be at least 2*PageSize (the first 2 pages fit
	// without triggering flush), but the 3rd page's Write succeeds in the
	// memLayer, then auto-flush fails. copied should include the 3rd page
	// but currently doesn't.
	pagesReported := copied / PageSize
	t.Logf("CopyFrom reported %d bytes copied (%d pages), err: %v", copied, pagesReported, err)

	// Clear faults and flush — the data in the memLayer/frozen layers
	// should now persist to S3.
	store.ClearAllFaults()
	if err := dst.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Read ALL pages from dst. Pages that were actually written to the
	// memLayer should be readable even if CopyFrom's `copied` didn't count them.
	var pagesActuallyWritten uint64
	buf := make([]byte, PageSize)
	for i := 0; i < 4; i++ {
		if _, err := dst.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", i, err)
		}
		if bytes.Equal(buf, srcPages[i]) {
			pagesActuallyWritten++
		}
	}

	t.Logf("pages reported by CopyFrom: %d, pages actually written: %d", pagesReported, pagesActuallyWritten)

	// The bug: pagesActuallyWritten > pagesReported.
	// CopyFrom wrote more pages than it reported, which causes the
	// simulation oracle to miss tracking those pages.
	if pagesActuallyWritten > pagesReported {
		t.Fatalf("CopyFrom under-reported: reported %d pages but %d were actually written to memLayer",
			pagesReported, pagesActuallyWritten)
	}
}

// TestCopyFromOracleConsistency is the simulation-level reproduction:
// after CopyFrom + flush failure + later successful flush + reopen,
// the data must match what the oracle would track.
//
// This simulates the exact sequence that caused the sim failure:
//  1. Source has data, destination branches from a timeline with different data
//  2. CopyFrom writes source pages to destination
//  3. Auto-flush fails mid-copy (S3 fault)
//  4. CopyFrom returns `copied` not counting the faulted page
//  5. A later Flush succeeds, persisting all memLayer data
//  6. On reopen, the page has the CopyFrom'd value (zeros from source)
//     but an oracle tracking only `copied` pages would expect the branch-inherited value
func TestCopyFromOracleConsistency(t *testing.T) {
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  2 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	// Create a "parent" volume and write distinct data to pages 0-3.
	parent, err := m.NewVolume(ctx, "parent", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}
	parentData := bytes.Repeat([]byte{0xDD}, PageSize)
	for i := 0; i < 4; i++ {
		if err := parent.Write(ctx, parentData, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := parent.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Clone parent → dst. dst inherits parent's data for pages 0-3.
	dst, err := parent.Clone(ctx, "dst")
	if err != nil {
		t.Fatal(err)
	}

	// Create a separate source volume with zeros on pages 0-3
	// (never written, so they're zeros).
	src, err := m.NewVolume(ctx, "src", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// CopyFrom zeros (src) over dst's inherited data.
	copied, copyErr := dst.CopyFrom(ctx, src, 0, 0, 4*PageSize)
	pagesReported := copied / PageSize
	t.Logf("CopyFrom: copied=%d (%d pages), err=%v", copied, pagesReported, copyErr)

	// Clear faults, flush dst to persist everything in memLayer.
	store.ClearAllFaults()
	if err := dst.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Close and reopen dst on a fresh manager (simulates different node).
	m.Close(ctx)
	m2 := newTestManager(t, store, cfg)
	dst2, err := m2.OpenVolume(ctx, "dst")
	if err != nil {
		t.Fatal(err)
	}

	// Simulate what the oracle would track: only `pagesReported` pages
	// are known to have been overwritten. The rest should still have
	// parent's data (0xDD).
	oracle := NewOracle()
	parentTL := "parent-tl" // placeholder
	dstTL := "dst-tl"

	// Oracle: parent wrote pages 0-3 with 0xDD.
	for i := 0; i < 4; i++ {
		oracle.RecordWrite("node", parentTL, PageIdx(i), parentData)
	}
	oracle.RecordFlush("node", parentTL)

	// Oracle: branch dst from parent.
	oracle.RecordBranch(dstTL, parentTL, 0)

	// Oracle: record only the pages CopyFrom reported (the bug).
	zeroPage := make([]byte, PageSize)
	for pg := uint64(0); pg < pagesReported; pg++ {
		oracle.RecordWrite("node", dstTL, PageIdx(pg), zeroPage)
	}
	oracle.RecordFlush("node", dstTL)

	// Now verify: read each page from dst and check against oracle.
	buf := make([]byte, PageSize)
	for i := 0; i < 4; i++ {
		if _, err := dst2.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", i, err)
		}
		expected := oracle.ExpectedRead("node2", dstTL, PageIdx(i))
		if !bytes.Equal(buf, expected) {
			t.Fatalf("page %d: oracle expected %x... but got %x... (CopyFrom reported %d pages, page %d was not tracked)",
				i, expected[:4], buf[:4], pagesReported, i)
		}
	}
}

// TestPartialWriteAutoFlushFault demonstrates that a sub-page write puts data
// into the memLayer even when auto-flush fails. If the oracle doesn't record
// the write (because Write returned an error), a later successful flush will
// persist data the oracle doesn't know about.
//
// Sequence:
//  1. Create a volume, page 5 starts as zeros
//  2. Set FlushThreshold low so writes trigger auto-flush
//  3. Write a full page to fill up the memLayer near threshold
//  4. Inject PUT fault
//  5. Do a sub-page write to page 5 (which was zeros)
//     -> writePage does read-modify-write: reads zeros, applies partial data, puts in memLayer
//     -> auto-flush fails -> Write returns error
//  6. Clear fault, flush successfully
//  7. Reopen and read page 5: it has non-zero data from the partial write
//     but an oracle that skipped RecordWrite on error would expect zeros
func TestPartialWriteAutoFlushFault(t *testing.T) {
	store := loophole.NewMemStore()

	cfg := Config{
		FlushThreshold:  2 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write a full page to page 0 to put some data in memLayer.
	fullPage := bytes.Repeat([]byte{0xAA}, PageSize)
	if err := v.Write(ctx, fullPage, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Now write page 1 to get near the flush threshold.
	if err := v.Write(ctx, fullPage, PageSize); err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault so auto-flush fails.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// Sub-page write to page 5 at offset 100, length 200.
	// Page 5 has never been written, so it's zeros.
	// writePage will: read zeros, copy partialData at offset 100, put full page in memLayer.
	// Then auto-flush fails -> Write returns error.
	partialData := bytes.Repeat([]byte{0xBB}, 200)
	writeErr := v.Write(ctx, partialData, 5*PageSize+100)
	t.Logf("partial write err: %v", writeErr)

	// The data IS in memLayer despite the error.
	// Clear faults and flush.
	store.ClearAllFaults()
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Close and reopen.
	m.Close(ctx)
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Read page 5. It should have the partial write data.
	buf := make([]byte, PageSize)
	if _, err := v2.Read(ctx, buf, 5*PageSize); err != nil {
		t.Fatal(err)
	}

	// Build expected page: zeros with partialData at offset 100.
	expectedPage := make([]byte, PageSize)
	copy(expectedPage[100:], partialData)

	if !bytes.Equal(buf, expectedPage) {
		t.Fatalf("page 5 mismatch after partial write + auto-flush fault:\n  got[100:104]=%x\n  exp[100:104]=%x\n  got is all zeros: %v",
			buf[100:104], expectedPage[100:104], bytes.Equal(buf, make([]byte, PageSize)))
	}

	// Now simulate the oracle bug: if opPartialWrite skips RecordWrite on error,
	// the oracle would still think page 5 is zeros.
	oracle := NewOracle()
	tlID := "test-tl"

	// Oracle: page 0 was written with 0xAA.
	oracle.RecordWrite("node", tlID, 0, fullPage)
	oracle.RecordFlush("node", tlID)

	// Oracle: page 1 was written with 0xAA (always recorded, like opWrite).
	oracle.RecordWrite("node", tlID, 1, fullPage)

	// BUG: partial write to page 5 returned error -> oracle does NOT record it.
	// (We intentionally skip RecordWrite here to demonstrate the bug.)

	// Flush is recorded.
	oracle.RecordFlush("node", tlID)

	// Oracle expects page 5 is zeros (unwritten).
	oracleExpected := oracle.ExpectedRead("node2", tlID, 5)
	if bytes.Equal(oracleExpected, buf) {
		t.Fatal("expected oracle to DISAGREE with actual data (demonstrating the bug)")
	}
	t.Logf("oracle expects zeros for page 5 but actual has partial write data — confirms the sim bug")
}

// TestMemLayerTombstoneThenPut verifies that writing to a page after tombstoning
// it doesn't corrupt slot 0's data. This was a bug where putTombstone set slot=0
// (zero value), and a subsequent put reused slot 0 instead of allocating a new one.
func TestMemLayerTombstoneThenPut(t *testing.T) {
	dir := t.TempDir()
	ml, err := newMemtable(dir, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ml.cleanup)

	// Write 3 pages into slots 0, 1, 2.
	page0 := make([]byte, PageSize)
	page1 := make([]byte, PageSize)
	page2 := make([]byte, PageSize)
	for i := range page0 {
		page0[i] = 0xAA
	}
	for i := range page1 {
		page1[i] = 0xBB
	}
	for i := range page2 {
		page2[i] = 0xCC
	}

	if err := ml.put(100, 1, page0); err != nil {
		t.Fatal(err)
	}
	if err := ml.put(200, 2, page1); err != nil {
		t.Fatal(err)
	}
	if err := ml.put(300, 3, page2); err != nil {
		t.Fatal(err)
	}

	// Tombstone page 300 (which was in slot 2). The tombstone entry has slot=0
	// as a zero value, which is NOT the correct slot.
	if err := ml.putTombstone(300, 4); err != nil {
		t.Fatal(err)
	}

	// Now write NEW data to page 300. Before the fix, put() would see the
	// existing tombstone entry, reuse slot 0 (the zero-value slot), and
	// overwrite page 100's data at slot 0.
	page3 := make([]byte, PageSize)
	for i := range page3 {
		page3[i] = 0xDD
	}
	if err := ml.put(300, 5, page3); err != nil {
		t.Fatal(err)
	}

	// Read page 100 (slot 0) — should still be 0xAA, not 0xDD.
	e0, ok := ml.get(100)
	if !ok {
		t.Fatal("page 100 not found")
	}
	data0, err := ml.readData(e0)
	if err != nil {
		t.Fatal(err)
	}
	if data0[0] != 0xAA {
		t.Fatalf("page 100 corrupted: expected 0xAA, got 0x%02X", data0[0])
	}

	// Read page 300 — should be 0xDD.
	e3, ok := ml.get(300)
	if !ok {
		t.Fatal("page 300 not found")
	}
	data3, err := ml.readData(e3)
	if err != nil {
		t.Fatal(err)
	}
	if data3[0] != 0xDD {
		t.Fatalf("page 300 wrong: expected 0xDD, got 0x%02X", data3[0])
	}
}

// TestPunchHoleFlushReopenRead reproduces the e2e-fuse "bad message" bug:
// write data, punch holes over some of it, flush, reopen from S3, read back.
func TestPunchHoleFlushReopenRead(t *testing.T) {
	store := loophole.NewMemStore()
	cacheDir := t.TempDir()
	config := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	// Write and flush with first manager.
	dc1, err := NewPageCache(filepath.Join(cacheDir, "diskcache-1"))
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewVolumeManager(store, cacheDir, config, nil, dc1)
	t.Cleanup(func() { dc1.Close() })
	v1, err := m1.NewVolume(ctx, "test", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write 8 pages with distinct patterns (simulating ext4 mkfs metadata).
	pages := make([][]byte, 8)
	for i := range pages {
		pages[i] = bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		if err := v1.Write(ctx, pages[i], uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Punch holes over pages 2-4 (simulating ext4 zero_range during mkfs).
	if err := v1.PunchHole(ctx, 2*PageSize, 3*PageSize); err != nil {
		t.Fatal(err)
	}

	// Overwrite page 1 with new data (simulating ext4 updating metadata).
	pages[1] = bytes.Repeat([]byte{byte('Z')}, PageSize)
	if err := v1.Write(ctx, pages[1], PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush everything to S3.
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen with a new manager (simulating process restart / remount).
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Verify all pages.
	for i := 0; i < 8; i++ {
		buf := make([]byte, PageSize)
		n, err := v2.Read(ctx, buf, uint64(i)*PageSize)
		if err != nil {
			t.Fatalf("page %d read error: %v", i, err)
		}
		if n != PageSize {
			t.Fatalf("page %d: read %d bytes, expected %d", i, n, PageSize)
		}

		var expected []byte
		if i >= 2 && i <= 4 {
			expected = make([]byte, PageSize) // punched = zeros
		} else {
			expected = pages[i]
		}
		if !bytes.Equal(buf, expected) {
			t.Fatalf("page %d: data mismatch: first bytes got=%x want=%x", i, buf[:8], expected[:8])
		}
	}
}

// TestConcurrentWriteReadFlushReopen stresses concurrent writes and reads
// with interleaved flushes, then reopens and verifies data.
func TestConcurrentWriteReadFlushReopen(t *testing.T) {
	store := loophole.NewMemStore()
	cacheDir := t.TempDir()
	config := Config{
		FlushThreshold:  8 * PageSize, // small threshold to force frequent flushes
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	dc1, err := NewPageCache(filepath.Join(cacheDir, "diskcache-1"))
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewVolumeManager(store, cacheDir, config, nil, dc1)
	t.Cleanup(func() { dc1.Close() })
	v1, err := m1.NewVolume(ctx, "test", 64*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write 32 pages with concurrent goroutines, interleaving writes and
	// partial-page updates with punch holes.
	const numPages = 32
	expected := make([][]byte, numPages)
	for i := range expected {
		expected[i] = make([]byte, PageSize) // starts as zeros
	}

	// Phase 1: write all pages with full-page data.
	for i := 0; i < numPages; i++ {
		page := make([]byte, PageSize)
		rand.Read(page)
		expected[i] = page
		if err := v1.Write(ctx, page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 2: punch holes on pages 8-15.
	if err := v1.PunchHole(ctx, 8*PageSize, 8*PageSize); err != nil {
		t.Fatal(err)
	}
	for i := 8; i < 16; i++ {
		expected[i] = make([]byte, PageSize)
	}

	// Phase 3: overwrite pages 4-7 with new data.
	for i := 4; i < 8; i++ {
		page := make([]byte, PageSize)
		rand.Read(page)
		expected[i] = page
		if err := v1.Write(ctx, page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 4: partial sub-page writes to pages 16-19.
	for i := 16; i < 20; i++ {
		subPage := make([]byte, PageSize/2)
		rand.Read(subPage)
		off := PageSize / 4
		copy(expected[i][off:off+len(subPage)], subPage)
		if err := v1.Write(ctx, subPage, uint64(i)*PageSize+uint64(off)); err != nil {
			t.Fatal(err)
		}
	}

	// Flush and close.
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify.
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numPages; i++ {
		buf := make([]byte, PageSize)
		n, err := v2.Read(ctx, buf, uint64(i)*PageSize)
		if err != nil {
			t.Fatalf("page %d: read error: %v", i, err)
		}
		if n != PageSize {
			t.Fatalf("page %d: read %d bytes, expected %d", i, n, PageSize)
		}
		if !bytes.Equal(buf, expected[i]) {
			t.Fatalf("page %d: data mismatch after reopen: first 8 bytes got=%x want=%x",
				i, buf[:8], expected[i][:8])
		}
	}
}

// TestCompactMergeDeltasOnly verifies that when an image exists and delta size
// is small relative to image size, compact merges deltas into one delta instead
// of rebuilding the image.
func TestMemLayerFullBackpressure(t *testing.T) {
	store := loophole.NewMemStore()
	cacheDir := t.TempDir()

	// Set FlushThreshold very high so the size-based flush check never triggers.
	// The memlayer slot cap (maxMemtableSlots=16384) will be hit first.
	// We use a smaller volume to keep the test fast — write maxMemtableSlots+100
	// unique pages. Without backpressure, write 16385 would fail with
	// "memlayer full".
	config := Config{
		FlushThreshold:  1 << 62, // effectively infinite
		MaxFrozenTables: 2,
	}
	ctx := t.Context()

	dc, err := NewPageCache(filepath.Join(cacheDir, "diskcache"))
	if err != nil {
		t.Fatal(err)
	}
	m := NewVolumeManager(store, cacheDir, config, nil, dc)
	t.Cleanup(func() { dc.Close() })
	defer m.Close(ctx)

	// maxMemtableSlots is 65536, so volume needs to be larger than that.
	const numPages = maxMemtableSlots + 100
	v, err := m.NewVolume(ctx, "backpressure-test", uint64(numPages+10)*PageSize, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := range numPages {
		var page [PageSize]byte
		// Store page index as a 4-byte little-endian value.
		binary.LittleEndian.PutUint32(page[:4], uint32(i))
		if err := v.Write(ctx, page[:], uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}

	// Flush and verify a sample of pages can be read back correctly.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Check first page, last page, and a few around the memlayer boundary.
	checkPages := []int{0, 1, maxMemtableSlots - 1, maxMemtableSlots, maxMemtableSlots + 1, numPages - 1}
	for _, i := range checkPages {
		buf := make([]byte, PageSize)
		if _, err := v.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", i, err)
		}
		got := binary.LittleEndian.Uint32(buf[:4])
		if got != uint32(i) {
			t.Fatalf("page %d: expected marker %d, got %d", i, i, got)
		}
	}
}

// TestConcurrentReadsAndFlushes hammers concurrent reads and flushes to
// catch data races in the lock-free read path.
func TestConcurrentReadsAndFlushes(t *testing.T) {
	store := loophole.NewMemStore()
	config := Config{
		FlushThreshold: 64 * PageSize,
	}
	m := newTestManager(t, store, config)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "test", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write initial data across several pages and flush.
	const numPages = 8
	for i := range numPages {
		page := make([]byte, PageSize)
		binary.LittleEndian.PutUint32(page, uint32(i))
		if err := v.Write(ctx, page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Run concurrent readers and writers+flushers.
	var wg sync.WaitGroup
	const nReaders = 4
	const nWriters = 2
	const iterations = 50

	for range nReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, PageSize)
			for range iterations {
				pageIdx := mathrand.IntN(numPages)
				n, err := v.Read(ctx, buf, uint64(pageIdx)*PageSize)
				if err != nil {
					t.Errorf("read page %d: %v", pageIdx, err)
					return
				}
				if n != PageSize {
					t.Errorf("read %d bytes, expected %d", n, PageSize)
					return
				}
			}
		}()
	}

	for w := range nWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				page := make([]byte, PageSize)
				binary.LittleEndian.PutUint32(page, uint32(1000*w+i))
				pageIdx := mathrand.IntN(numPages)
				if err := v.Write(ctx, page, uint64(pageIdx)*PageSize); err != nil {
					t.Errorf("write: %v", err)
					return
				}
				if i%10 == 0 {
					if err := v.Flush(ctx); err != nil {
						t.Errorf("flush: %v", err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// TestAsyncFlushWriteDoesNotBlockOnS3 verifies that writes succeed even when
// S3 is failing, as long as frozen layer capacity is not exhausted. The
// background flush loop handles uploads asynchronously.
func TestAsyncFlushWriteDoesNotBlockOnS3(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize, // freeze after every page
		MaxFrozenTables: 10,       // allow many frozen layers before backpressure
		FlushInterval:   50 * time.Millisecond,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Make S3 uploads fail.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated S3 outage"),
	})

	// Write several pages — should all succeed because async flush doesn't
	// block the write path, and we have room for frozen layers.
	page := make([]byte, PageSize)
	for i := range 5 {
		page[0] = byte(i + 1)
		err := v.Write(ctx, page, uint64(i)*PageSize)
		if err != nil {
			t.Fatalf("write %d failed (should not block on S3): %v", i, err)
		}
	}

	// Verify data is readable from frozen layers + memlayer.
	buf := make([]byte, PageSize)
	for i := range 5 {
		if _, err := v.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", i, err)
		}
		if buf[0] != byte(i+1) {
			t.Fatalf("page %d: expected %d, got %d", i, i+1, buf[0])
		}
	}

	// Clear fault — background flush loop should drain frozen layers.
	store.ClearAllFaults()
	// Give the flush loop time to run.
	time.Sleep(200 * time.Millisecond)

	// Verify data survives after flush by closing and reopening.
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	vol := v.(*volume)
	if len(vol.layer.index.L0) == 0 {
		t.Fatal("expected delta layers after flush")
	}
}

// TestAsyncFlushNotifyWakesLoop verifies that the flush loop wakes up
// promptly when notified, rather than waiting for the timer.
func TestAsyncFlushNotifyWakesLoop(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize,
		MaxFrozenTables: 10,
		FlushInterval:   10 * time.Second, // very long timer
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write a page — triggers freeze + notify.
	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// The flush loop timer is 10s, but notify should wake it immediately.
	// Wait a short time and check that frozen layers were drained.
	vol := v.(*volume)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		vol.layer.frozenMu.RLock()
		n := len(vol.layer.frozenTables)
		vol.layer.frozenMu.RUnlock()
		if n == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	vol.layer.frozenMu.RLock()
	n := len(vol.layer.frozenTables)
	vol.layer.frozenMu.RUnlock()
	if n != 0 {
		t.Fatalf("expected 0 frozen layers after notify, got %d", n)
	}

	if len(vol.layer.index.L0) == 0 {
		t.Fatal("expected delta layers after async flush")
	}
}

// TestBackpressureStillBlocksInline verifies that when frozen layers hit
// MaxFrozenTables, writes block on S3 inline (backpressure).
func TestBackpressureStillBlocksInline(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   10 * time.Second, // effectively disabled
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Inject S3 fault so flushes fail.
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("simulated S3 fault"),
	})

	// Write enough pages to fill MaxFrozenTables. The first writes succeed
	// (freeze to frozen layers), but eventually backpressure kicks in and
	// the write fails because inline flush fails.
	page := make([]byte, PageSize)
	var writeErr error
	for i := range 20 {
		page[0] = byte(i)
		writeErr = v.Write(ctx, page, uint64(i)*PageSize)
		if writeErr != nil {
			break
		}
	}

	if writeErr == nil {
		t.Fatal("expected write to fail due to backpressure + S3 fault")
	}
	if !strings.Contains(writeErr.Error(), "simulated S3 fault") {
		t.Fatalf("expected S3 fault error, got: %v", writeErr)
	}

	// Clear fault — data in frozen layers should be flushable.
	store.ClearAllFaults()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("flush after clearing fault: %v", err)
	}
}

// TestFlushRetryOnTransientError verifies that flushMemLayer retries
// on transient S3 errors before giving up.
func TestFlushRetryOnTransientError(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xEE
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Fault that expires after 3 calls.
	var faultCount atomic.Int32
	store.SetFault(loophole.OpPutReader, "", loophole.Fault{
		Err: fmt.Errorf("transient S3 error"),
		ShouldFault: func(string) bool {
			return faultCount.Add(1) <= 3
		},
	})

	// Flush should succeed after retries.
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("flush should succeed after retries: %v", err)
	}

	// Verify data.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xEE {
		t.Fatalf("expected 0xEE, got 0x%02X", buf[0])
	}
}

// TestWriteTriggersEarlyFlushWhenStale verifies that a write triggers a
// flush after a 2s delay when the last flush was more than FlushInterval ago.
func TestWriteTriggersEarlyFlushWhenStale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := loophole.NewMemStore()
		cfg := Config{
			FlushThreshold:  64 * PageSize, // large — memtable never fills
			MaxFrozenTables: 10,
			FlushInterval:   1 * time.Hour, // regular timer never fires
		}
		m := newTestManager(t, store, cfg)
		ctx := t.Context()

		v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
		if err != nil {
			t.Fatal(err)
		}
		vol := v.(*volume)

		// Write data — not stale yet, so no early flush.
		page := make([]byte, PageSize)
		page[0] = 0xAA
		if err := v.Write(ctx, page, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)

		vol.layer.mu.RLock()
		l0Count := len(vol.layer.index.L0)
		vol.layer.mu.RUnlock()
		if l0Count != 0 {
			t.Fatalf("expected 0 L0 entries before stale write, got %d", l0Count)
		}

		// Simulate stale state: set lastFlushAt to 2 hours ago.
		vol.layer.lastFlushAt.Store(time.Now().Add(-2 * time.Hour).UnixMilli())

		// Write again — triggers early flush path.
		page[0] = 0xBB
		if err := v.Write(ctx, page, PageSize); err != nil {
			t.Fatal(err)
		}

		// After 1s the flush hasn't happened yet (2s delay).
		time.Sleep(1 * time.Second)
		vol.layer.mu.RLock()
		l0Count = len(vol.layer.index.L0)
		vol.layer.mu.RUnlock()
		if l0Count != 0 {
			t.Fatalf("expected 0 L0 entries after 1s (flush delay is 2s), got %d", l0Count)
		}

		// After another 2s the flush should complete.
		time.Sleep(2 * time.Second)
		vol.layer.mu.RLock()
		l0Count = len(vol.layer.index.L0)
		vol.layer.mu.RUnlock()
		if l0Count == 0 {
			t.Fatal("expected L0 entries after early flush, got 0")
		}

		// Verify data integrity.
		buf := make([]byte, PageSize)
		if _, err := v.Read(ctx, buf, 0); err != nil {
			t.Fatal(err)
		}
		if buf[0] != 0xAA {
			t.Fatalf("page 0: expected 0xAA, got 0x%02X", buf[0])
		}
		if _, err := v.Read(ctx, buf, PageSize); err != nil {
			t.Fatal(err)
		}
		if buf[0] != 0xBB {
			t.Fatalf("page 1: expected 0xBB, got 0x%02X", buf[0])
		}
	})
}

// TestSingleflightDeduplicatesL0Downloads verifies that concurrent reads
// for the same uncached L0 blob only trigger one S3 download.
func TestSingleflightDeduplicatesL0Downloads(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write and flush to create L0.
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Clear the in-memory L0 cache to force re-download.
	vol := v.(*volume)
	vol.layer.l0Cache.clear()

	// Record S3 Get count before concurrent reads.
	getBefore := store.Count(loophole.OpGet)

	// Launch concurrent reads for the same page (same L0 blob).
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			buf := make([]byte, PageSize)
			_, errs[idx] = v.Read(ctx, buf, 0)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}

	// Without singleflight, we'd see up to 10 Gets. With it, should be ≤2
	// (1 for the L0 blob + possibly 1 for index.json or other metadata).
	getAfter := store.Count(loophole.OpGet)
	gets := getAfter - getBefore
	if gets > 2 {
		t.Fatalf("expected ≤2 S3 Gets with singleflight, got %d", gets)
	}
}

// TestBoundedCacheEviction verifies that the L0/block caches don't
// grow beyond MaxCacheEntries.
func TestBoundedCacheEviction(t *testing.T) {
	var c boundedCache[int]
	c.init(3)

	c.put("a", 1)
	c.put("b", 2)
	c.put("c", 3)

	// All three should be present.
	for _, k := range []string{"a", "b", "c"} {
		if _, ok := c.get(k); !ok {
			t.Fatalf("expected key %q to be present", k)
		}
	}

	// Adding a 4th should evict one.
	c.put("d", 4)
	c.mu.RLock()
	n := len(c.entries)
	c.mu.RUnlock()
	if n != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", n)
	}

	// "d" should always be present (never evicts the just-inserted key).
	if _, ok := c.get("d"); !ok {
		t.Fatal("expected key 'd' to be present after insert")
	}
}

// TestRefreshFollowMode verifies that a reader can see data written by
// another writer after calling Refresh().
func TestRefreshFollowMode(t *testing.T) {
	store := loophole.NewMemStore()
	ctx := t.Context()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}

	// Writer creates a volume and writes data.
	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := wv.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := wv.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Reader opens the same layer directly (simulating read-only follower).
	writerVol := wv.(*volume)
	readerLayer, err := openLayer(ctx, store, writerVol.layer.id, cfg, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer readerLayer.Close()

	// Reader should see the flushed data (loaded from index.json on open).
	buf := make([]byte, PageSize)
	if _, err := readerLayer.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xDD {
		t.Fatalf("expected 0xDD on initial open, got 0x%02X", buf[0])
	}

	// Writer writes more data and flushes.
	page[0] = 0xEE
	if err := wv.Write(ctx, page, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := wv.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Reader doesn't see new data before refresh.
	buf2 := make([]byte, PageSize)
	if _, err := readerLayer.Read(ctx, buf2, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf2[0] == 0xEE {
		t.Fatal("reader should not see new data before refresh()")
	}

	// After refresh, reader sees the new data.
	if err := readerLayer.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := readerLayer.Read(ctx, buf2, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf2[0] != 0xEE {
		t.Fatalf("expected 0xEE after refresh, got 0x%02X", buf2[0])
	}
}

// TestFlushReportsMetrics verifies that FlushBytes metric is incremented.
func TestFlushReportsMetrics(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Read current metric value.
	before := promCounterValue(metrics.FlushBytes)

	page := make([]byte, PageSize)
	page[0] = 0xFF
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	after := promCounterValue(metrics.FlushBytes)
	if after <= before {
		t.Fatalf("expected FlushBytes to increase, before=%f after=%f", before, after)
	}
}

// TestReadAtZeroCopyMemtable verifies that ReadAt returns a pinned
// zero-copy slice when reading a page-aligned full page from the memtable.
func TestReadAtZeroCopyMemtable(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  64 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write a page.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	page[PageSize-1] = 0xBB
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// ReadAt page-aligned full page — should be zero-copy from memtable.
	data, release, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if len(data) != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, len(data))
	}
	if data[0] != 0xAA {
		t.Fatalf("expected 0xAA, got 0x%02X", data[0])
	}
	if data[PageSize-1] != 0xBB {
		t.Fatalf("expected 0xBB, got 0x%02X", data[PageSize-1])
	}
}

// TestReadAtZeroCopyAfterFlush verifies that ReadAt works after data
// has been flushed to L0 (falls back to allocating path).
func TestReadAtZeroCopyAfterFlush(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
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

	// ReadAt after flush — data comes from L0/cache.
	data, release, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if data[0] != 0xCC {
		t.Fatalf("expected 0xCC, got 0x%02X", data[0])
	}
}

// TestReadAtSubPageFallback verifies that sub-page or unaligned ReadAt
// falls back to the allocating path correctly.
func TestReadAtSubPageFallback(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  64 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	for i := range page {
		page[i] = byte(i % 256)
	}
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Sub-page read at offset 100, length 200.
	data, release, err := v.ReadAt(ctx, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if len(data) != 200 {
		t.Fatalf("expected 200 bytes, got %d", len(data))
	}
	for i := range 200 {
		expected := byte((100 + i) % 256)
		if data[i] != expected {
			t.Fatalf("byte %d: expected 0x%02X, got 0x%02X", i, expected, data[i])
			break
		}
	}
}

// TestReadAtPinPreventsCleanup verifies that a pinned read prevents
// memtable cleanup until released.
func TestReadAtPinPreventsCleanup(t *testing.T) {
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  PageSize, // freeze after 1 page
		MaxFrozenTables: 10,
		FlushInterval:   -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := v.Write(ctx, page, 0); err != nil {
		t.Fatal(err)
	}

	// Pin the page via ReadAt before flushing.
	data, release, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}

	// Flush — this freezes and flushes the memtable.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Data should still be valid while pinned.
	if data[0] != 0xDD {
		t.Fatalf("expected 0xDD while pinned, got 0x%02X", data[0])
	}

	// Release the pin.
	release()
}
