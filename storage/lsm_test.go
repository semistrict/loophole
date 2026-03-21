package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/semistrict/loophole/metrics"
	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
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
	return newTestManager(t, objstore.NewMemStore(), testConfig)
}

func testStoreManager(t *testing.T) (*objstore.MemStore, *Manager) {
	t.Helper()
	store := objstore.NewMemStore()
	return store, newTestManager(t, store, testConfig)
}

func TestMemLayerPutGet(t *testing.T) {
	ml := testDirtyBatch(1024)

	// Write a page.
	page := filledPage(0xAB)
	if err := ml.stagePage(42, page); err != nil {
		t.Fatal(err)
	}

	// Read it back.
	rec, ok := ml.lookup(42)
	if !ok {
		t.Fatal("page 42 not found")
	}
	data := rec.bytes()
	if !bytes.Equal(data, page[:]) {
		t.Fatal("page data mismatch")
	}

	// Non-existent page.
	_, ok = ml.lookup(99)
	if ok {
		t.Fatal("page 99 should not exist")
	}
}

func TestMemLayerZeroOverwrite(t *testing.T) {
	ml := testDirtyBatch(1024)

	// Write then overwrite with zeros.
	page := filledPage(0)
	page[0] = 0xFF
	if err := ml.stagePage(10, page); err != nil {
		t.Fatal(err)
	}
	if err := ml.stagePage(10, Page{}); err != nil {
		t.Fatal(err)
	}

	rec, ok := ml.lookup(10)
	if !ok {
		t.Fatal("page 10 not found")
	}
	data := rec.bytes()
	if data[0] != 0 {
		t.Fatalf("expected zero page, got first byte %d", data[0])
	}
}

func TestMemLayerFreeze(t *testing.T) {
	ml := testDirtyBatch(1024)

	if err := ml.stagePage(1, filledPage(0)); err != nil {
		t.Fatal(err)
	}
	ml.markClosed()

	// Writes should fail after freeze.
	if err := ml.stagePage(2, filledPage(0)); err == nil {
		t.Fatal("expected error writing to frozen memlayer")
	}
}

func TestWriteFlushRead(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write data across multiple pages.
	data := make([]byte, PageSize*3)
	rand.Read(data)
	if err := v.Write(data, 0); err != nil {
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
	if err := v.Flush(); err != nil {
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
	store := objstore.NewMemStore()
	formatTestStore(t, store)

	config := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	// Write and flush with first manager.
	m1 := &Manager{ObjectStore: store, config: config}
	t.Cleanup(func() { m1.Close() })
	v1, err := m1.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize*2)
	rand.Read(data)
	if err := v1.Write(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v1.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a new manager (simulating process restart).
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume("test")
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

	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialPageWrite(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write at an offset within a page.
	data := []byte("hello world")
	offset := uint64(100)
	if err := v.Write(data, offset); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write two pages.
	data := make([]byte, PageSize*2)
	rand.Read(data)
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole covering the first page exactly.
	if err := v.PunchHole(0, PageSize); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write one full page of 'X'.
	data := bytes.Repeat([]byte("X"), PageSize)
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole covering only the first 4096 bytes (sub-page).
	if err := v.PunchHole(0, 4096); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write one page of 'Y'.
	data := bytes.Repeat([]byte("Y"), PageSize)
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}

	// Flush write to S3.
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Punch hole.
	if err := v.PunchHole(0, PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush tombstone to S3.
	if err := v.Flush(); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write one page of 'X'.
	data := bytes.Repeat([]byte("X"), PageSize)
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}

	// Punch hole.
	if err := v.PunchHole(0, PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush to S3 (moves tombstone from memLayer to delta layer).
	if err := v.Flush(); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write 5 pages to fill the memLayer with some data.
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot freezes the memLayer.
	if err := snapshotVolume(t, v, "snap"); err != nil {
		t.Fatal(err)
	}

	// Flush should succeed — the frozen memLayer's ephemeral file should be readable.
	if err := v.Flush(); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write, flush, write more, flush again.
	data1 := make([]byte, PageSize)
	rand.Read(data1)
	if err := v.Write(data1, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, PageSize)
	rand.Read(data2)
	if err := v.Write(data2, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write data to parent.
	data := make([]byte, PageSize)
	data[0] = 0xAA
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}

	// Snapshot.
	if err := snapshotVolume(t, v, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Write more to parent after snapshot.
	data[0] = 0xBB
	if err := v.Write(data, 0); err != nil {
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

	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write data.
	data := make([]byte, PageSize)
	data[0] = 0xCC
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Clone.
	clone := cloneOpen(t, v, "clone1")

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
	if err := clone.Write(data, 0); err != nil {
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

func TestCloneFromCheckpoint(t *testing.T) {
	store := objstore.NewMemStore()
	m := newTestManager(t, store, testConfig)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write two pages and flush so they land in L1/L2.
	page0 := make([]byte, PageSize)
	page0[0] = 0xAA
	if err := v.Write(page0, 0); err != nil {
		t.Fatal(err)
	}
	page1 := make([]byte, PageSize)
	page1[0] = 0xBB
	if err := v.Write(page1, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint, then write more to the parent (clone should not see this).
	cpID, err := v.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	page0[0] = 0xFF
	if err := v.Write(page0, 0); err != nil {
		t.Fatal(err)
	}

	// Clone from checkpoint on a separate manager.
	if err := CloneFromCheckpoint(ctx, m.Store(), "parent", cpID, "clone1"); err != nil {
		t.Fatal(err)
	}
	m2 := &Manager{ObjectStore: store, config: testConfig}
	t.Cleanup(func() { _ = m2.Close() })
	clone, err := m2.OpenVolume("clone1")
	if err != nil {
		t.Fatal(err)
	}

	// Clone sees the checkpoint data, not the post-checkpoint write.
	buf := make([]byte, PageSize)
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xAA {
		t.Fatalf("clone page 0: expected 0xAA, got 0x%02x", buf[0])
	}
	if _, err := clone.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xBB {
		t.Fatalf("clone page 1: expected 0xBB, got 0x%02x", buf[0])
	}

	// Clone is independently writable.
	page0[0] = 0xCC
	if err := clone.Write(page0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("clone after write: expected 0xCC, got 0x%02x", buf[0])
	}
}

func TestOverwriteAfterFlush(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write page 0, flush, overwrite page 0, read.
	data1 := make([]byte, PageSize)
	data1[0] = 0x11
	if err := v.Write(data1, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, PageSize)
	data2[0] = 0x22
	if err := v.Write(data2, 0); err != nil {
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
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	// Node A: create volume, write data, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0xAA
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := snapshotVolume(t, v, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Node B: open the snapshot and read data via ancestor chain.
	mB := newTestManager(t, store, cfg)
	snapVol, err := mB.OpenVolume("snap1")
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
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	// Create volume, write, flush.
	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0x11
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Snapshot parent -> snap1
	if err := snapshotVolume(t, v, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Open snap1 as writable clone so we can write to it.
	snap1 := cloneOpen(t, v, "snap1-clone")

	// Write to snap1-clone, flush.
	data[0] = 0x22
	if err := snap1.Write(data, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := snap1.Flush(); err != nil {
		t.Fatal(err)
	}

	// Snapshot snap1-clone -> snap2
	if err := snapshotVolume(t, snap1, "snap2"); err != nil {
		t.Fatal(err)
	}

	// Open snap2 on a fresh manager (different node).
	m2 := newTestManager(t, store, cfg)
	snap2, err := m2.OpenVolume("snap2")
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

// TestSnapshotReadAfterMultipleFlushes tests that snapshots created after
// multiple flush cycles still see all written data.
func TestSnapshotReadAfterMultipleFlushes(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple pages and flush multiple times.
	for i := byte(0); i < 5; i++ {
		data := make([]byte, PageSize)
		data[0] = i + 1
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Take two snapshots at different points.
	if err := snapshotVolume(t, v, "snap-pre"); err != nil {
		t.Fatal(err)
	}

	// Second snapshot.
	if err := snapshotVolume(t, v, "snap-post"); err != nil {
		t.Fatal(err)
	}

	// Open each snapshot on its own manager.
	mPre := newTestManager(t, store, cfg)
	snapPre, err := mPre.OpenVolume("snap-pre")
	if err != nil {
		t.Fatal(err)
	}

	mPost := newTestManager(t, store, cfg)
	snapPost, err := mPost.OpenVolume("snap-post")
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
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	// Node A: create, write, flush, snapshot.
	mA := newTestManager(t, store, cfg)
	v, err := mA.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, PageSize)
	data[0] = 0xCC
	if err := v.Write(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := snapshotVolume(t, v, "snap1"); err != nil {
		t.Fatal(err)
	}

	// Node B: open snap1, clone it, read from clone.
	mB := newTestManager(t, store, cfg)
	snapVol, err := mB.OpenVolume("snap1")
	if err != nil {
		t.Fatal(err)
	}

	cloneVol := cloneOpen(t, snapVol, "snap1-clone")

	buf := make([]byte, PageSize)
	if _, err := cloneVol.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("clone of snap: expected 0xCC, got 0x%02x", buf[0])
	}
}

// --- Manager edge case tests ---

func TestNewVolumeDuplicateName(t *testing.T) {
	m := testManager(t)

	if _, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	_, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestNewVolumeDefaultSize(t *testing.T) {
	m := testManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Size() != DefaultVolumeSize {
		t.Fatalf("expected default size %d, got %d", DefaultVolumeSize, v.Size())
	}
}

func TestOpenVolumeCacheHit(t *testing.T) {
	m := testManager(t)

	v1, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := m.OpenVolume("vol")
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

	_, err := m.OpenVolume("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestDeleteVolumeWhileOpen(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	if _, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	err := DeleteVolume(ctx, m.Store(), "vol")
	if err == nil || !strings.Contains(err.Error(), "lease held by") {
		t.Fatalf("expected 'lease held by' error, got: %v", err)
	}
}

func TestDeleteVolumeThenList(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	// Close the volume so it can be deleted.
	if err := v.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	if err := DeleteVolume(ctx, m.Store(), "vol"); err != nil {
		t.Fatal(err)
	}

	names, err := ListAllVolumes(ctx, store)
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

	if _, err := m.NewVolume(CreateParams{Volume: "a", Size: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}

	names := m.Volumes()
	if len(names) != 1 || names[0] != "a" {
		t.Fatalf("expected [a], got %v", names)
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

	if _, err := m.NewVolume(CreateParams{Volume: "v1", Size: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes()) != 0 {
		t.Fatal("Volumes() should be empty after Close")
	}
}

// --- Flush edge case tests ---

func TestFlushNoWrites(t *testing.T) {
	m := testManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	// No writes, just flush — should be a no-op.
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}
}

// --- Flush S3 failure tests ---

func TestFlushPutReaderFail(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write data.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	// Arm fault on PutReader.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 PUT failure"),
	})

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- v.Flush()
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("flush returned early under transient PutReader fault: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Clear the fault and let the blocked flush complete.
	store.ClearAllFaults()
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush failed after clearing PutReader fault: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not complete after clearing PutReader fault")
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

func TestFailedRewriteDoesNotCorruptVisibleBlock(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	// Build an initial sparse block with an ancestor page we want preserved.
	require.NoError(t, v.Write(bytes.Repeat([]byte{0xAA}, PageSize), 37*PageSize))
	require.NoError(t, v.Write(bytes.Repeat([]byte{0xBB}, PageSize), 68*PageSize))
	require.NoError(t, v.Flush())

	clone := cloneOpen(t, v, "clone")
	defer func() { _ = clone.ReleaseRef() }()

	// Establish a visible child-owned block first.
	require.NoError(t, clone.Write(bytes.Repeat([]byte{0xCC}, PageSize), 228*PageSize))
	require.NoError(t, clone.Flush())

	// Now attempt to rewrite that same child-owned block. The object write lands,
	// but the flush reports an error after upload. Readers must still observe the
	// previously committed block, not the failed rewrite payload.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		PostErr: fmt.Errorf("phantom put failure"),
	})
	require.NoError(t, clone.Write(bytes.Repeat([]byte{0xDD}, PageSize), 0))
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- clone.Flush()
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("flush returned early under phantom PutReader fault: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	buf := make([]byte, PageSize)
	_, err = clone.Read(ctx, buf, 37*PageSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xAA}, PageSize), buf)

	_, err = clone.Read(ctx, buf, 228*PageSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xCC}, PageSize), buf)

	// After clearing the fault, the blocked rewrite should flush cleanly.
	store.ClearAllFaults()
	select {
	case err := <-flushDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not complete after clearing phantom PutReader fault")
	}
	_, err = clone.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xDD}, PageSize), buf)
}

func TestPhantomAutoFlushRetryPreservesLaterPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}
	writePage := func(t *testing.T, v *Volume, page int, b byte) error {
		t.Helper()
		return v.Write(pageWithByte(b), uint64(page)*PageSize)
	}

	m := newTestManager(t, store, cfg)
	root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)
	require.NoError(t, writePage(t, root, 61, 0x61))
	require.NoError(t, root.Flush())

	child := cloneOpen(t, root, "child")
	defer func() { _ = child.ReleaseRef() }()

	// Make block 0 child-owned first.
	require.NoError(t, writePage(t, child, 5, 0x05))
	require.NoError(t, child.Flush())

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		PostErr: fmt.Errorf("phantom auto-flush failure"),
	})

	for _, page := range []int{236, 241, 187, 21, 185, 80, 26, 12} {
		require.NoError(t, writePage(t, child, page, byte(page)))
	}
	store.ClearAllFaults()

	for _, page := range []int{119, 77, 78, 146, 196, 61} {
		require.NoError(t, writePage(t, child, page, byte(page)))
	}
	require.NoError(t, child.Flush())

	buf := make([]byte, PageSize)
	_, err = child.Read(ctx, buf, 61*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(byte(61)), buf)
}

func TestLoadLayerMapFromListing(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	// Create, write, flush (flush writes directly to L1/L2).
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}
	vol := v

	// Write another page and flush so we have both delta and image layers.
	page2 := make([]byte, PageSize)
	page2[0] = 0xDD
	if err := v.Write(page2, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Find and delete layers.json from the store.
	tlID := vol.layer.id
	layersKey := "layers/" + tlID + "/layers.json"
	if err := store.DeleteObject(ctx, layersKey); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen on new manager — should fall back to S3 listing.
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume("vol")
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
	store := objstore.NewMemStore()
	ctx := t.Context()

	// Create two volumes via separate managers (one volume per manager),
	// then verify ListAllVolumes sees both in S3.
	m1 := newTestManager(t, store, testConfig)
	v1, err := m1.NewVolume(CreateParams{Volume: "alpha", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.ReleaseRef(); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2 := newTestManager(t, store, testConfig)
	v2, err := m2.NewVolume(CreateParams{Volume: "beta", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := v2.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	names, err := ListAllVolumes(ctx, store)
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

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// AcquireRef on a live volume should succeed.
	if err := v.AcquireRef(); err != nil {
		t.Fatalf("AcquireRef on live volume: %v", err)
	}

	// Now refs=2. Release once → refs=1 (no destroy).
	if err := v.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	// Volume should still be usable.
	buf := make([]byte, PageSize)
	if _, err := v.Read(ctx, buf, 0); err != nil {
		t.Fatalf("read after first ReleaseRef: %v", err)
	}

	// Release again → refs=0 → destroy.
	if err := v.ReleaseRef(); err != nil {
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

	store.SetFault(objstore.OpPutIfNotExists, "", objstore.Fault{
		Err: fmt.Errorf("simulated meta.json failure"),
	})
	_, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err == nil || !strings.Contains(err.Error(), "simulated meta.json failure") {
		t.Fatalf("expected meta.json failure, got: %v", err)
	}
}

// TestNewVolumeRefFail tests that a PutIfNotExists failure on the volume ref
// propagates an error from NewVolume (timeline meta.json succeeds, volume ref fails).
func TestNewVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)

	// Key-specific fault: only the volume ref write (volumes/vol) fails,
	// not the timeline meta.json write (timelines/<uuid>/meta.json).
	store.SetFault(objstore.OpPutIfNotExists, "volumes/vol/index.json", objstore.Fault{
		Err: fmt.Errorf("simulated volume ref failure"),
	})

	_, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err == nil || !strings.Contains(err.Error(), "simulated volume ref failure") {
		t.Fatalf("expected volume ref failure, got: %v", err)
	}
}

// TestDeleteVolumeS3Fail tests that a DeleteObject failure on the volume ref
// propagates an error from DeleteVolume.
func TestDeleteVolumeS3Fail(t *testing.T) {
	store, m := testStoreManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	// Close the volume so it can be deleted.
	if err := v.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	store.SetFault(objstore.OpDeleteObject, "", objstore.Fault{
		Err: fmt.Errorf("simulated delete failure"),
	})
	err = DeleteVolume(ctx, m.Store(), "vol")
	if err == nil || !strings.Contains(err.Error(), "simulated delete failure") {
		t.Fatalf("expected delete failure, got: %v", err)
	}

	// Clear fault — delete should work.
	store.ClearAllFaults()
	if err := DeleteVolume(ctx, m.Store(), "vol"); err != nil {
		t.Fatalf("retry delete failed: %v", err)
	}
}

// TestListAllVolumesFail tests that a ListKeys failure propagates from ListAllVolumes.
func TestListAllVolumesFail(t *testing.T) {
	store := objstore.NewMemStore()
	ctx := t.Context()

	store.SetFault(objstore.OpListKeys, "", objstore.Fault{
		Err: fmt.Errorf("simulated list failure"),
	})
	_, err := ListAllVolumes(ctx, store)
	if err == nil || !strings.Contains(err.Error(), "simulated list failure") {
		t.Fatalf("expected list failure, got: %v", err)
	}
}

// TestOpenVolumeRefFail tests that a Get failure when reading the volume ref
// propagates from OpenVolume.
func TestOpenVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)

	// Create a volume successfully.
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	// Close so it's not cached.
	if err := v.ReleaseRef(); err != nil {
		t.Fatal(err)
	}

	// Fault on Get — reading the volume ref fails.
	store.SetFault(objstore.OpGet, "", objstore.Fault{
		Err: fmt.Errorf("simulated get failure"),
	})
	_, err = m.OpenVolume("vol")
	if err == nil || !strings.Contains(err.Error(), "simulated get failure") {
		t.Fatalf("expected get failure on OpenVolume, got: %v", err)
	}
}

// TestOpenTimelineMetaFail tests that a Get failure when reading timeline meta.json
// propagates from OpenVolume (volume ref reads fine, but timeline meta fails).
func TestWritePartialPageReadFail(t *testing.T) {
	store, m := testStoreManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write a full page first, then flush so the data is on S3.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Close the first manager to release the lease.
	m.Close()

	// Open on a fresh manager (empty local cache) so reads must go to S3.
	m2 := newTestManager(t, store, m.config)
	v2, err := m2.OpenVolume("vol")
	if err != nil {
		t.Fatal(err)
	}

	// Now fault Get — a partial page write needs to read the existing page.
	store.SetFault(objstore.OpGet, "", objstore.Fault{
		Err: fmt.Errorf("simulated read failure"),
	})

	// Write 1 byte at offset 1 — partial page, triggers read-modify-write.
	err = v2.Write([]byte{0xBB}, 1)
	if err == nil || !strings.Contains(err.Error(), "simulated read failure") {
		t.Fatalf("expected read failure on partial write, got: %v", err)
	}

	// Clear fault — full page writes should still work (no read needed).
	store.ClearAllFaults()
	if err := v2.Write(page, 0); err != nil {
		t.Fatalf("full page write after clearing fault: %v", err)
	}
}

// TestGetDeltaLayerFail tests that a Get failure when downloading a delta layer
// propagates through readPage.
func TestSnapshotCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x55
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Fault PutIfNotExists — createChild writes child meta.json.
	store.SetFault(objstore.OpPutIfNotExists, "", objstore.Fault{
		Err: fmt.Errorf("simulated createChild failure"),
	})
	err = snapshotVolume(t, v, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated createChild failure") {
		t.Fatalf("expected createChild failure, got: %v", err)
	}

	// Clear — snapshot should work.
	store.ClearAllFaults()
	if err := snapshotVolume(t, v, "snap"); err != nil {
		t.Fatalf("snapshot retry: %v", err)
	}
}

// TestSnapshotPutVolumeRefFail tests that a PutIfNotExists failure on the
// snapshot's volume ref propagates from Snapshot.
func TestSnapshotPutVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x66
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Key-specific fault: only the snapshot volume ref write (volumes/snap) fails,
	// not the child timeline meta.json write.
	store.SetFault(objstore.OpPutIfNotExists, "volumes/snap/index.json", objstore.Fault{
		Err: fmt.Errorf("simulated putVolumeRef failure"),
	})

	err = snapshotVolume(t, v, "snap")
	if err == nil || !strings.Contains(err.Error(), "simulated putVolumeRef failure") {
		t.Fatalf("expected putVolumeRef failure, got: %v", err)
	}
}

// TestCreateChildUpdateParentMetaFail tests that a PutBytesCAS failure when
// updating the parent's meta.json during createChild propagates.
func TestCloneCreateChildFail(t *testing.T) {
	store, m := testStoreManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x88
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Fault PutIfNotExists for createChild.
	store.SetFault(objstore.OpPutIfNotExists, "", objstore.Fault{
		Err: fmt.Errorf("simulated clone createChild failure"),
	})
	err = v.Clone("clone")
	if err == nil || !strings.Contains(err.Error(), "simulated clone createChild failure") {
		t.Fatalf("expected clone createChild failure, got: %v", err)
	}
}

// TestClonePutVolumeRefFail tests that putVolumeRef failure propagates from Clone.
func TestClonePutVolumeRefFail(t *testing.T) {
	store, m := testStoreManager(t)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0x99
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Key-specific fault: only the clone volume ref write (volumes/clone) fails.
	store.SetFault(objstore.OpPutIfNotExists, "volumes/clone/index.json", objstore.Fault{
		Err: fmt.Errorf("simulated clone putVolumeRef failure"),
	})

	err = v.Clone("clone")
	if err == nil || !strings.Contains(err.Error(), "simulated clone putVolumeRef failure") {
		t.Fatalf("expected clone putVolumeRef failure, got: %v", err)
	}
}

// TestOpenAncestorFail tests that an ancestor timeline open failure propagates.
func TestWriteBackpressurePreservesData(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold:    PageSize, // freeze after every write
		FlushInterval:     -1,       // disable periodic flush
		MaxDirtyPageSlots: 1,        // no extra slot; the next unique page must block
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Write page 0 — succeeds and gets flushed.
	page0 := make([]byte, PageSize)
	for i := range page0 {
		page0[i] = 0xAA
	}
	if err := v.Write(page0, 0); err != nil {
		t.Fatalf("write page 0: %v", err)
	}

	// Step 2: Write page 1 while S3 is faulted. The write should stage
	// locally and remain immediately readable even though the pending batch
	// cannot drain yet.
	page1 := make([]byte, PageSize)
	for i := range page1 {
		page1[i] = 0xBB
	}
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 fault"),
	})
	if err := v.Write(page1, PageSize); err != nil {
		t.Fatalf("write page 1: %v", err)
	}

	require.Eventually(t, func() bool {
		v.layer.dirtyMu.Lock()
		defer v.layer.dirtyMu.Unlock()
		return v.layer.pending != nil
	}, time.Second, 10*time.Millisecond)

	// Step 3: Write page 2. This fills the active batch but should still return
	// successfully because the batch had one free slot.
	page2 := make([]byte, PageSize)
	for i := range page2 {
		page2[i] = 0xCC
	}
	if err := v.Write(page2, 2*PageSize); err != nil {
		t.Fatalf("write page 2: %v", err)
	}

	// Step 4: Write page 3. With one page pending and the active batch already
	// full, this write should block until S3 recovers instead of failing.
	page3 := make([]byte, PageSize)
	for i := range page3 {
		page3[i] = 0xDD
	}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- v.Write(page3, 3*PageSize)
	}()
	buf := make([]byte, PageSize)

	select {
	case err := <-writeDone:
		t.Fatalf("write page 3 returned early under backpressure: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if _, err := v.Read(ctx, buf, PageSize); err != nil {
		t.Fatalf("read page 1 while blocked: %v", err)
	}
	if !bytes.Equal(buf, page1) {
		t.Fatalf("page 1 while blocked: expected 0xBB, got %x...", buf[:4])
	}

	// Step 5: Clear faults and flush everything.
	store.ClearAllFaults()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write page 3 after recovery: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write page 3 did not complete after clearing S3 fault")
	}
	if err := v.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Step 6: Verify all four pages are readable with correct data.
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

	if _, err := v.Read(ctx, buf, 3*PageSize); err != nil {
		t.Fatalf("read page 3: %v", err)
	}
	if !bytes.Equal(buf, page3) {
		t.Fatalf("page 3: expected 0xDD, got %x...", buf[:4])
	}
}

// TestCopyFromAutoFlushFault verifies that CopyFrom blocks under transient
// auto-flush failure and completes once the destination can drain again.
func TestCopyFromAutoFlushFault(t *testing.T) {
	store := objstore.NewMemStore()

	// Very low flush threshold: 2 pages triggers auto-flush.
	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	// Create source volume on its own manager.
	mSrc := newTestManager(t, store, cfg)
	src, err := mSrc.NewVolume(CreateParams{Volume: "src", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	srcPages := make([][]byte, 4)
	for i := range srcPages {
		srcPages[i] = bytes.Repeat([]byte{byte(0xA0 + i)}, PageSize)
		if err := src.Write(srcPages[i], uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Flush(); err != nil {
		t.Fatal(err)
	}

	// Create destination volume on a separate manager (same store).
	mDst := newTestManager(t, store, cfg)
	dst, err := mDst.NewVolume(CreateParams{Volume: "dst", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault so auto-flush during CopyFrom cannot drain.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	copied, err := dst.CopyFrom(src, 0, 0, 4*PageSize)
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if copied != 4*PageSize {
		t.Fatalf("expected CopyFrom to report full length after recovery, got %d", copied)
	}

	// Clear faults and flush the dirty batches.
	store.ClearAllFaults()
	if err := dst.Flush(); err != nil {
		t.Fatal(err)
	}

	// Read all pages from dst. They should all match the source.
	buf := make([]byte, PageSize)
	for i := 0; i < 4; i++ {
		if _, err := dst.Read(ctx, buf, uint64(i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", i, err)
		}
		if !bytes.Equal(buf, srcPages[i]) {
			t.Fatalf("page %d: expected copied source data", i)
		}
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
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	// Create a "parent" volume and write distinct data to pages 0-3.
	mParent := newTestManager(t, store, cfg)
	parent, err := mParent.NewVolume(CreateParams{Volume: "parent", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	parentData := bytes.Repeat([]byte{0xDD}, PageSize)
	for i := 0; i < 4; i++ {
		if err := parent.Write(parentData, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := parent.Flush(); err != nil {
		t.Fatal(err)
	}

	// Clone parent → dst. dst inherits parent's data for pages 0-3.
	if err := parent.Clone("dst"); err != nil {
		t.Fatal(err)
	}
	if err := mParent.Close(); err != nil {
		t.Fatal(err)
	}

	// Open dst on its own manager.
	mDst := newTestManager(t, store, cfg)
	dst, err := mDst.OpenVolume("dst")
	if err != nil {
		t.Fatal(err)
	}

	// Create a separate source volume with zeros on pages 0-3
	// (never written, so they're zeros).
	mSrc := newTestManager(t, store, cfg)
	src, err := mSrc.NewVolume(CreateParams{Volume: "src", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// CopyFrom zeros (src) over dst's inherited data.
	copied, copyErr := dst.CopyFrom(src, 0, 0, 4*PageSize)
	pagesReported := copied / PageSize
	t.Logf("CopyFrom: copied=%d (%d pages), err=%v", copied, pagesReported, copyErr)

	// Clear faults, flush dst to persist everything in memLayer.
	store.ClearAllFaults()
	if err := dst.Flush(); err != nil {
		t.Fatal(err)
	}

	// Close and reopen dst on a fresh manager (simulates different node).
	mDst.Close()
	mSrc.Close()
	m2 := newTestManager(t, store, cfg)
	dst2, err := m2.OpenVolume("dst")
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

	// Oracle: record only the pages CopyFrom reported.
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
	store := objstore.NewMemStore()

	cfg := Config{
		FlushThreshold: 2 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	// Write a full page to page 0 to put some data in memLayer.
	fullPage := bytes.Repeat([]byte{0xAA}, PageSize)
	if err := v.Write(fullPage, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Now write page 1 to get near the flush threshold.
	if err := v.Write(fullPage, PageSize); err != nil {
		t.Fatal(err)
	}

	// Inject PUT fault so auto-flush fails.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("injected S3 PUT failure"),
	})

	// Sub-page write to page 5 at offset 100, length 200.
	// Page 5 has never been written, so it's zeros.
	// writePage will: read zeros, copy partialData at offset 100, put full page in memLayer.
	// Then auto-flush fails -> Write returns error.
	partialData := bytes.Repeat([]byte{0xBB}, 200)
	writeErr := v.Write(partialData, 5*PageSize+100)
	t.Logf("partial write err: %v", writeErr)

	// The data IS in memLayer despite the error.
	// Clear faults and flush.
	store.ClearAllFaults()
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Close and reopen.
	m.Close()
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume("test")
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

// TestMemLayerOverwriteReusesSlot verifies that overwriting a page reuses
// the same slot and doesn't corrupt other pages' data.
func TestMemLayerOverwriteReusesSlot(t *testing.T) {
	ml := testDirtyBatch(64)

	if err := ml.stagePage(100, filledPage(0xAA)); err != nil {
		t.Fatal(err)
	}
	if err := ml.stagePage(200, filledPage(0xBB)); err != nil {
		t.Fatal(err)
	}
	if err := ml.stagePage(300, filledPage(0xCC)); err != nil {
		t.Fatal(err)
	}

	// Overwrite page 300 with new data — should reuse slot 2.
	if err := ml.stagePage(300, filledPage(0xDD)); err != nil {
		t.Fatal(err)
	}

	// Read page 100 — should still be 0xAA.
	rec0, ok := ml.lookup(100)
	if !ok {
		t.Fatal("page 100 not found")
	}
	data0 := rec0.bytes()
	if data0[0] != 0xAA {
		t.Fatalf("page 100 corrupted: expected 0xAA, got 0x%02X", data0[0])
	}

	// Read page 300 — should be 0xDD.
	rec3, ok := ml.lookup(300)
	if !ok {
		t.Fatal("page 300 not found")
	}
	data3 := rec3.bytes()
	if data3[0] != 0xDD {
		t.Fatalf("page 300 wrong: expected 0xDD, got 0x%02X", data3[0])
	}
}

// TestPunchHoleFlushReopenRead reproduces the e2e "bad message" bug:
// write data, punch holes over some of it, flush, reopen from S3, read back.
func TestPunchHoleFlushReopenRead(t *testing.T) {
	store := objstore.NewMemStore()

	config := Config{
		FlushThreshold: 16 * PageSize,
	}
	ctx := t.Context()

	// Write and flush with first manager.
	formatTestStore(t, store)
	m1 := &Manager{ObjectStore: store, config: config}
	v1, err := m1.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write 8 pages with distinct patterns (simulating ext4 mkfs metadata).
	pages := make([][]byte, 8)
	for i := range pages {
		pages[i] = bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		if err := v1.Write(pages[i], uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Punch holes over pages 2-4 (simulating ext4 zero_range during mkfs).
	if err := v1.PunchHole(2*PageSize, 3*PageSize); err != nil {
		t.Fatal(err)
	}

	// Overwrite page 1 with new data (simulating ext4 updating metadata).
	pages[1] = bytes.Repeat([]byte{byte('Z')}, PageSize)
	if err := v1.Write(pages[1], PageSize); err != nil {
		t.Fatal(err)
	}

	// Flush everything to S3.
	if err := v1.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a new manager (simulating process restart / remount).
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume("test")
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
	store := objstore.NewMemStore()

	config := Config{
		FlushThreshold: 8 * PageSize, // small threshold to force frequent flushes
	}
	ctx := t.Context()

	formatTestStore(t, store)
	m1 := &Manager{ObjectStore: store, config: config}
	v1, err := m1.NewVolume(CreateParams{Volume: "test", Size: 64 * PageSize})
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
		if err := v1.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 2: punch holes on pages 8-15.
	if err := v1.PunchHole(8*PageSize, 8*PageSize); err != nil {
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
		if err := v1.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 4: partial sub-page writes to pages 16-19.
	for i := 16; i < 20; i++ {
		subPage := make([]byte, PageSize/2)
		rand.Read(subPage)
		off := PageSize / 4
		copy(expected[i][off:off+len(subPage)], subPage)
		if err := v1.Write(subPage, uint64(i)*PageSize+uint64(off)); err != nil {
			t.Fatal(err)
		}
	}

	// Flush and close.
	if err := v1.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify.
	m2 := newTestManager(t, store, config)
	v2, err := m2.OpenVolume("test")
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

// TestMemLayerFullBackpressure verifies that the dirty pages layer handles
// backpressure when the dirty pages is full.
func TestMemLayerFullBackpressure(t *testing.T) {
	store := objstore.NewMemStore()

	// Use a small dirty pages slot cap so the test fills it quickly and
	// exercises the backpressure path without writing 65K+ pages.
	const testSlotCap = 512
	config := Config{
		FlushThreshold:    1 << 62, // effectively infinite — force slot cap to trigger
		MaxDirtyPageSlots: testSlotCap,
	}
	ctx := t.Context()

	formatTestStore(t, store)
	m := &Manager{ObjectStore: store, config: config}
	defer m.Close()

	const numPages = testSlotCap + 100
	v, err := m.NewVolume(CreateParams{Volume: "backpressure-test", Size: uint64(numPages+10) * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	for i := range numPages {
		var page Page
		// Store page index as a 4-byte little-endian value.
		binary.LittleEndian.PutUint32(page[:4], uint32(i))
		if err := v.Write(page[:], uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}

	// Flush and verify a sample of pages can be read back correctly.
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Check first page, last page, and a few around the slot cap boundary.
	checkPages := []int{0, 1, testSlotCap - 1, testSlotCap, testSlotCap + 1, numPages - 1}
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
	store := objstore.NewMemStore()
	config := Config{
		FlushThreshold: 64 * PageSize,
	}
	m := newTestManager(t, store, config)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write initial data across several pages and flush.
	const numPages = 8
	for i := range numPages {
		page := make([]byte, PageSize)
		binary.LittleEndian.PutUint32(page, uint32(i))
		if err := v.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(); err != nil {
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
				if err := v.Write(page, uint64(pageIdx)*PageSize); err != nil {
					t.Errorf("write: %v", err)
					return
				}
				if i%10 == 0 {
					if err := v.Flush(); err != nil {
						t.Errorf("flush: %v", err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// TestBackpressureBlocksWriteOnS3Failure verifies that writes block instead of
// returning transient errors when S3 is down and the dirty pipeline is full.
func TestBackpressureBlocksWriteOnS3Failure(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize, // freeze after every page
		FlushInterval:  -1,       // no proactive timer; worker still drains pending
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// First write succeeds (goes into the active dirty batch).
	page := make([]byte, PageSize)
	page[0] = 1
	require.NoError(t, v.Write(page, 0))

	// Make S3 uploads fail.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 outage"),
	})

	// Second write stages into the fresh active batch while the first page is
	// stuck in pending.
	page[0] = 2
	require.NoError(t, v.Write(page, PageSize))

	// Third write has nowhere to go and must block until S3 recovers.
	page[0] = 3
	done := make(chan error, 1)
	go func() {
		done <- v.Write(page, 2*PageSize)
	}()

	select {
	case err := <-done:
		t.Fatalf("write should block while S3 is unavailable, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Staged pages remain readable while the writer is blocked.
	buf := make([]byte, PageSize)
	_, err = v.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, byte(1), buf[0])
	_, err = v.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(2), buf[0])

	// Clear fault and the blocked write should complete.
	store.ClearAllFaults()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return err == nil
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	// Verify data survives after flush by closing and reopening.
	if err := v.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	vol := v
	vol.layer.mu.RLock()
	l1Count := vol.layer.l1Map.Len()
	vol.layer.mu.RUnlock()
	if l1Count == 0 {
		t.Fatal("expected L1 entries after flush")
	}
}

func TestBackpressureBlockedWriteStagesCurrentPage(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	require.NoError(t, err)

	first := bytes.Repeat([]byte{0x11}, PageSize)
	require.NoError(t, v.Write(first, 0))

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 outage"),
	})

	second := bytes.Repeat([]byte{0x22}, PageSize)
	require.NoError(t, v.Write(second, PageSize))

	buf := make([]byte, PageSize)
	_, err = v.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, second, buf)

	done := make(chan error, 1)
	third := bytes.Repeat([]byte{0x33}, PageSize)
	go func() {
		done <- v.Write(third, 2*PageSize)
	}()
	select {
	case err := <-done:
		t.Fatalf("write should block, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	store.ClearAllFaults()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return err == nil
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	_, err = v.Read(ctx, buf, 2*PageSize)
	require.NoError(t, err)
	require.Equal(t, third, buf)
}

func TestBackpressurePartialWriteCanStillStageMergedPage(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	require.NoError(t, err)

	seedPage := bytes.Repeat([]byte{0x11}, PageSize)
	require.NoError(t, v.Write(seedPage, 0))

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 outage"),
	})

	partial := bytes.Repeat([]byte{0x22}, 256)
	require.NoError(t, v.Write(partial, PageSize+700))

	buf := make([]byte, PageSize)
	_, err = v.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	expected := make([]byte, PageSize)
	copy(expected[700:], partial)
	require.Equal(t, expected, buf)

	store.ClearAllFaults()
	require.NoError(t, v.Flush())
}

func TestBackpressurePunchHoleCanStillStageTombstones(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold:    1 << 20,
		FlushInterval:     -1,
		MaxDirtyPageSlots: 1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	require.NoError(t, err)

	first := bytes.Repeat([]byte{0x11}, PageSize)
	second := bytes.Repeat([]byte{0x22}, PageSize)
	require.NoError(t, v.Write(first, 0))
	require.NoError(t, v.Flush())
	require.NoError(t, v.Write(second, PageSize))
	require.NoError(t, v.Flush())

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 outage"),
	})

	require.NoError(t, v.PunchHole(0, 2*PageSize))

	buf := make([]byte, PageSize)
	_, err = v.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, make([]byte, PageSize), buf)

	_, err = v.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, make([]byte, PageSize), buf)

	store.ClearAllFaults()
	require.NoError(t, v.Flush())
}

// TestAsyncFlushNotifyWakesLoop verifies that the flush loop wakes up
// promptly when notified, rather than waiting for the timer.
func TestAsyncFlushNotifyWakesLoop(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize,
		FlushInterval:  10 * time.Second, // very long timer
	}
	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write a page — triggers freeze + notify.
	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	// The flush loop timer is 10s, but notify should wake it immediately.
	// Wait a short time and check that frozen layers were drained.
	vol := v
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		vol.layer.dirtyMu.Lock()
		hasFrozen := vol.layer.pending != nil
		vol.layer.dirtyMu.Unlock()
		if !hasFrozen {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	vol.layer.dirtyMu.Lock()
	hasFrozen := vol.layer.pending != nil
	vol.layer.dirtyMu.Unlock()
	if hasFrozen {
		t.Fatalf("expected no frozen dirty pages after notify")
	}

	vol.layer.mu.RLock()
	l1Count := vol.layer.l1Map.Len()
	vol.layer.mu.RUnlock()
	if l1Count == 0 {
		t.Fatal("expected L1 entries after async flush")
	}
}

// TestBackpressureStillBlocksInline verifies that when the dirty queue fills
// up and flush cannot progress, the writer blocks instead of returning the
// transient S3 error.
func TestBackpressureStillBlocksInline(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Inject S3 fault so flushes fail.
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated S3 fault"),
	})

	page := make([]byte, PageSize)
	page[0] = 1
	require.NoError(t, v.Write(page, 0))
	page[0] = 2
	require.NoError(t, v.Write(page, PageSize))

	done := make(chan error, 1)
	page[0] = 3
	go func() {
		done <- v.Write(page, 2*PageSize)
	}()
	select {
	case err := <-done:
		t.Fatalf("expected write to block, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Clear fault — data in frozen layers should be flushable.
	store.ClearAllFaults()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return err == nil
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	if err := v.Flush(); err != nil {
		t.Fatalf("flush after clearing fault: %v", err)
	}
}

func TestManagerCloseStopsFlushRetryLoop(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	require.NoError(t, err)

	page := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, v.Write(page, 0))

	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("simulated persistent S3 outage"),
	})

	page = bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, v.Write(page, PageSize))

	closed := make(chan error, 1)
	go func() {
		closed <- m.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("manager close should stop the flush retry loop and return")
	}
}

// TestFlushRetryOnTransientError verifies that flushMemLayer retries
// on transient S3 errors before giving up.
func TestFlushRetryOnTransientError(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xEE
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	// Fault that expires after 3 calls.
	var faultCount atomic.Int32
	store.SetFault(objstore.OpPutReader, "", objstore.Fault{
		Err: fmt.Errorf("transient S3 error"),
		ShouldFault: func(string) bool {
			return faultCount.Add(1) <= 3
		},
	})

	// Flush should succeed after retries.
	if err := v.Flush(); err != nil {
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
		store := objstore.NewMemStore()
		cfg := Config{
			FlushThreshold: 64 * PageSize, // large — dirty pages never fills
			FlushInterval:  1 * time.Hour, // regular timer never fires
		}
		m := newTestManager(t, store, cfg)
		ctx := t.Context()

		v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
		if err != nil {
			t.Fatal(err)
		}
		vol := v

		// Write data — not stale yet, so no early flush.
		page := make([]byte, PageSize)
		page[0] = 0xAA
		if err := v.Write(page, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)

		vol.layer.mu.RLock()
		l1Count := vol.layer.l1Map.Len()
		vol.layer.mu.RUnlock()
		if l1Count != 0 {
			t.Fatalf("expected 0 L1 entries before stale write, got %d", l1Count)
		}

		// Simulate stale state: set lastFlushAt to 2 hours ago.
		vol.layer.lastFlushAt.Store(time.Now().Add(-2 * time.Hour).UnixMilli())

		// Write again — triggers early flush path.
		page[0] = 0xBB
		if err := v.Write(page, PageSize); err != nil {
			t.Fatal(err)
		}

		// After 1s the flush hasn't happened yet (2s delay).
		time.Sleep(1 * time.Second)
		vol.layer.mu.RLock()
		l1Count = vol.layer.l1Map.Len()
		vol.layer.mu.RUnlock()
		if l1Count != 0 {
			t.Fatalf("expected 0 L1 entries after 1s (flush delay is 2s), got %d", l1Count)
		}

		// After another 2s the flush should complete.
		time.Sleep(2 * time.Second)
		vol.layer.mu.RLock()
		l1Count = vol.layer.l1Map.Len()
		vol.layer.mu.RUnlock()
		if l1Count == 0 {
			t.Fatal("expected L1 entries after early flush, got 0")
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

// TestSingleflightDeduplicatesBlockDownloads verifies that concurrent reads
// for the same uncached block only trigger one S3 download.
func TestSingleflightDeduplicatesBlockDownloads(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write and flush to create L1 blocks.
	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Clear the in-memory block cache to force re-download.
	vol := v
	vol.layer.blockCache.clear()

	// Record S3 Get count before concurrent reads.
	getBefore := store.Count(objstore.OpGet)

	// Launch concurrent reads for the same page (same block).
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
	// (1 for the block blob + possibly 1 for index.json or other metadata).
	getAfter := store.Count(objstore.OpGet)
	gets := getAfter - getBefore
	if gets > 2 {
		t.Fatalf("expected ≤2 S3 Gets with singleflight, got %d", gets)
	}
}

// TestBoundedCacheEviction verifies that the block caches don't
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
	store := objstore.NewMemStore()
	ctx := t.Context()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}

	// Writer creates a volume and writes data.
	writer := newTestManager(t, store, cfg)
	wv, err := writer.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := wv.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := wv.Flush(); err != nil {
		t.Fatal(err)
	}

	// Reader opens the same layer directly (simulating read-only follower).
	writerVol := wv
	readerLayer, err := openLayer(ctx, layerParams{store: store, id: writerVol.layer.id, config: cfg})
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
	if err := wv.Write(page, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := wv.Flush(); err != nil {
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
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Read current metric value.
	before := promCounterValue(metrics.FlushBytes)

	page := make([]byte, PageSize)
	page[0] = 0xFF
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	after := promCounterValue(metrics.FlushBytes)
	if after <= before {
		t.Fatalf("expected FlushBytes to increase, before=%f after=%f", before, after)
	}
}

// TestReadAtDirtyPages verifies that ReadAt returns the expected page contents.
func TestReadAtDirtyPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 64 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	// Write a page.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	page[PageSize-1] = 0xBB
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	data, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}

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

// TestReadAtAfterFlush verifies that ReadAt works after data has been flushed.
func TestReadAtAfterFlush(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xCC
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	data, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}

	if data[0] != 0xCC {
		t.Fatalf("expected 0xCC, got 0x%02X", data[0])
	}
}

// TestReadAtSubPageFallback verifies that sub-page or unaligned ReadAt
// falls back to the allocating path correctly.
func TestReadAtSubPageFallback(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 64 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	for i := range page {
		page[i] = byte(i % 256)
	}
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	// Sub-page read at offset 100, length 200.
	data, err := v.ReadAt(ctx, 100, 200)
	if err != nil {
		t.Fatal(err)
	}

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

// TestReadAtReturnsCopy verifies that ReadAt returns a detached copy.
func TestReadAtReturnsCopy(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 64 * PageSize,
		FlushInterval:  -1,
	}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	page[0] = 0xDD
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}

	data, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 0xAA
	again, err := v.ReadAt(ctx, 0, PageSize)
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != 0xDD {
		t.Fatalf("expected stored data to be unchanged, got 0x%02X", again[0])
	}
}

// TestDirectWritebackClone verifies that WritePagesDirect + Clone works
// on a volume in direct writeback mode. This is the diff snapshot pattern:
// the volume is mmap'd (direct mode), dirty pages are written via
// WritePagesDirect, then the volume is cloned.
func TestDirectWritebackClone(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	const volSize = 256 * PageSize
	v, err := m.NewVolume(CreateParams{Volume: "mem-vol", Size: volSize})
	if err != nil {
		t.Fatal(err)
	}

	// Write initial data.
	page := make([]byte, PageSize)
	page[0] = 0xAA
	if err := v.Write(page, 0); err != nil {
		t.Fatal(err)
	}
	page[0] = 0xBB
	if err := v.Write(page, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	// Enable direct writeback (simulates mmap.MapVolume).
	if err := v.EnableDirectWriteback(); err != nil {
		t.Fatal("EnableDirectWriteback:", err)
	}

	// Normal Write should be rejected in direct mode.
	err = v.Write(page, 0)
	if err == nil {
		t.Fatal("expected error from Write() in direct mode, got nil")
	}

	// WritePagesDirect should work.
	page[0] = 0xCC
	if err := v.WritePagesDirect([]DirectPage{{Offset: 0, Data: append([]byte(nil), page...)}}); err != nil {
		t.Fatalf("WritePagesDirect: %v", err)
	}

	// Clone should work even in direct mode.
	clone := cloneOpen(t, v, "clone-1")
	defer func() { _ = clone.ReleaseRef() }()

	// Clone should see the direct-written data.
	buf := make([]byte, PageSize)
	if _, err := clone.Read(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xCC {
		t.Fatalf("clone page 0: expected 0xCC, got 0x%02x", buf[0])
	}

	// Clone should see original data on unmodified pages.
	if _, err := clone.Read(ctx, buf, PageSize); err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0xBB {
		t.Fatalf("clone page 1: expected 0xBB, got 0x%02x", buf[0])
	}
}

// TestDirectWritebackMultiPageRange verifies that WritePagesDirect accepts
// multi-page contiguous ranges (not just single pages).
func TestDirectWritebackMultiPageRange(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	v, err := m.NewVolume(CreateParams{Volume: "mem-vol", Size: 64 * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := v.EnableDirectWriteback(); err != nil {
		t.Fatal(err)
	}

	// Write a 3-page contiguous range as a single DirectPage.
	data := make([]byte, 3*PageSize)
	for i := range 3 {
		binary.LittleEndian.PutUint32(data[i*PageSize:], uint32(i+10))
	}
	if err := v.WritePagesDirect([]DirectPage{{
		Offset: 4 * PageSize,
		Data:   data,
	}}); err != nil {
		t.Fatal("WritePagesDirect multi-page:", err)
	}

	// Read back each page individually.
	buf := make([]byte, PageSize)
	for i := range 3 {
		if _, err := v.Read(ctx, buf, uint64(4+i)*PageSize); err != nil {
			t.Fatalf("read page %d: %v", 4+i, err)
		}
		got := binary.LittleEndian.Uint32(buf)
		want := uint32(i + 10)
		if got != want {
			t.Fatalf("page %d: expected %d, got %d", 4+i, want, got)
		}
	}
}

// TestDiffSnapshotClone replicates the Firecracker diff snapshot pattern:
// 1. Create volume, write full data (simulating full memory dump), flush
// 2. Clone volume → clone-1 (full snapshot)
// 3. Write sparse dirty pages to the SAME volume (simulating KVM dirty pages), flush
// 4. Clone volume again → clone-2 (diff snapshot)
// 5. Verify clone-2 has both dirty pages AND original data
//
// This is the exact sequence Firecracker uses:
//
//	loophole_open(mem_vol) → write dirty pages → flush → clone → close
func TestDiffSnapshotClone(t *testing.T) {
	m := testManager(t)
	ctx := t.Context()

	const volSize = 256 * PageSize // 1 MiB
	v, err := m.NewVolume(CreateParams{Volume: "mem-vol", Size: volSize})
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Write "full memory dump" — fill every page with its page index.
	const numPages = 256
	page := make([]byte, PageSize)
	for i := range numPages {
		binary.LittleEndian.PutUint32(page, uint32(i))
		page[4] = 0xAA // marker: original data
		if err := v.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal("flush after full dump:", err)
	}

	// Step 2: Clone → clone-1 (full snapshot)
	clone1 := cloneOpen(t, v, "clone-1")
	defer func() { _ = clone1.ReleaseRef() }()

	// Verify clone-1 has the original data.
	buf := make([]byte, PageSize)
	if _, err := clone1.Read(ctx, buf, 42*PageSize); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(buf) != 42 || buf[4] != 0xAA {
		t.Fatalf("clone-1 page 42: expected idx=42 marker=0xAA, got idx=%d marker=0x%02x",
			binary.LittleEndian.Uint32(buf), buf[4])
	}

	// Step 3: Write sparse dirty pages to the SAME volume (simulating diff snapshot).
	// Dirty pages: 0, 10, 42, 100, 255 — sparse subset.
	dirtyPages := []int{0, 10, 42, 100, 255}
	for _, pg := range dirtyPages {
		binary.LittleEndian.PutUint32(page, uint32(pg))
		page[4] = 0xBB // marker: dirty data
		if err := v.Write(page, uint64(pg)*PageSize); err != nil {
			t.Fatalf("dirty write page %d: %v", pg, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal("flush after dirty writes:", err)
	}

	// Step 4: Clone → clone-2 (diff snapshot)
	clone2 := cloneOpen(t, v, "clone-2")
	defer func() { _ = clone2.ReleaseRef() }()

	// Step 5: Verify clone-2.

	// Dirty pages should have the new data.
	dirtySet := make(map[int]bool)
	for _, pg := range dirtyPages {
		dirtySet[pg] = true
	}
	for _, pg := range dirtyPages {
		if _, err := clone2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read dirty page %d from clone-2: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xBB {
			t.Fatalf("clone-2 dirty page %d: expected idx=%d marker=0xBB, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}

	// Clean pages should have the original data.
	cleanPages := []int{1, 5, 20, 50, 128, 200, 254}
	for _, pg := range cleanPages {
		if _, err := clone2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read clean page %d from clone-2: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xAA {
			t.Fatalf("clone-2 clean page %d: expected idx=%d marker=0xAA, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}
}

// TestDiffSnapshotCloneReopened is like TestDiffSnapshotClone but reopens
// the volume on a fresh manager before writing dirty pages. This simulates
// the case where Firecracker restores from a snapshot and the daemon is a
// fresh process that reopens the volume.
func TestDiffSnapshotCloneReopened(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	// Node 1: Create volume, write full data, clone-1.
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(CreateParams{Volume: "mem-vol", Size: 256 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	for i := range 256 {
		binary.LittleEndian.PutUint32(page, uint32(i))
		page[4] = 0xAA
		if err := v.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}

	clone1 := cloneOpen(t, v, "clone-1")
	_ = clone1.ReleaseRef()
	_ = m1.Close() // shut down node 1

	// Node 2: Reopen "mem-vol", write dirty pages, clone-2.
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume("mem-vol")
	if err != nil {
		t.Fatal("reopen mem-vol:", err)
	}

	dirtyPages := []int{0, 10, 42, 100, 255}
	for _, pg := range dirtyPages {
		binary.LittleEndian.PutUint32(page, uint32(pg))
		page[4] = 0xBB
		if err := v2.Write(page, uint64(pg)*PageSize); err != nil {
			t.Fatalf("dirty write page %d: %v", pg, err)
		}
	}
	if err := v2.Flush(); err != nil {
		t.Fatal("flush dirty:", err)
	}

	clone2 := cloneOpen(t, v2, "clone-2")

	// Verify dirty pages in clone-2.
	buf := make([]byte, PageSize)
	for _, pg := range dirtyPages {
		if _, err := clone2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read dirty page %d: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xBB {
			t.Fatalf("clone-2 dirty page %d: expected idx=%d marker=0xBB, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}

	// Verify clean pages in clone-2.
	cleanPages := []int{1, 5, 20, 50, 128, 200, 254}
	for _, pg := range cleanPages {
		if _, err := clone2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read clean page %d: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xAA {
			t.Fatalf("clone-2 clean page %d: expected idx=%d marker=0xAA, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}
}

// TestDiffSnapshotCloneReadFromFreshNode tests the full scenario end-to-end:
// create, write, clone-1, write dirty, clone-2, then open clone-2 on a
// completely fresh manager (simulating the restored VM reading memory).
func TestDiffSnapshotCloneReadFromFreshNode(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	// --- Phase 1: Full snapshot ---
	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(CreateParams{Volume: "mem-vol", Size: 256 * PageSize})
	if err != nil {
		t.Fatal(err)
	}

	page := make([]byte, PageSize)
	for i := range 256 {
		binary.LittleEndian.PutUint32(page, uint32(i))
		page[4] = 0xAA
		if err := v.Write(page, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal(err)
	}
	clone1 := cloneOpen(t, v, "clone-1")
	_ = clone1.ReleaseRef()

	// --- Phase 2: Diff snapshot (same manager/process) ---
	dirtyPages := []int{0, 10, 42, 100, 255}
	for _, pg := range dirtyPages {
		binary.LittleEndian.PutUint32(page, uint32(pg))
		page[4] = 0xBB
		if err := v.Write(page, uint64(pg)*PageSize); err != nil {
			t.Fatalf("dirty write page %d: %v", pg, err)
		}
	}
	if err := v.Flush(); err != nil {
		t.Fatal("flush dirty:", err)
	}
	clone2 := cloneOpen(t, v, "clone-2")
	_ = clone2.ReleaseRef()
	_ = m1.Close()

	// --- Phase 3: Fresh node reads clone-2 ---
	m2 := newTestManager(t, store, cfg)
	c2, err := m2.OpenVolume("clone-2")
	if err != nil {
		t.Fatal("open clone-2:", err)
	}

	buf := make([]byte, PageSize)

	// Dirty pages should have 0xBB marker.
	for _, pg := range dirtyPages {
		if _, err := c2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read dirty page %d: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xBB {
			t.Fatalf("fresh-node clone-2 dirty page %d: expected idx=%d marker=0xBB, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}

	// Clean pages should have 0xAA marker.
	cleanPages := []int{1, 5, 20, 50, 128, 200, 254}
	for _, pg := range cleanPages {
		if _, err := c2.Read(ctx, buf, uint64(pg)*PageSize); err != nil {
			t.Fatalf("read clean page %d: %v", pg, err)
		}
		idx := binary.LittleEndian.Uint32(buf)
		if idx != uint32(pg) || buf[4] != 0xAA {
			t.Fatalf("fresh-node clone-2 clean page %d: expected idx=%d marker=0xAA, got idx=%d marker=0x%02x",
				pg, pg, idx, buf[4])
		}
	}
}

func TestSparseAncestorPagePreservedAcrossChildSparseFlush(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	m1 := newTestManager(t, store, cfg)
	parent, err := m1.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	if err != nil {
		t.Fatal(err)
	}

	ancestorPage := bytes.Repeat([]byte{0x5c}, PageSize)
	if err := parent.Write(ancestorPage, 37*PageSize); err != nil {
		t.Fatal(err)
	}
	if err := parent.Flush(); err != nil {
		t.Fatal(err)
	}

	child1 := cloneOpen(t, parent, "child-1")
	defer func() { _ = child1.ReleaseRef() }()
	child2 := cloneOpen(t, child1, "child-2")
	defer func() { _ = child2.ReleaseRef() }()
	child3 := cloneOpen(t, child2, "child-3")
	defer func() { _ = child3.ReleaseRef() }()

	page0 := bytes.Repeat([]byte{0xa0}, PageSize)
	page1 := bytes.Repeat([]byte{0xa1}, PageSize)
	require.NoError(t, child3.Write(page0, 0))
	require.NoError(t, child3.Flush())
	require.NoError(t, child3.Write(page1, PageSize))
	require.NoError(t, child3.Flush())

	buf := make([]byte, PageSize)
	_, err = child3.Read(ctx, buf, 37*PageSize)
	require.NoError(t, err)
	require.Equal(t, ancestorPage, buf)
}

func TestSingleBlockCloneChainPreservesUntouchedAncestorPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 16 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}

	writePage := func(t *testing.T, v *Volume, page int, b byte) {
		t.Helper()
		require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
	}

	punchPage := func(t *testing.T, v *Volume, page int) {
		t.Helper()
		require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
	}

	m := newTestManager(t, store, cfg)
	root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	// Recreate the relevant single-block shape from the simulation.
	writePage(t, root, 37, 0x5c)
	for page, b := range map[int]byte{
		38: 0x38, 39: 0x39, 40: 0x40, 41: 0x41, 42: 0x42, 43: 0x43, 44: 0x44,
		45: 0x45, 46: 0x46, 68: 0x68, 69: 0x69, 107: 0x6b, 111: 0x6f, 124: 0x7c,
		127: 0x7f, 129: 0x81, 134: 0x86, 136: 0x88, 177: 0xb1, 188: 0xbc, 209: 0xd1,
		213: 0xd5, 220: 0xdc, 230: 0xe6,
	} {
		writePage(t, root, page, b)
	}
	punchPage(t, root, 19)
	require.NoError(t, root.Flush())

	child1 := cloneOpen(t, root, "child-1")
	defer func() { _ = child1.ReleaseRef() }()
	for page, b := range map[int]byte{
		14: 0x14, 68: 0x68, 93: 0x93, 115: 0x73, 167: 0xa7, 188: 0xbc,
	} {
		writePage(t, child1, page, b)
	}
	punchPage(t, child1, 212)
	punchPage(t, child1, 213)
	punchPage(t, child1, 214)
	require.NoError(t, child1.Flush())

	child2 := cloneOpen(t, child1, "child-2")
	defer func() { _ = child2.ReleaseRef() }()

	for page, b := range map[int]byte{
		28: 0x28, 102: 0x66, 115: 0x73, 193: 0xc1, 198: 0xc6,
	} {
		writePage(t, child2, page, b)
	}
	require.NoError(t, child2.Flush())

	writePage(t, child2, 174, 0x00)
	require.NoError(t, child2.Flush())

	for page, b := range map[int]byte{
		19: 0x19, 26: 0x1a, 38: 0x26, 132: 0x84, 151: 0x97, 234: 0xea,
	} {
		writePage(t, child2, page, b)
	}
	require.NoError(t, child2.Flush())

	buf := make([]byte, PageSize)
	_, err = child2.Read(ctx, buf, 37*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(0x5c), buf)
}

func TestSimulationSingleBlockSequenceWithoutFaults(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}
	writePage := func(t *testing.T, v *Volume, page int, b byte) {
		t.Helper()
		require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
	}
	punchPage := func(t *testing.T, v *Volume, page int) {
		t.Helper()
		require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
	}

	m := newTestManager(t, store, cfg)
	root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	for page, b := range map[int]byte{
		37: 0x5c, 38: 0x38, 39: 0x39, 40: 0x40, 41: 0x41, 42: 0x42, 43: 0x43,
		44: 0x44, 45: 0x45, 46: 0x46, 68: 0x68, 69: 0x69, 107: 0x6b, 111: 0x6f,
		124: 0x7c, 127: 0x7f, 129: 0x81, 134: 0x86, 136: 0x88, 177: 0xb1,
		188: 0xbc, 209: 0xd1, 213: 0xd5, 220: 0xdc, 230: 0xe6,
	} {
		writePage(t, root, page, b)
	}
	punchPage(t, root, 19)
	require.NoError(t, root.Flush())

	child1 := cloneOpen(t, root, "child-1")
	defer func() { _ = child1.ReleaseRef() }()
	for page, b := range map[int]byte{
		14: 0x14, 68: 0x68, 93: 0x93, 115: 0x73, 167: 0xa7, 188: 0xbc,
	} {
		writePage(t, child1, page, b)
	}
	punchPage(t, child1, 212)
	punchPage(t, child1, 213)
	punchPage(t, child1, 214)
	require.NoError(t, child1.Flush())

	child2 := cloneOpen(t, child1, "child-2")
	defer func() { _ = child2.ReleaseRef() }()
	for _, page := range []int{228, 229, 230, 231, 232} {
		punchPage(t, child2, page)
	}
	require.NoError(t, child2.Flush())

	for page, b := range map[int]byte{
		0: 0x10, 43: 0x43, 55: 0x55, 93: 0x93, 99: 0x99, 181: 0xb5, 192: 0xc0, 201: 0xc9,
	} {
		writePage(t, child2, page, b)
	}
	require.NoError(t, child2.Flush())

	child3 := cloneOpen(t, child2, "child-3")
	defer func() { _ = child3.ReleaseRef() }()
	writePage(t, child3, 52, 0x52)
	punchPage(t, child3, 173)
	punchPage(t, child3, 174)
	require.NoError(t, child3.Flush())
	writePage(t, child3, 131, 0x83)
	require.NoError(t, child3.Flush())

	buf := make([]byte, PageSize)
	_, err = child3.Read(ctx, buf, 37*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(0x5c), buf)
}

func TestDirectReproRelayeredSingleBlockPreservesEarlierChildPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}
	writePage := func(t *testing.T, v *Volume, page int, b byte) {
		t.Helper()
		require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
	}
	punchPage := func(t *testing.T, v *Volume, page int) {
		t.Helper()
		require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
	}
	flushPages := func(t *testing.T, v *Volume, writes map[int]byte, punches ...int) {
		t.Helper()
		for _, page := range punches {
			punchPage(t, v, page)
		}
		pages := make([]int, 0, len(writes))
		for page := range writes {
			pages = append(pages, page)
		}
		sort.Ints(pages)
		for _, page := range pages {
			writePage(t, v, page, writes[page])
		}
		require.NoError(t, v.Flush())
	}

	m := newTestManager(t, store, cfg)
	root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	flushPages(t, root, map[int]byte{
		48: 0x48, 64: 0x64, 112: 0x70, 192: 0xc0, 234: 0xea,
	})
	flushPages(t, root, map[int]byte{}, 131)
	flushPages(t, root, map[int]byte{}, 56, 57)
	flushPages(t, root, map[int]byte{}, 72, 73)
	flushPages(t, root, map[int]byte{}, 189)
	flushPages(t, root, map[int]byte{
		3: 0x03, 35: 0x23, 61: 0x3d, 182: 0xb6, 210: 0xd2, 242: 0xf2,
	})
	flushPages(t, root, map[int]byte{
		2: 0x02, 25: 0x19, 40: 0x28, 54: 0x36, 204: 0xcc, 216: 0xd8, 221: 0xdd,
	})

	branchA := cloneOpen(t, root, "branch-a")
	defer func() { _ = branchA.ReleaseRef() }()

	branchB := cloneOpen(t, root, "branch-b")
	defer func() { _ = branchB.ReleaseRef() }()

	flushPages(t, root, map[int]byte{}, 31)
	flushPages(t, root, map[int]byte{
		13: 0x13, 14: 0x14, 27: 0x27, 70: 0x46, 103: 0x67, 117: 0x75,
		164: 0xa4, 166: 0xa6, 183: 0xb7, 241: 0xf1, 251: 0xfb,
	}, 100, 101, 172, 173, 174)
	flushPages(t, root, map[int]byte{
		13: 0x8d, 19: 0x19, 65: 0x41, 111: 0x6f, 120: 0x78,
		134: 0x86, 144: 0x90, 158: 0x9e, 163: 0xa3, 235: 0xeb,
	})
	flushPages(t, root, map[int]byte{
		63: 0x3f, 207: 0xcf, 223: 0xdf, 239: 0xef, 244: 0xf4,
	})
	flushPages(t, root, map[int]byte{
		17: 0x11, 60: 0x3c, 63: 0x40, 119: 0x77, 123: 0x7b,
		148: 0x94, 173: 0xad, 195: 0xc3,
	})
	flushPages(t, root, map[int]byte{}, 91)
	flushPages(t, root, map[int]byte{
		5: 0x05, 45: 0x2d, 74: 0x4a, 131: 0x83, 144: 0x91,
		194: 0xc2, 205: 0xcd, 246: 0xf6, 251: 0xfc, 255: 0xff,
	})
	flushPages(t, root, map[int]byte{
		28: 0x1c, 49: 0x31, 67: 0x43, 118: 0x76, 125: 0x7d,
		152: 0x98, 153: 0x99, 159: 0x9f, 229: 0xe5, 248: 0xf8,
	})
	flushPages(t, root, map[int]byte{
		61: 0x3d, 68: 0x44, 86: 0x56, 94: 0x5e, 99: 0x63, 101: 0x65,
		123: 0x7c, 127: 0x7f, 182: 0xb6, 221: 0xdd, 230: 0xe6, 255: 0xfe,
	})
	flushPages(t, root, map[int]byte{
		58: 0x3a, 78: 0x4e, 81: 0x51, 86: 0x57, 102: 0x66, 107: 0x6b,
		119: 0x78, 122: 0x7a, 123: 0x7d, 126: 0x7e, 146: 0x92, 175: 0xaf,
		183: 0xb9, 209: 0xd1, 223: 0xe0, 244: 0xf5, 250: 0xfa,
	})
	flushPages(t, root, map[int]byte{
		31: 0x9f, 36: 0x24, 108: 0x6c, 160: 0xa0, 212: 0xd4,
	})
	flushPages(t, root, map[int]byte{
		11: 0x0b, 27: 0x2a, 41: 0x29, 60: 0x3d, 72: 0x48, 139: 0x8b,
		150: 0x96, 157: 0x9d, 175: 0xb0, 178: 0xb2, 187: 0xbb, 202: 0xca,
		212: 0xd5, 225: 0xe1, 226: 0xe2,
	}, 253)
	flushPages(t, root, map[int]byte{
		83: 0x53, 179: 0xb3, 199: 0xc7, 242: 0xf2,
	})
	flushPages(t, root, map[int]byte{
		2: 0x12, 76: 0x4c, 88: 0x58, 90: 0x5a, 113: 0x71,
		123: 0x7e, 129: 0x81, 133: 0x85, 204: 0xcc,
	})

	buf := make([]byte, PageSize)
	_, err = root.Read(ctx, buf, 14*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(0x14), buf)
}

func TestFailedCloneAfterSnapshotDoesNotDropEarlierPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}
	writePage := func(t *testing.T, v *Volume, page int, b byte) {
		t.Helper()
		require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
	}
	punchPage := func(t *testing.T, v *Volume, page int) {
		t.Helper()
		require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
	}
	flushPages := func(t *testing.T, v *Volume, writes map[int]byte, punches ...int) {
		t.Helper()
		for _, page := range punches {
			punchPage(t, v, page)
		}
		pages := make([]int, 0, len(writes))
		for page := range writes {
			pages = append(pages, page)
		}
		sort.Ints(pages)
		for _, page := range pages {
			writePage(t, v, page, writes[page])
		}
		require.NoError(t, v.Flush())
	}

	m := newTestManager(t, store, cfg)
	root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	flushPages(t, root, map[int]byte{
		2: 0x02, 3: 0x03, 25: 0x19, 35: 0x23, 40: 0x28, 48: 0x30,
		54: 0x36, 61: 0x3d, 64: 0x40, 112: 0x70, 182: 0xb6, 192: 0xc0,
		204: 0xcc, 210: 0xd2, 216: 0xd8, 221: 0xdd, 234: 0xea, 242: 0xf2,
	}, 56, 57, 72, 73, 131, 189)

	branch := cloneOpen(t, root, "branch-a")
	defer func() { _ = branch.ReleaseRef() }()

	flushPages(t, root, map[int]byte{}, 31)
	flushPages(t, root, map[int]byte{
		13: 0x13, 14: 0x14, 27: 0x27, 70: 0x46, 103: 0x67, 117: 0x75,
		164: 0xa4, 166: 0xa6, 183: 0xb7, 241: 0xf1, 251: 0xfb,
	}, 100, 101, 172, 173, 174)

	store.SetFault(objstore.OpGet, "volumes/root/index.json", objstore.Fault{
		Err: fmt.Errorf("simulated relayer volume-ref GET failure"),
	})
	err = root.Clone("failed-clone")
	require.ErrorContains(t, err, "re-layer parent: update volume ref")
	store.ClearAllFaults()

	flushPages(t, root, map[int]byte{
		13: 0x8d, 19: 0x19, 65: 0x41, 111: 0x6f, 120: 0x78,
		134: 0x86, 144: 0x90, 158: 0x9e, 163: 0xa3, 235: 0xeb,
	})
	flushPages(t, root, map[int]byte{
		63: 0x3f, 207: 0xcf, 223: 0xdf, 239: 0xef, 244: 0xf4,
	})
	flushPages(t, root, map[int]byte{
		17: 0x11, 60: 0x3c, 63: 0x40, 119: 0x77, 123: 0x7b,
		148: 0x94, 173: 0xad, 195: 0xc3,
	})
	flushPages(t, root, map[int]byte{}, 91)
	flushPages(t, root, map[int]byte{
		5: 0x05, 45: 0x2d, 74: 0x4a, 131: 0x83, 144: 0x91,
		194: 0xc2, 205: 0xcd, 246: 0xf6, 251: 0xfc, 255: 0xff,
	})

	buf := make([]byte, PageSize)
	_, err = root.Read(ctx, buf, 14*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(0x14), buf)
}

func TestFaultedSingleBlockRewriteStillPreservesEarlierPages(t *testing.T) {
	type flushBatch struct {
		writes  map[int]byte
		punches []int
	}

	runScenario := func(t *testing.T, faultBatch int, faultMode string, phantom bool) {
		t.Helper()

		store := objstore.NewMemStore()
		cfg := Config{
			FlushThreshold: 256 * PageSize,
			FlushInterval:  -1,
		}
		ctx := t.Context()

		pageWithByte := func(b byte) []byte {
			return bytes.Repeat([]byte{b}, PageSize)
		}
		writePage := func(t *testing.T, v *Volume, page int, b byte) {
			t.Helper()
			require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
		}
		punchPage := func(t *testing.T, v *Volume, page int) {
			t.Helper()
			require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
		}
		flushBatchFn := func(t *testing.T, v *Volume, batch flushBatch) error {
			t.Helper()
			for _, page := range batch.punches {
				punchPage(t, v, page)
			}
			pages := make([]int, 0, len(batch.writes))
			for page := range batch.writes {
				pages = append(pages, page)
			}
			sort.Ints(pages)
			for _, page := range pages {
				writePage(t, v, page, batch.writes[page])
			}
			return v.Flush()
		}

		m := newTestManager(t, store, cfg)
		root, err := m.NewVolume(CreateParams{Volume: "root", Size: 256 * PageSize, NoFormat: true})
		require.NoError(t, err)

		require.NoError(t, flushBatchFn(t, root, flushBatch{
			writes: map[int]byte{
				2: 0x02, 3: 0x03, 25: 0x19, 35: 0x23, 40: 0x28, 48: 0x30,
				54: 0x36, 61: 0x3d, 64: 0x40, 112: 0x70, 182: 0xb6, 192: 0xc0,
				204: 0xcc, 210: 0xd2, 216: 0xd8, 221: 0xdd, 234: 0xea, 242: 0xf2,
			},
			punches: []int{56, 57, 72, 73, 131, 189},
		}))

		branch := cloneOpen(t, root, "branch-a")
		defer func() { _ = branch.ReleaseRef() }()

		require.NoError(t, flushBatchFn(t, root, flushBatch{punches: []int{31}}))
		require.NoError(t, flushBatchFn(t, root, flushBatch{
			writes: map[int]byte{
				13: 0x13, 14: 0x14, 27: 0x27, 70: 0x46, 103: 0x67, 117: 0x75,
				164: 0xa4, 166: 0xa6, 183: 0xb7, 241: 0xf1, 251: 0xfb,
			},
			punches: []int{100, 101, 172, 173, 174},
		}))

		batches := []flushBatch{
			{writes: map[int]byte{13: 0x8d, 19: 0x19, 65: 0x41, 111: 0x6f, 120: 0x78, 134: 0x86, 144: 0x90, 158: 0x9e, 163: 0xa3, 235: 0xeb}},
			{writes: map[int]byte{63: 0x3f, 207: 0xcf, 223: 0xdf, 239: 0xef, 244: 0xf4}},
			{writes: map[int]byte{17: 0x11, 60: 0x3c, 63: 0x40, 119: 0x77, 123: 0x7b, 148: 0x94, 173: 0xad, 195: 0xc3}},
			{punches: []int{91}},
			{writes: map[int]byte{5: 0x05, 45: 0x2d, 74: 0x4a, 131: 0x83, 144: 0x91, 194: 0xc2, 205: 0xcd, 246: 0xf6, 251: 0xfc, 255: 0xff}},
			{writes: map[int]byte{28: 0x1c, 49: 0x31, 67: 0x43, 118: 0x76, 125: 0x7d, 152: 0x98, 153: 0x99, 159: 0x9f, 229: 0xe5, 248: 0xf8}},
			{writes: map[int]byte{61: 0x3d, 68: 0x44, 86: 0x56, 94: 0x5e, 99: 0x63, 101: 0x65, 123: 0x7c, 127: 0x7f, 182: 0xb6, 221: 0xdd, 230: 0xe6, 255: 0xfe}},
			{writes: map[int]byte{58: 0x3a, 78: 0x4e, 81: 0x51, 86: 0x57, 102: 0x66, 107: 0x6b, 119: 0x78, 122: 0x7a, 123: 0x7d, 126: 0x7e, 146: 0x92, 175: 0xaf, 183: 0xb9, 209: 0xd1, 223: 0xe0, 244: 0xf5, 250: 0xfa}},
			{writes: map[int]byte{31: 0x9f, 36: 0x24, 108: 0x6c, 160: 0xa0, 212: 0xd4}},
			{writes: map[int]byte{11: 0x0b, 27: 0x2a, 41: 0x29, 60: 0x3d, 72: 0x48, 139: 0x8b, 150: 0x96, 157: 0x9d, 175: 0xb0, 178: 0xb2, 187: 0xbb, 202: 0xca, 212: 0xd5, 225: 0xe1, 226: 0xe2}, punches: []int{253}},
			{writes: map[int]byte{83: 0x53, 179: 0xb3, 199: 0xc7, 242: 0xf2}},
			{writes: map[int]byte{2: 0x12, 76: 0x4c, 88: 0x58, 90: 0x5a, 113: 0x71, 123: 0x7e, 129: 0x81, 133: 0x85, 204: 0xcc}},
		}

		for i, batch := range batches {
			if i == faultBatch {
				switch faultMode {
				case "put":
					fault := objstore.Fault{Err: fmt.Errorf("simulated block upload failure")}
					if phantom {
						fault = objstore.Fault{PostErr: fmt.Errorf("simulated phantom block upload failure")}
					}
					store.SetFault(objstore.OpPutReader, "", fault)
				case "index-get":
					store.SetFault(objstore.OpGet, "layers/"+root.layer.id+"/index.json", objstore.Fault{
						Err: fmt.Errorf("simulated index get failure"),
					})
				default:
					t.Fatalf("unknown fault mode %q", faultMode)
				}

				flushDone := make(chan error, 1)
				go func() {
					flushDone <- flushBatchFn(t, root, batch)
				}()

				select {
				case err := <-flushDone:
					t.Fatalf("flush returned early under transient %s fault: %v", faultMode, err)
				case <-time.After(200 * time.Millisecond):
				}

				store.ClearAllFaults()
				select {
				case err := <-flushDone:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatalf("flush did not complete after clearing %s fault", faultMode)
				}
				continue
			}
			require.NoError(t, flushBatchFn(t, root, batch))
		}

		buf := make([]byte, PageSize)
		_, err = root.Read(ctx, buf, 14*PageSize)
		require.NoError(t, err)
		require.Equal(t, pageWithByte(0x14), buf)
	}

	for i := 0; i < 12; i++ {
		t.Run(fmt.Sprintf("puterr-batch-%02d", i), func(t *testing.T) {
			runScenario(t, i, "put", false)
		})
		t.Run(fmt.Sprintf("phantom-batch-%02d", i), func(t *testing.T) {
			runScenario(t, i, "put", true)
		})
		t.Run(fmt.Sprintf("indexget-batch-%02d", i), func(t *testing.T) {
			runScenario(t, i, "index-get", false)
		})
	}
}

func TestSingleLayerManySparseFlushesPreserveEarlierPages(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	ctx := t.Context()

	pageWithByte := func(b byte) []byte {
		return bytes.Repeat([]byte{b}, PageSize)
	}
	writePage := func(t *testing.T, v *Volume, page int, b byte) {
		t.Helper()
		require.NoError(t, v.Write(pageWithByte(b), uint64(page)*PageSize))
	}

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "vol", Size: 256 * PageSize, NoFormat: true})
	require.NoError(t, err)

	writePage(t, v, 31, 0x31)
	require.NoError(t, v.Flush())

	for _, page := range []int{100, 101, 172, 173, 174} {
		require.NoError(t, v.PunchHole(uint64(page)*PageSize, PageSize))
	}
	for page, b := range map[int]byte{
		13: 0x13, 14: 0x14, 27: 0x27, 70: 0x70, 103: 0x67, 117: 0x75,
		164: 0xa4, 166: 0xa6, 183: 0xb7, 241: 0xf1, 251: 0xfb,
	} {
		writePage(t, v, page, b)
	}
	require.NoError(t, v.Flush())

	for _, batch := range []map[int]byte{
		{19: 0x19, 63: 0x3f, 65: 0x41, 111: 0x6f, 120: 0x78, 134: 0x86, 144: 0x90, 158: 0x9e, 163: 0xa3, 235: 0xeb},
		{207: 0xcf, 223: 0xdf, 239: 0xef, 244: 0xf4},
		{17: 0x11, 60: 0x3c, 63: 0x40, 119: 0x77, 123: 0x7b, 148: 0x94, 173: 0xad, 195: 0xc3},
		{91: 0x00},
		{5: 0x05, 45: 0x45, 74: 0x4a, 131: 0x83, 144: 0x91, 194: 0xc2, 205: 0xcd, 246: 0xf6, 251: 0xfa, 255: 0xff},
		{28: 0x1c, 49: 0x31, 67: 0x43, 118: 0x76, 125: 0x7d, 152: 0x98, 153: 0x99, 159: 0x9f, 229: 0xe5, 248: 0xf8},
		{61: 0x3d, 68: 0x44, 86: 0x56, 94: 0x5e, 99: 0x63, 101: 0x65, 123: 0x7c, 127: 0x7f, 182: 0xb6, 221: 0xdd, 230: 0xe6, 255: 0xfe},
		{58: 0x3a, 78: 0x4e, 81: 0x51, 86: 0x57, 102: 0x66, 107: 0x6b, 119: 0x78, 122: 0x7a, 123: 0x7d, 126: 0x7e, 146: 0x92, 175: 0xaf, 183: 0xb8, 209: 0xd1, 223: 0xe0, 244: 0xf5, 250: 0xfa},
		{31: 0x32, 36: 0x24, 108: 0x6c, 160: 0xa0, 212: 0xd4},
		{11: 0x0b, 27: 0x28, 41: 0x29, 60: 0x3d, 72: 0x48, 139: 0x8b, 150: 0x96, 157: 0x9d, 175: 0xb0, 178: 0xb2, 187: 0xbb, 202: 0xca, 212: 0xd5, 225: 0xe1, 226: 0xe2, 253: 0x00},
		{83: 0x53, 179: 0xb3, 199: 0xc7, 242: 0xf2},
		{2: 0x02, 76: 0x4c, 88: 0x58, 90: 0x5a, 113: 0x71, 123: 0x7e, 129: 0x81, 133: 0x85, 204: 0xcc},
	} {
		for page, b := range batch {
			writePage(t, v, page, b)
		}
		require.NoError(t, v.Flush())
	}

	buf := make([]byte, PageSize)
	_, err = v.Read(ctx, buf, 14*PageSize)
	require.NoError(t, err)
	require.Equal(t, pageWithByte(0x14), buf)
}

func TestPunchHoleTombstoneNoSlotConsumed(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{FlushThreshold: 4 * PageSize, FlushInterval: -1}

	ly, err := openLayer(t.Context(), layerParams{store: store, id: "test", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	// Write 2 pages of data first (to have something to punch).
	data := bytes.Repeat([]byte{0xFF}, 2*PageSize)
	require.NoError(t, ly.Write(data, 0))

	// Record dirty pages slot count before punch.
	ly.dirtyMu.Lock()
	slotsBefore := ly.active.pages()
	ly.dirtyMu.Unlock()

	// Punch both pages.
	require.NoError(t, ly.PunchHole(0, 2*PageSize))

	// Tombstones should NOT have consumed additional slots.
	ly.dirtyMu.Lock()
	slotsAfter := ly.active.pages()
	ly.dirtyMu.Unlock()
	require.Equal(t, slotsBefore, slotsAfter)

	// But reads should return zeros.
	buf := make([]byte, 2*PageSize)
	_, err = ly.Read(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 2*PageSize), buf)
}

func TestPunchHoleTombstoneCloneInheritance(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{FlushThreshold: 4 * PageSize, FlushInterval: -1}
	m := newTestManager(t, store, cfg)
	ctx := t.Context()

	// Parent writes pages 0-3.
	parent, err := m.NewVolume(CreateParams{Volume: "parent", Size: 1024 * 1024})
	require.NoError(t, err)
	for i := range 4 {
		page := bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		require.NoError(t, parent.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, parent.Flush())

	// Clone (opens child on a separate manager, matching production).
	child := cloneOpen(t, parent, "child")

	// Punch pages 1-2 in child.
	require.NoError(t, child.PunchHole(PageSize, 2*PageSize))
	require.NoError(t, child.Flush())

	// Child: page 0 = 'A', pages 1-2 = zeros, page 3 = 'D'.
	for i := range 4 {
		buf := make([]byte, PageSize)
		_, err := child.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err)
		if i == 1 || i == 2 {
			require.Equal(t, zeroPage[:], buf, "child page %d should be zeros", i)
		} else {
			require.Equal(t, bytes.Repeat([]byte{byte('A' + i)}, PageSize), buf,
				"child page %d should be parent data", i)
		}
	}

	// Parent: all 4 pages still have original data.
	for i := range 4 {
		buf := make([]byte, PageSize)
		_, err := parent.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte('A' + i)}, PageSize), buf,
			"parent page %d should be unchanged", i)
	}
}

func TestPunchHoleTombstoneFlushReopenRead(t *testing.T) {
	store := objstore.NewMemStore()
	cfg := Config{FlushThreshold: 16 * PageSize}
	ctx := t.Context()

	formatTestStore(t, store)
	m1 := &Manager{ObjectStore: store, config: cfg}
	v1, err := m1.NewVolume(CreateParams{Volume: "test", Size: 1024 * 1024})
	require.NoError(t, err)

	// Write 8 pages.
	for i := range 8 {
		page := bytes.Repeat([]byte{byte('A' + i)}, PageSize)
		require.NoError(t, v1.Write(page, uint64(i)*PageSize))
	}

	// Punch pages 2-4.
	require.NoError(t, v1.PunchHole(2*PageSize, 3*PageSize))
	require.NoError(t, v1.Flush())
	require.NoError(t, m1.Close())

	// Reopen.
	m2 := newTestManager(t, store, cfg)
	v2, err := m2.OpenVolume("test")
	require.NoError(t, err)

	for i := range 8 {
		buf := make([]byte, PageSize)
		_, err := v2.Read(ctx, buf, uint64(i)*PageSize)
		require.NoError(t, err)
		if i >= 2 && i <= 4 {
			require.Equal(t, zeroPage[:], buf, "page %d should be zeros", i)
		} else {
			expected := bytes.Repeat([]byte{byte('A' + i)}, PageSize)
			require.Equal(t, expected, buf, "page %d", i)
		}
	}
}
