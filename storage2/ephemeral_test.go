package storage2

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole"
)

// TestFlushAfterCloneEphemeralEOF reproduces the "read ephemeral at N: EOF" bug.
// Scenario from TestE2E_Torture:
//  1. Write many pages (triggers auto-flush cycles)
//  2. Clone (freezes memtable)
//  3. Write more pages to parent
//  4. Flush parent → fails reading frozen memtable's ephemeral file
func TestFlushAfterCloneEphemeralEOF(t *testing.T) {
	// Use a small flush threshold to trigger multiple freeze/flush cycles
	// during the initial writes, matching the e2e behavior where mkfs.ext4
	// + file writes generate hundreds of pages.
	cfg := Config{
		FlushThreshold:  4 * PageSize,
		MaxFrozenTables: 2,
	}
	m := newTestManager(t, loophole.NewMemStore(), cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "parent", 128*1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1: Write many pages. This triggers multiple freeze+flush cycles
	// internally (FlushThreshold=4 pages, so every 4 pages we freeze+flush).
	nPages := 20
	for i := 0; i < nPages; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, PageSize)
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}

	// Phase 2: Flush to clear all frozen layers, then clone.
	if err := v.Flush(); err != nil {
		t.Fatalf("pre-clone flush: %v", err)
	}

	snap, err := v.Clone("child")
	if err != nil {
		t.Fatalf("clone: %v", err)
	}

	// Phase 3: Write more pages to parent after clone.
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte{byte(0xF0 + i)}, PageSize)
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatalf("post-clone write page %d: %v", i, err)
		}
	}

	// Phase 4: Flush parent — this must flush the frozen memLayer from the clone.
	if err := v.Flush(); err != nil {
		t.Fatalf("post-clone flush: %v", err)
	}

	// Verify parent has the new data.
	buf := make([]byte, PageSize)
	n, err := v.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if buf[0] != 0xF0 {
		t.Fatalf("expected parent page 0 = 0xF0, got 0x%02x", buf[0])
	}

	// Verify child still has old data.
	n, err = snap.Read(ctx, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("read %d bytes, expected %d", n, PageSize)
	}
	if buf[0] != 1 {
		t.Fatalf("expected child page 0 = 0x01, got 0x%02x", buf[0])
	}
}

// TestMultipleFlushCyclesThenClone tests that repeated flush cycles
// don't corrupt the ephemeral file state when followed by a clone.
func TestMultipleFlushCyclesThenClone(t *testing.T) {
	cfg := Config{
		FlushThreshold:  4 * PageSize,
		MaxFrozenTables: 1, // aggressive: flush after every 4 pages
	}
	m := newTestManager(t, loophole.NewMemStore(), cfg)
	ctx := t.Context()

	v, err := m.NewVolume(ctx, "vol", 128*1024*1024, "")
	if err != nil {
		t.Fatal(err)
	}

	// Write 50 pages — triggers ~12 freeze+flush cycles.
	for i := 0; i < 50; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, PageSize)
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatalf("write page %d: %v", i, err)
		}
	}

	// Clone.
	child, err := v.Clone("child")
	if err != nil {
		t.Fatalf("clone: %v", err)
	}

	// Write to parent after clone.
	for i := 0; i < 10; i++ {
		data := bytes.Repeat([]byte{0xFF}, PageSize)
		if err := v.Write(data, uint64(i)*PageSize); err != nil {
			t.Fatalf("post-clone write %d: %v", i, err)
		}
	}

	// Flush parent.
	if err := v.Flush(); err != nil {
		t.Fatalf("flush parent: %v", err)
	}

	// Flush child (should be a no-op since it has no writes).
	if err := child.Flush(); err != nil {
		t.Fatalf("flush child: %v", err)
	}

	// Verify all 50 pages in child.
	buf := make([]byte, PageSize)
	for i := 0; i < 50; i++ {
		n, err := child.Read(ctx, buf, uint64(i)*PageSize)
		if err != nil {
			t.Fatalf("child read page %d: %v", i, err)
		}
		if n != PageSize {
			t.Fatalf("child read page %d: got %d bytes", i, n)
		}
		expected := byte(i + 1)
		if buf[0] != expected {
			t.Fatalf("child page %d: expected 0x%02x, got 0x%02x", i, expected, buf[0])
		}
	}
}
