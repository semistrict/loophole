package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole"
)

var _ managedVolume = (*frozenVolume)(nil)

// frozenVolume is a lightweight, read-only volume backed by a frozen layer.
// No memtable, no lease, no flush, no write path.
type frozenVolume struct {
	volumeHooks

	name    string
	size    uint64
	volType string
	layer   *layer
	manager *Manager
	refs    atomic.Int32
}

func newFrozenVolume(name string, size uint64, volType string, ly *layer, m *Manager) *frozenVolume {
	v := &frozenVolume{
		name:    name,
		size:    size,
		volType: volType,
		layer:   ly,
		manager: m,
	}
	v.refs.Store(1)
	return v
}

func (v *frozenVolume) Name() string       { return v.name }
func (v *frozenVolume) Size() uint64       { return v.size }
func (v *frozenVolume) ReadOnly() bool     { return true }
func (v *frozenVolume) VolumeType() string { return v.volType }

func (v *frozenVolume) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	return v.layer.Read(ctx, buf, offset)
}

func (v *frozenVolume) ReadAt(ctx context.Context, offset uint64, n int) ([]byte, error) {
	buf := make([]byte, n)
	got, err := v.Read(ctx, buf, offset)
	if err != nil {
		return nil, err
	}
	return buf[:got], nil
}

func (v *frozenVolume) Write([]byte, uint64) error {
	return fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) EnableDirectWriteback() error {
	return fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) DisableDirectWriteback() error {
	return fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) WritePagesDirect([]loophole.DirectPage) error {
	return fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) PunchHole(uint64, uint64) error {
	return fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) ZeroRange(offset, length uint64) error {
	return v.PunchHole(offset, length)
}

func (v *frozenVolume) Flush() error      { return nil }
func (v *frozenVolume) FlushLocal() error { return nil }

func (v *frozenVolume) Snapshot(snapshotName string) error {
	return fmt.Errorf("cannot snapshot frozen volume %q", v.name)
}

func (v *frozenVolume) Checkpoint() (string, error) {
	return "", fmt.Errorf("cannot checkpoint frozen volume %q", v.name)
}

// Clone creates a writable copy. No flush needed — frozen layer is immutable.
func (v *frozenVolume) Clone(cloneName string) error {
	m := v.manager
	ctx := context.Background()

	// Build the child index in memory — inherits all L0/L1/L2 from the zygote.
	childID := m.idGen()
	idx := v.layer.index
	idx.NextSeq = v.layer.nextSeq.Load()
	idx.L1 = v.layer.l1Map.Ranges()
	idx.L2 = v.layer.l2Map.Ranges()
	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index for clone: %w", err)
	}

	ref := volumeRef{
		LayerID: childID,
		Size:    v.size,
		Type:    v.volType,
	}
	refData, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("marshal volume ref: %w", err)
	}

	// Write child index + volume ref in parallel — they are independent.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := v.layer.store.At("layers/"+childID).PutIfNotExists(gctx, "index.json", idxData, map[string]string{
			"created_at": time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			return fmt.Errorf("create clone index: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := m.volRefs.PutIfNotExists(gctx, cloneName+"/index.json", refData); err != nil {
			return fmt.Errorf("create clone ref: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (v *frozenVolume) CopyFrom(loophole.Volume, uint64, uint64, uint64) (uint64, error) {
	return 0, fmt.Errorf("volume %q is frozen", v.name)
}

func (v *frozenVolume) Freeze() error { return nil } // already frozen

func (v *frozenVolume) Refresh(ctx context.Context) error {
	return v.layer.refresh(ctx)
}

func (v *frozenVolume) AcquireRef() error {
	for range 128 {
		n := v.refs.Load()
		if n <= 0 {
			return fmt.Errorf("volume %q is closed", v.name)
		}
		if v.refs.CompareAndSwap(n, n+1) {
			slog.Debug("frozenVolume: AcquireRef", "volume", v.name, "refsAfter", n+1)
			return nil
		}
	}
	panic("refs cas contention")
}

func (v *frozenVolume) ReleaseRef() error {
	newRefs := v.refs.Add(-1)
	if newRefs < 0 {
		panic(fmt.Sprintf("frozenVolume %q: ReleaseRef with refs already 0 (now %d)", v.name, newRefs))
	}
	slog.Debug("frozenVolume: ReleaseRef", "volume", v.name, "refsAfter", newRefs)
	if newRefs == 0 {
		v.manager.closeVolume(v.name)
		v.layer.Close()
	}
	return nil
}

func (v *frozenVolume) isReadOnly() bool { return true }
func (v *frozenVolume) flush() error     { return nil }
func (v *frozenVolume) close()           { v.layer.Close() }

func (v *frozenVolume) DebugInfo() VolumeDebugInfo {
	return VolumeDebugInfo{
		Name:     v.name,
		Size:     v.size,
		Type:     v.volType,
		ReadOnly: true,
		Refs:     v.refs.Load(),
		Layer:    v.layer.debugInfo(),
	}
}
