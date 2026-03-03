package lsm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"encoding/json"

	"github.com/google/uuid"

	"github.com/semistrict/loophole"
)

// Compile-time check.
var _ loophole.Volume = (*volume)(nil)

// volume wraps a Timeline to implement IVolume.
type volume struct {
	name     string
	size     uint64
	readOnly atomic.Bool
	refs     atomic.Int32

	mu       sync.RWMutex
	timeline *Timeline

	manager *Manager
}

func newVolume(name string, size uint64, tl *Timeline, m *Manager) *volume {
	if size == 0 {
		size = DefaultVolumeSize
	}
	v := &volume{
		name:     name,
		size:     size,
		timeline: tl,
		manager:  m,
	}
	v.refs.Store(1)
	return v
}

func (v *volume) Name() string   { return v.name }
func (v *volume) Size() uint64   { return v.size }
func (v *volume) ReadOnly() bool { return v.readOnly.Load() }

func (v *volume) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.timeline.Read(ctx, buf, offset)
}

func (v *volume) Write(ctx context.Context, data []byte, offset uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.timeline.Write(ctx, data, offset)
}

func (v *volume) PunchHole(ctx context.Context, offset, length uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.timeline.PunchHole(ctx, offset, length)
}

func (v *volume) Flush(ctx context.Context) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.timeline.Flush(ctx)
}

func (v *volume) Snapshot(ctx context.Context, snapshotName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.readOnly.Load() {
		return fmt.Errorf("cannot snapshot read-only volume %q", v.name)
	}

	// 1. Record branchSeq = timeline.nextSeq
	// 2. Freeze current memLayer
	// 3. Start new memLayer for parent (writes resume immediately)
	// 4. Create child timeline: ancestor = parent, ancestorSeq = branchSeq
	// 5. Write child's meta.json to S3

	branchSeq := v.timeline.nextSeq.Load()
	if err := v.timeline.freezeMemLayer(); err != nil {
		return fmt.Errorf("freeze memlayer for snapshot: %w", err)
	}

	// Flush all frozen layers to S3 so that when the snapshot's child
	// timeline is opened, its ancestor view includes all parent data.
	if err := v.timeline.flushFrozenLayers(ctx); err != nil {
		return fmt.Errorf("flush for snapshot: %w", err)
	}

	childID := uuid.NewString()
	if err := v.timeline.createChild(ctx, childID, branchSeq); err != nil {
		return fmt.Errorf("create snapshot child: %w", err)
	}

	// Create the snapshot as a read-only volume ref.
	if snapshotName != "" {
		ref := volumeRef{TimelineID: childID, Size: v.size, ReadOnly: true}
		if err := v.manager.putVolumeRef(ctx, snapshotName, ref); err != nil {
			return fmt.Errorf("create snapshot ref: %w", err)
		}
	}

	return nil
}

func (v *volume) Clone(ctx context.Context, cloneName string) (loophole.Volume, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// If writable, freeze + continue (like current freezeAndContinue).
	// If read-only, the timeline is already frozen; just create a child.

	branchSeq := v.timeline.nextSeq.Load()
	if !v.readOnly.Load() {
		if err := v.timeline.freezeMemLayer(); err != nil {
			return nil, fmt.Errorf("freeze memlayer for clone: %w", err)
		}
	}

	// Flush all frozen layers to S3 before opening the child. The child's
	// ancestor timeline is opened from S3, so unflushed data would be invisible.
	if err := v.timeline.flushFrozenLayers(ctx); err != nil {
		return nil, fmt.Errorf("flush for clone: %w", err)
	}

	childID := uuid.NewString()
	if err := v.timeline.createChild(ctx, childID, branchSeq); err != nil {
		return nil, fmt.Errorf("create clone child: %w", err)
	}

	ref := volumeRef{TimelineID: childID, Size: v.size}
	if err := v.manager.putVolumeRef(ctx, cloneName, ref); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	return v.manager.OpenVolume(ctx, cloneName)
}

func (v *volume) CopyFrom(ctx context.Context, src loophole.Volume, srcOff, dstOff, length uint64) (uint64, error) {
	if v.readOnly.Load() {
		return 0, fmt.Errorf("volume %q is read-only", v.name)
	}

	// Read pages from src, write to dst through the normal path.
	var copied uint64
	buf := make([]byte, PageSize)
	for copied < length {
		chunk := min(length-copied, PageSize)
		n, err := src.Read(ctx, buf[:chunk], srcOff+copied)
		if err != nil {
			return copied, err
		}
		if err := v.Write(ctx, buf[:n], dstOff+copied); err != nil {
			return copied, err
		}
		copied += uint64(n)
	}
	return copied, nil
}

func (v *volume) Freeze(ctx context.Context) error {
	if v.readOnly.Load() {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.timeline.Flush(ctx); err != nil {
		return err
	}
	v.readOnly.Store(true)
	return nil
}

func (v *volume) AcquireRef() error {
	for range 128 {
		n := v.refs.Load()
		if n <= 0 {
			return fmt.Errorf("volume %q is closed", v.name)
		}
		if v.refs.CompareAndSwap(n, n+1) {
			return nil
		}
	}
	panic("refs cas contention")
}

func (v *volume) ReleaseRef(ctx context.Context) error {
	if v.refs.Add(-1) == 0 {
		return v.destroy(ctx)
	}
	return nil
}

func (v *volume) destroy(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.manager.lease != nil {
		_ = v.timeline.releaseLease(ctx, v.manager.lease.Token())
	}
	v.manager.closeVolume(v.name)
	v.timeline.close(ctx)
	return nil
}

// --- manager helper ---

func (m *Manager) putVolumeRef(ctx context.Context, name string, ref volumeRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	created, err := m.volRefs.PutIfNotExists(ctx, name, data)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("volume %q already exists", name)
	}
	return nil
}
