//go:build !js

package storage2

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/semistrict/loophole"
)

var _ loophole.Volume = (*volume)(nil)

type volume struct {
	name     string
	size     uint64
	volType  string
	readOnly atomic.Bool
	refs     atomic.Int32

	mu    sync.RWMutex
	layer *layer

	manager *Manager
}

func newVolume(name string, size uint64, volType string, ly *layer, m *Manager) *volume {
	if size == 0 {
		size = DefaultVolumeSize
	}
	v := &volume{
		name:    name,
		size:    size,
		volType: volType,
		layer:   ly,
		manager: m,
	}
	v.refs.Store(1)
	return v
}

func (v *volume) Name() string       { return v.name }
func (v *volume) Size() uint64       { return v.size }
func (v *volume) ReadOnly() bool     { return v.readOnly.Load() }
func (v *volume) VolumeType() string { return v.volType }

func (v *volume) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Read(ctx, buf, offset)
}

func (v *volume) ReadAt(ctx context.Context, offset uint64, n int) ([]byte, func(), error) {
	// No zero-copy path yet — allocate and copy.
	buf := make([]byte, n)
	v.mu.RLock()
	got, err := v.layer.Read(ctx, buf, offset)
	v.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	return buf[:got], func() {}, nil
}

func (v *volume) Write(ctx context.Context, data []byte, offset uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Write(ctx, data, offset)
}

func (v *volume) PunchHole(ctx context.Context, offset, length uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.PunchHole(ctx, offset, length)
}

func (v *volume) ZeroRange(ctx context.Context, offset, length uint64) error {
	return v.PunchHole(ctx, offset, length)
}

func (v *volume) Flush(ctx context.Context) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Flush(ctx)
}

func (v *volume) branch(ctx context.Context) (string, error) {
	// Flush everything to S3.
	if !v.readOnly.Load() {
		if err := v.layer.Flush(ctx); err != nil {
			return "", fmt.Errorf("flush for branch: %w", err)
		}
	}

	childID := v.manager.idGen()
	if err := v.layer.Snapshot(ctx, childID); err != nil {
		return "", fmt.Errorf("snapshot: %w", err)
	}
	return childID, nil
}

func (v *volume) Snapshot(ctx context.Context, snapshotName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.readOnly.Load() {
		return fmt.Errorf("cannot snapshot read-only volume %q", v.name)
	}

	childID, err := v.branch(ctx)
	if err != nil {
		return err
	}

	if snapshotName != "" {
		ref := volumeRef{TimelineID: childID, Size: v.size, ReadOnly: true, Type: v.volType}
		if err := v.manager.putVolumeRef(ctx, snapshotName, ref); err != nil {
			return fmt.Errorf("create snapshot ref: %w", err)
		}
	}
	return nil
}

func (v *volume) Clone(ctx context.Context, cloneName string) (loophole.Volume, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	childID, err := v.branch(ctx)
	if err != nil {
		return nil, err
	}

	ref := volumeRef{TimelineID: childID, Size: v.size, Type: v.volType}
	if err := v.manager.putVolumeRef(ctx, cloneName, ref); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	return v.manager.OpenVolume(ctx, cloneName)
}

func (v *volume) CopyFrom(ctx context.Context, src loophole.Volume, srcOff, dstOff, length uint64) (uint64, error) {
	if v.readOnly.Load() {
		return 0, fmt.Errorf("volume %q is read-only", v.name)
	}

	var copied uint64
	buf := make([]byte, PageSize)
	for copied < length {
		chunk := min(length-copied, PageSize)
		n, err := src.Read(ctx, buf[:chunk], srcOff+copied)
		if err != nil {
			return copied, err
		}
		if err := v.Write(ctx, buf[:n], dstOff+copied); err != nil {
			return copied + uint64(n), err
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
	if err := v.layer.Flush(ctx); err != nil {
		return err
	}
	v.readOnly.Store(true)
	return nil
}

func (v *volume) Refresh(ctx context.Context) error {
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
	v.manager.closeVolume(v.name)
	v.layer.Close()
	return nil
}
