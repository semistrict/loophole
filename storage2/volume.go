//go:build !js

package storage2

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/metrics"
)

var _ loophole.Volume = (*volume)(nil)

type volume struct {
	volumeHooks

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

var _ managedVolume = (*volume)(nil)

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
	pageIdx, pageOff := PageIdxOf(offset)

	// Fast path: single full page, page-aligned — zero-copy.
	if pageOff == 0 && n == PageSize {
		metrics.ReadAtZeroCopy.Inc()
		v.mu.RLock()
		snap := v.layer.snapshotLayers()
		data, release, err := v.layer.readPagePinned(ctx, &snap, pageIdx)
		v.mu.RUnlock()
		if err != nil {
			return nil, nil, err
		}
		return data, release, nil
	}

	// Slow path: sub-page, cross-page, or non-aligned — allocate and copy.
	metrics.ReadAtCopy.Inc()
	buf := make([]byte, n)
	v.mu.RLock()
	got, err := v.layer.Read(ctx, buf, offset)
	v.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	return buf[:got], func() {}, nil
}

func (v *volume) Write(data []byte, offset uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Write(data, offset)
}

func (v *volume) PunchHole(offset, length uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.PunchHole(offset, length)
}

func (v *volume) ZeroRange(offset, length uint64) error {
	return v.PunchHole(offset, length)
}

func (v *volume) Flush() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Flush()
}

// FlushLocal notifies the background flush loop to upload pending data
// without blocking. If no background loop is running, this is a no-op.
// Suitable for FUSE fsync where we don't want to block on S3.
func (v *volume) FlushLocal() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ly := v.layer
	if ly.flushNotify != nil {
		select {
		case ly.flushNotify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (v *volume) branch() (string, error) {
	// Flush everything to S3.
	if !v.readOnly.Load() {
		if err := v.layer.Flush(); err != nil {
			return "", fmt.Errorf("flush for branch: %w", err)
		}
	}

	childID := v.manager.idGen()
	if err := v.layer.Snapshot(childID); err != nil {
		return "", fmt.Errorf("snapshot: %w", err)
	}
	return childID, nil
}

func (v *volume) Snapshot(snapshotName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.readOnly.Load() {
		return fmt.Errorf("cannot snapshot read-only volume %q", v.name)
	}

	childID, err := v.branch()
	if err != nil {
		return err
	}

	if snapshotName != "" {
		ref := volumeRef{LayerID: childID, Size: v.size, ReadOnly: true, Type: v.volType}
		if err := v.manager.putVolumeRef(context.Background(), snapshotName, ref); err != nil {
			return fmt.Errorf("create snapshot ref: %w", err)
		}
	}
	return nil
}

func (v *volume) Clone(cloneName string) (loophole.Volume, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	childID, err := v.branch()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ref := volumeRef{LayerID: childID, Size: v.size, Type: v.volType}
	if err := v.manager.putVolumeRef(ctx, cloneName, ref); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	return v.manager.OpenVolume(ctx, cloneName)
}

func (v *volume) CopyFrom(src loophole.Volume, srcOff, dstOff, length uint64) (uint64, error) {
	if v.readOnly.Load() {
		return 0, fmt.Errorf("volume %q is read-only", v.name)
	}

	ctx := context.Background()
	var copied uint64
	buf := make([]byte, PageSize)
	for copied < length {
		chunk := min(length-copied, PageSize)
		n, err := src.Read(ctx, buf[:chunk], srcOff+copied)
		if err != nil {
			return copied, err
		}
		if err := v.Write(buf[:n], dstOff+copied); err != nil {
			return copied + uint64(n), err
		}
		copied += uint64(n)
	}
	return copied, nil
}

func (v *volume) Freeze() error {
	if v.readOnly.Load() {
		return nil
	}
	// Fire hooks before acquiring the write lock — hooks may need to
	// write to the volume (e.g. FS cache flush) which requires RLock.
	slog.Info("freeze: firing before-freeze hooks", "volume", v.name)
	if err := v.fireBeforeFreeze(); err != nil {
		return fmt.Errorf("before-freeze hook: %w", err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	slog.Info("freeze: flushing layer", "volume", v.name)
	if err := v.layer.Flush(); err != nil {
		return err
	}
	slog.Info("freeze: flush complete, checking metadata", "volume", v.name)
	ctx := context.Background()
	// Check current metadata to ensure not already frozen.
	meta, err := v.layer.layerStore.HeadMeta(ctx, "index.json")
	if err != nil {
		return fmt.Errorf("read layer metadata: %w", err)
	}
	if meta == nil {
		meta = make(map[string]string)
	}
	if meta["frozen_at"] != "" {
		return fmt.Errorf("layer %q is already frozen (at %s)", v.layer.id, meta["frozen_at"])
	}
	// Set frozen_at in object metadata (no body rewrite).
	now := time.Now().UTC().Format(time.RFC3339)
	meta["frozen_at"] = now
	slog.Info("freeze: setting frozen_at metadata", "volume", v.name, "frozen_at", now)
	if err := v.layer.layerStore.SetMeta(ctx, "index.json", meta); err != nil {
		return fmt.Errorf("set frozen metadata: %w", err)
	}
	v.readOnly.Store(true)
	slog.Info("freeze: done", "volume", v.name)
	return nil
}

func (v *volume) Refresh(ctx context.Context) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.refresh(ctx)
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

func (v *volume) ReleaseRef() error {
	if v.refs.Add(-1) == 0 {
		return v.destroy()
	}
	return nil
}

func (v *volume) destroy() error {
	v.fireBeforeClose()
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.readOnly.Load() {
		if err := v.layer.Flush(); err != nil {
			slog.Warn("flush on destroy failed", "volume", v.name, "error", err)
		}
		v.manager.releaseVolumeLease(context.Background(), v.name)
	}
	v.manager.closeVolume(v.name)
	v.layer.Close()
	return nil
}

func (v *volume) isReadOnly() bool { return v.readOnly.Load() }
func (v *volume) flush() error     { return v.layer.Flush() }
func (v *volume) close()           { v.layer.Close() }

// VolumeDebugInfo holds volume + layer structure details for the debug endpoint.
type VolumeDebugInfo struct {
	Name     string         `json:"name"`
	Size     uint64         `json:"size"`
	Type     string         `json:"type"`
	ReadOnly bool           `json:"read_only"`
	Refs     int32          `json:"refs"`
	Layer    LayerDebugInfo `json:"layer"`
}

func (v *volume) DebugInfo() VolumeDebugInfo {
	return VolumeDebugInfo{
		Name:     v.name,
		Size:     v.size,
		Type:     v.volType,
		ReadOnly: v.readOnly.Load(),
		Refs:     v.refs.Load(),
		Layer:    v.layer.debugInfo(),
	}
}
