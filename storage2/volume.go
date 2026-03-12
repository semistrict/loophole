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

	directRefs int

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
	if v.directRefs > 0 {
		return fmt.Errorf("volume %q is in direct writeback mode", v.name)
	}
	return v.layer.Write(data, offset)
}

func (v *volume) PunchHole(offset, length uint64) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.directRefs > 0 {
		return fmt.Errorf("volume %q is in direct writeback mode", v.name)
	}
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
	if v.directRefs > 0 {
		return nil
	}
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
	// Flush everything to S3 (includes any pending direct L0 blobs).
	if !v.readOnly.Load() {
		if err := v.layer.Flush(); err != nil {
			return "", fmt.Errorf("flush for branch: %w", err)
		}
	}

	childID := v.manager.idGen()
	if err := v.layer.Snapshot(childID); err != nil {
		return "", fmt.Errorf("snapshot: %w", err)
	}

	// Re-layer the parent: create a new layer so the old one is never
	// written to again. Both child and new parent inherit the same
	// L0/L1/L2 objects, which are immutable since no one writes to the
	// old layer anymore. This preserves the invariant that only frozen
	// (immutable) layers are ever referenced by other layers.
	if !v.readOnly.Load() {
		if err := v.relayer(); err != nil {
			return "", fmt.Errorf("re-layer parent: %w", err)
		}
	}

	return childID, nil
}

// relayer creates a new layer for this volume, swapping out the old one.
// After this call, the old layer is effectively frozen — no one writes to it.
func (v *volume) relayer() error {
	ctx := context.Background()
	oldLayer := v.layer

	// Create a new layer with a copy of the current index.
	newID := v.manager.idGen()
	if err := oldLayer.Snapshot(newID); err != nil {
		return fmt.Errorf("create new parent layer: %w", err)
	}

	// Read the current index state for the new layer.
	oldLayer.mu.RLock()
	idx := oldLayer.index
	idx.NextSeq = oldLayer.nextSeq.Load()
	idx.L1 = oldLayer.l1Map.Ranges()
	idx.L2 = oldLayer.l2Map.Ranges()
	oldLayer.mu.RUnlock()

	// Update the volume ref to point to the new layer and get a new writeLeaseSeq.
	seq, err := v.manager.relayerVolume(ctx, v.name, newID)
	if err != nil {
		return fmt.Errorf("update volume ref: %w", err)
	}

	cacheDir := v.manager.cacheDir + "/layers/" + newID
	newLayer, err := initLayerFromIndex(v.manager.store, newID, v.manager.config, v.manager.diskCache, cacheDir, idx)
	if err != nil {
		return fmt.Errorf("init new layer: %w", err)
	}
	newLayer.writeLeaseSeq = seq

	if v.manager.config.FlushInterval > 0 {
		newLayer.startPeriodicFlush(ctx)
	}

	// Swap.
	v.layer = newLayer
	oldLayer.Close()

	slog.Info("relayer: parent switched to new layer",
		"volume", v.name, "old_layer", oldLayer.id, "new_layer", newID)
	return nil
}

func (v *volume) Snapshot(snapshotName string) error {
	slog.Warn("deprecated: use Checkpoint() instead of Snapshot()")
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

func (v *volume) Checkpoint() (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.readOnly.Load() {
		return "", fmt.Errorf("cannot checkpoint read-only volume %q", v.name)
	}

	childID, err := v.branch()
	if err != nil {
		return "", err
	}

	ts, err := v.manager.putCheckpoint(context.Background(), v.name, childID)
	if err != nil {
		return "", fmt.Errorf("create checkpoint: %w", err)
	}
	return ts, nil
}

func (v *volume) Clone(cloneName string) (loophole.Volume, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	slog.Info("volume: clone starting", "src", v.name, "dst", cloneName, "directRefs", v.directRefs)

	childID, err := v.branch()
	if err != nil {
		slog.Error("volume: clone branch failed", "src", v.name, "dst", cloneName, "error", err)
		return nil, err
	}

	ctx := context.Background()
	ref := volumeRef{LayerID: childID, Size: v.size, Type: v.volType}
	if err := v.manager.putVolumeRef(ctx, cloneName, ref); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	slog.Info("volume: clone completed", "src", v.name, "dst", cloneName, "childID", childID)
	return v.manager.OpenVolume(cloneName)
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

// CompactL0 flushes the memtable and compacts all L0 entries into L1/L2
// blocks regardless of the L0 threshold.
func (v *volume) CompactL0() error {
	v.mu.RLock()
	ly := v.layer
	v.mu.RUnlock()
	if err := ly.Flush(); err != nil {
		return fmt.Errorf("flush before compact: %w", err)
	}
	return ly.ForceCompactL0()
}

func (v *volume) Freeze() error {
	return v.freeze(false)
}

// FreezeWithCompact freezes the volume with L0→L1 compaction before marking
// it frozen. Order: fire hooks → stop writes → flush → compact → mark frozen.
func (v *volume) FreezeWithCompact() error {
	return v.freeze(true)
}

func (v *volume) freeze(compact bool) error {
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
	if v.directRefs > 0 {
		return fmt.Errorf("volume %q has active direct writeback mappings", v.name)
	}
	slog.Info("freeze: flushing layer", "volume", v.name)
	if err := v.layer.Flush(); err != nil {
		return err
	}
	if compact {
		slog.Info("freeze: compacting L0→L1", "volume", v.name)
		if err := v.layer.ForceCompactL0(); err != nil {
			return fmt.Errorf("compact before freeze: %w", err)
		}
		slog.Info("freeze: compaction complete", "volume", v.name)
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
			slog.Debug("volume: AcquireRef", "volume", v.name, "refsAfter", n+1)
			return nil
		}
	}
	panic("refs cas contention")
}

func (v *volume) ReleaseRef() error {
	newRefs := v.refs.Add(-1)
	if newRefs < 0 {
		panic(fmt.Sprintf("volume %q: ReleaseRef with refs already 0 (now %d)", v.name, newRefs))
	}
	slog.Debug("volume: ReleaseRef", "volume", v.name, "refsAfter", newRefs)
	if newRefs == 0 {
		return v.destroy()
	}
	return nil
}

func (v *volume) destroy() error {
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

func (v *volume) EnableDirectWriteback() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	if v.directRefs == 0 {
		if err := v.layer.Flush(); err != nil {
			return fmt.Errorf("flush before direct mode: %w", err)
		}
		v.layer.stopPeriodicFlush()
	}
	v.directRefs++
	return nil
}

func (v *volume) DisableDirectWriteback() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.directRefs == 0 {
		return fmt.Errorf("volume %q is not in direct writeback mode", v.name)
	}
	v.directRefs--
	if v.directRefs == 0 && v.manager.config.FlushInterval > 0 {
		v.layer.startPeriodicFlush(context.Background())
	}
	return nil
}

func (v *volume) WritePagesDirect(pages []loophole.DirectPage) error {
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.directRefs == 0 {
		return fmt.Errorf("volume %q is not in direct writeback mode", v.name)
	}
	return v.layer.WritePagesDirectL0(pages)
}

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
