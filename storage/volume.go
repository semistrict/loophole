package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/semistrict/loophole/objstore"
)

type Volume struct {
	name     string
	size     uint64
	volType  string
	refs     atomic.Int32
	readOnly bool

	mu    sync.RWMutex
	layer *layer

	directRefs      int
	manager         *Manager
	lease           *objstore.LeaseSession
	leaseCloseOnce  sync.Once
	onRemoteRelease func(ctx context.Context)
}

func newVolume(name string, size uint64, volType string, ly *layer, m *Manager) *Volume {
	if size == 0 {
		size = DefaultVolumeSize
	}
	v := &Volume{
		name:    name,
		size:    size,
		volType: volType,
		layer:   ly,
		manager: m,
		lease:   objstore.NewLeaseSession(m.store.At("leases")),
	}
	v.lease.Handle("release", v.handleLeaseRelease)
	v.refs.Store(1)
	return v
}

func (v *Volume) Name() string { return v.name }
func (v *Volume) Size() uint64 { return v.size }

func (v *Volume) VolumeType() string { return v.volType }

func (v *Volume) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Read(ctx, buf, offset)
}

// ReadPages collects per-page zero-copy slices for the given byte range.
// Each slice points directly into mmap'd memory. The returned cleanup
// function MUST be called when the caller is done with all slices
// (e.g. after writev completes). The caller must not modify the slices.
func (v *Volume) ReadPages(ctx context.Context, offset uint64, length int) ([][]byte, func(), error) {
	v.mu.RLock()
	g := v.manager.safepoint.Enter()
	slices, err := v.layer.ReadPages(ctx, g, offset, length)
	if err != nil {
		g.Exit()
		v.mu.RUnlock()
		return nil, nil, err
	}
	return slices, func() {
		g.Exit()
		v.mu.RUnlock()
	}, nil
}

func (v *Volume) ReadAt(ctx context.Context, offset uint64, n int) ([]byte, error) {
	buf := make([]byte, n)
	v.mu.RLock()
	got, err := v.layer.Read(ctx, buf, offset)
	v.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return buf[:got], nil
}

func (v *Volume) Write(data []byte, offset uint64) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.readOnly {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	if v.directRefs > 0 {
		return fmt.Errorf("volume %q is in direct writeback mode", v.name)
	}
	return v.layer.Write(data, offset)
}

func (v *Volume) PunchHole(offset, length uint64) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.readOnly {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	if v.directRefs > 0 {
		return fmt.Errorf("volume %q is in direct writeback mode", v.name)
	}
	return v.layer.PunchHole(offset, length)
}

func (v *Volume) ZeroRange(offset, length uint64) error {
	return v.PunchHole(offset, length)
}

func (v *Volume) Flush() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.readOnly {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	return v.layer.Flush()
}

// FlushLocal notifies the background flush loop to upload pending data
// without blocking. If no background loop is running, this is a no-op.
// Suitable for FUSE fsync where we don't want to block on S3.
func (v *Volume) FlushLocal() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.readOnly {
		return nil
	}
	if v.directRefs > 0 {
		return nil
	}
	if v.layer.flushNotify != nil {
		select {
		case v.layer.flushNotify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (v *Volume) branch() (string, error) {
	if v.readOnly {
		return "", fmt.Errorf("volume %q is read-only", v.name)
	}
	if err := v.layer.Flush(); err != nil {
		return "", fmt.Errorf("flush for branch: %w", err)
	}

	childID := newLayerID()
	if err := v.layer.Snapshot(childID); err != nil {
		return "", fmt.Errorf("snapshot: %w", err)
	}

	if err := v.relayer(); err != nil {
		return "", fmt.Errorf("re-layer parent: %w", err)
	}

	return childID, nil
}

// relayer creates a new layer for this volume, swapping out the old one.
// After this call, the old layer is effectively frozen. No further writes
// target it, which keeps shared ancestry immutable for snapshots/checkpoints.
func (v *Volume) relayer() error {
	ctx := context.Background()
	oldLayer := v.layer

	newID := newLayerID()
	if err := oldLayer.Snapshot(newID); err != nil {
		return fmt.Errorf("create new parent layer: %w", err)
	}

	oldLayer.mu.RLock()
	idx := oldLayer.index
	idx.NextSeq = oldLayer.nextSeq.Load()
	idx.L1 = oldLayer.l1Map.Ranges()
	idx.L2 = oldLayer.l2Map.Ranges()
	oldLayer.mu.RUnlock()

	seq, err := relayerVolumeRef(ctx, v.manager.volRefs, v.name, newID)
	if err != nil {
		return fmt.Errorf("update volume ref: %w", err)
	}

	newLayer, err := initLayerFromIndex(layerParams{
		store:     v.manager.store,
		id:        newID,
		config:    v.manager.config,
		diskCache: v.manager.diskCache,
		safepoint: v.manager.safepoint,
	}, idx)
	if err != nil {
		return fmt.Errorf("init new layer: %w", err)
	}
	newLayer.writeLeaseSeq = seq
	v.layer = newLayer
	oldLayer.Close()

	slog.Info("relayer: parent switched to new layer",
		"volume", v.name, "old_layer", oldLayer.id, "new_layer", newID)
	return nil
}

func (v *Volume) Checkpoint() (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	childID, err := v.branch()
	if err != nil {
		return "", err
	}

	ts, err := putCheckpoint(context.Background(), v.manager.volRefs, v.name, childID)
	if err != nil {
		return "", fmt.Errorf("create checkpoint: %w", err)
	}
	return ts, nil
}

func (v *Volume) Clone(cloneName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	slog.Info("volume: clone starting", "src", v.name, "dst", cloneName, "directRefs", v.directRefs)

	childID, err := v.branch()
	if err != nil {
		slog.Error("volume: clone branch failed", "src", v.name, "dst", cloneName, "error", err)
		return err
	}

	ref := volumeRef{LayerID: childID, Size: v.size, Type: v.volType}
	if err := putVolumeRef(context.Background(), v.manager.volRefs, cloneName, ref); err != nil {
		return fmt.Errorf("create clone ref: %w", err)
	}

	slog.Info("volume: clone completed", "src", v.name, "dst", cloneName, "childID", childID)
	return nil
}

func (v *Volume) CopyFrom(src *Volume, srcOff, dstOff, length uint64) (uint64, error) {
	if v.readOnly {
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
			verify, readErr := v.ReadAt(ctx, dstOff+copied, n)
			if readErr == nil && len(verify) == n && bytes.Equal(verify, buf[:n]) {
				return copied + uint64(n), err
			}
			return copied, err
		}
		copied += uint64(n)
	}
	return copied, nil
}

func (v *Volume) Refresh(ctx context.Context) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.refresh(ctx)
}

func (v *Volume) AcquireRef() error {
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

func (v *Volume) ReleaseRef() error {
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

func (v *Volume) destroy() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.layer.Flush(); err != nil {
		slog.Warn("flush on destroy failed", "volume", v.name, "error", err)
	}
	v.layer.beginShutdown()
	v.releaseLease(context.Background())
	v.manager.closeVolume(v.name)
	v.layer.Close()
	v.closeLeaseSession(context.Background())
	return nil
}

func (v *Volume) flush() error { return v.layer.Flush() }
func (v *Volume) close() {
	v.layer.Close()
	v.closeLeaseSession(context.Background())
}

func (v *Volume) SetOnRemoteRelease(fn func(ctx context.Context)) {
	v.onRemoteRelease = fn
}

func (v *Volume) leaseToken() string {
	return v.lease.Token()
}

func (v *Volume) acquireLease(ctx context.Context) (uint64, error) {
	if err := v.lease.EnsureStarted(ctx); err != nil {
		return 0, fmt.Errorf("start lease: %w", err)
	}
	key, err := volumeIndexKey(v.name)
	if err != nil {
		return 0, err
	}
	var seq uint64
	err = objstore.ModifyJSON[volumeRef](ctx, v.manager.volRefs, key, func(ref *volumeRef) error {
		if err := v.lease.CheckAvailable(ctx, ref.LeaseToken); err != nil {
			return fmt.Errorf("volume %s: %w", v.name, err)
		}
		ref.LeaseToken = v.lease.Token()
		ref.WriteLeaseSeq++
		seq = ref.WriteLeaseSeq
		return nil
	})
	return seq, err
}

func (v *Volume) releaseLease(ctx context.Context) {
	key, err := volumeIndexKey(v.name)
	if err != nil {
		slog.Warn("release volume lease", "volume", v.name, "error", err)
		return
	}
	if err := objstore.ModifyJSON[volumeRef](ctx, v.manager.volRefs, key, func(ref *volumeRef) error {
		if ref.LeaseToken == v.lease.Token() {
			ref.LeaseToken = ""
		}
		return nil
	}); err != nil {
		if errors.Is(err, objstore.ErrNotFound) {
			return
		}
		slog.Warn("release volume lease", "volume", v.name, "error", err)
	}
}

func (v *Volume) closeLeaseSession(ctx context.Context) {
	v.leaseCloseOnce.Do(func() {
		if err := v.lease.Close(ctx); err != nil {
			slog.Warn("close lease session", "volume", v.name, "error", err)
		}
	})
}

func (v *Volume) handleLeaseRelease(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("decode release params: %w", err)
	}
	if req.Volume != "" && req.Volume != v.name {
		return nil, fmt.Errorf("release requested for %q on volume %q", req.Volume, v.name)
	}

	slog.Info("release: releasing volume", "volume", v.name)
	if v.onRemoteRelease != nil {
		v.onRemoteRelease(ctx)
	}
	v.manager.closeVolume(v.name)
	v.layer.beginShutdown()
	if err := v.flush(); err != nil {
		slog.Warn("release: flush failed", "volume", v.name, "error", err)
	}
	v.releaseLease(ctx)
	v.close()
	return map[string]string{"status": "ok"}, nil
}

func (v *Volume) EnableDirectWriteback() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.readOnly {
		return fmt.Errorf("volume %q is read-only", v.name)
	}

	if v.directRefs == 0 {
		if err := v.layer.Flush(); err != nil {
			return fmt.Errorf("flush before direct mode: %w", err)
		}
	}
	v.directRefs++
	return nil
}

func (v *Volume) DisableDirectWriteback() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.directRefs == 0 {
		return fmt.Errorf("volume %q is not in direct writeback mode", v.name)
	}
	v.directRefs--
	return nil
}

func (v *Volume) WritePagesDirect(pages []DirectPage) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.readOnly {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	if v.directRefs == 0 {
		return fmt.Errorf("volume %q is not in direct writeback mode", v.name)
	}
	for _, p := range pages {
		if err := v.layer.Write(p.Data, p.Offset); err != nil {
			return err
		}
	}
	return nil
}

// VolumeDebugInfo holds volume + layer structure details for the debug endpoint.
type VolumeDebugInfo struct {
	Name  string         `json:"name"`
	Size  uint64         `json:"size"`
	Type  string         `json:"type"`
	Refs  int32          `json:"refs"`
	Layer LayerDebugInfo `json:"layer"`
}

func (v *Volume) DebugInfo() VolumeDebugInfo {
	return VolumeDebugInfo{
		Name:  v.name,
		Size:  v.size,
		Type:  v.volType,
		Refs:  v.refs.Load(),
		Layer: v.layer.debugInfo(),
	}
}

// ListCheckpoints lists checkpoints for this volume (convenience for Manager.ListCheckpoints).
func (v *Volume) ListCheckpoints(ctx context.Context) ([]CheckpointInfo, error) {
	return ListCheckpoints(ctx, v.manager.Store(), v.name)
}

// CloneFromCheckpoint creates a clone from a checkpoint of this volume
// (convenience for Manager.CloneFromCheckpoint).
func (v *Volume) CloneFromCheckpoint(ctx context.Context, checkpointID, cloneName string) error {
	return CloneFromCheckpoint(ctx, v.manager.Store(), v.name, checkpointID, cloneName)
}

// Info returns metadata for this volume.
func (v *Volume) Info(ctx context.Context) (VolumeInfo, error) {
	return GetVolumeInfo(ctx, v.manager.Store(), v.name)
}
