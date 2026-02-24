package loophole

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/semistrict/loophole/metrics"
)

// Volume is a named, mountable unit backed by a Layer.
// If the volume is writable, writes go to its layer.
// If read-only (a mounted snapshot), the layer is only used for reads.
//
// Reference counting: refs starts at 1 (the namespace ref held by
// VolumeManager). FUSE inode lookups and open file handles add +1 via
// AcquireRef. Close() drops the namespace ref (-1); ReleaseRef() drops
// a FUSE ref (-1). Whoever brings refs to 0 drains I/O and calls
// closeVolume. All ref operations are lock-free (atomic).
type Volume struct {
	name     string
	readOnly atomic.Bool
	refs     atomic.Int32 // starts at 1; destroy when 0
	closed   atomic.Bool  // true after Close(); prevents double-decrement

	// mu protects layer swaps. Read-lock for normal I/O, write-lock for snapshot/clone.
	mu    sync.RWMutex
	layer *Layer

	vm *VolumeManager
}

var ErrVolumeClosed = errors.New("volume closed")

func (v *Volume) mustBeOpen() {
	if v.refs.Load() <= 0 {
		panic(fmt.Sprintf("volume %q used after Close()", v.name))
	}
}

// publish checks that the volume is still alive and accepting operations.
// Used by OpenVolume to verify a cached volume is still usable.
func (v *Volume) publish() error {
	if v.closed.Load() || v.refs.Load() <= 0 {
		return ErrVolumeClosed
	}
	return nil
}

// AcquireRef pins this volume for a live kernel reference
// (inode cache or open file handle).
func (v *Volume) AcquireRef() error {
	for {
		n := v.refs.Load()
		if n <= 0 {
			return ErrVolumeClosed
		}
		if v.refs.CompareAndSwap(n, n+1) {
			return nil
		}
	}
}

// ReleaseRef drops one reference acquired via AcquireRef.
func (v *Volume) ReleaseRef(ctx context.Context) error {
	if v.refs.Add(-1) == 0 {
		return v.destroy(ctx)
	}
	return nil
}

// Close drops the namespace ref (the +1 from creation). Idempotent.
func (v *Volume) Close(ctx context.Context) error {
	if !v.closed.CompareAndSwap(false, true) {
		return nil
	}
	if v.refs.Add(-1) == 0 {
		return v.destroy(ctx)
	}
	return nil
}

// destroy drains in-flight I/O and releases the layer.
// Called by whoever brings refs to 0.
func (v *Volume) destroy(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.vm.closeVolume(ctx, v)
}

func (v *Volume) Name() string {
	v.mustBeOpen()
	return v.name
}

func (v *Volume) ReadOnly() bool {
	v.mustBeOpen()
	return v.readOnly.Load()
}

// VolumeIO wraps a Volume with a fixed context, implementing io.ReaderAt
// and io.WriterAt. Use this to pass a Volume to interfaces that require
// those (e.g. lwext4 CGO block device callbacks).
type VolumeIO struct {
	ctx context.Context
	vol *Volume
}

var (
	_ io.ReaderAt = (*VolumeIO)(nil)
	_ io.WriterAt = (*VolumeIO)(nil)
)

// IO returns a VolumeIO that binds ctx to all reads and writes through
// the io.ReaderAt / io.WriterAt interfaces.
func (v *Volume) IO(ctx context.Context) *VolumeIO {
	v.mustBeOpen()
	return &VolumeIO{ctx: ctx, vol: v}
}

func (vio *VolumeIO) ReadAt(p []byte, off int64) (int, error) {
	return vio.vol.Read(vio.ctx, uint64(off), p)
}

func (vio *VolumeIO) WriteAt(p []byte, off int64) (int, error) {
	if err := vio.vol.Write(vio.ctx, uint64(off), p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (vio *VolumeIO) Sync() error {
	err := vio.vol.Flush(vio.ctx)
	if err != nil {
		slog.Error("VolumeIO.Sync failed", "volume", vio.vol.name, "error", err)
	}
	return err
}

func (vio *VolumeIO) Trim(offset, length int64) error {
	return vio.vol.PunchHole(vio.ctx, uint64(offset), uint64(length))
}

func (vio *VolumeIO) WriteZeroes(offset, length int64, _ bool) error {
	return vio.vol.PunchHole(vio.ctx, uint64(offset), uint64(length))
}

func (v *Volume) Read(ctx context.Context, offset uint64, buf []byte) (int, error) {
	v.mustBeOpen()
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Read(ctx, offset, buf)
}

func (v *Volume) Write(ctx context.Context, offset uint64, data []byte) error {
	v.mustBeOpen()
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Write(ctx, offset, data)
}

func (v *Volume) PunchHole(ctx context.Context, offset, length uint64) error {
	v.mustBeOpen()
	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.PunchHole(ctx, offset, length)
}

func (v *Volume) Flush(ctx context.Context) error {
	v.mustBeOpen()
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.layer.Flush(ctx)
}

// Snapshot freezes the current layer, creates a child layer for this
// volume to continue writing to, and returns the frozen layer's ID
// as the snapshot handle. The volume remains writable afterward.
func (v *Volume) Snapshot(ctx context.Context, snapshotName string) error {
	v.mustBeOpen()
	t := metrics.NewTimer(metrics.SnapshotDuration)
	defer t.ObserveDuration()

	if v.readOnly.Load() {
		return fmt.Errorf("volume %q is read-only", v.name)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	frozenLayer, continuationID, err := v.freezeAndContinue(ctx)
	if err != nil {
		return err
	}

	if err := v.vm.updateVolumeRef(ctx, v.name, continuationID); err != nil {
		if closeErr := frozenLayer.Close(ctx); closeErr != nil {
			slog.Warn("close frozen layer during error recovery", "layer", frozenLayer.id, "error", closeErr)
		}
		return fmt.Errorf("update volume ref: %w", err)
	}

	// Named snapshots become independent read-only volumes
	// that point at the just-frozen layer.
	if snapshotName != "" {
		if err := v.vm.putVolumeRef(ctx, snapshotName, frozenLayer.id); err != nil {
			if closeErr := frozenLayer.Close(ctx); closeErr != nil {
				slog.Warn("close frozen layer during error recovery", "layer", frozenLayer.id, "error", closeErr)
			}
			return fmt.Errorf("create snapshot ref: %w", err)
		}
	}

	if err := frozenLayer.Close(ctx); err != nil {
		return fmt.Errorf("close frozen layer: %w", err)
	}

	return nil
}

// Clone creates a writable copy of this volume.
//
// On a read-only volume the layer is already frozen, so Clone just
// creates a single child — no snapshot needed.
//
// On a mutable volume Clone first snapshots (freeze + continuation),
// then clones the newly-frozen layer.
func (v *Volume) Clone(ctx context.Context, cloneName string) (*Volume, error) {
	v.mustBeOpen()
	t := metrics.NewTimer(metrics.CloneDuration)
	defer t.ObserveDuration()

	v.mu.Lock()
	defer v.mu.Unlock()

	frozenLayer := v.layer

	if !v.readOnly.Load() {
		var continuationID string
		var err error
		frozenLayer, continuationID, err = v.freezeAndContinue(ctx)
		if err != nil {
			return nil, err
		}

		if err := v.vm.updateVolumeRef(ctx, v.name, continuationID); err != nil {
			return nil, fmt.Errorf("update volume ref: %w", err)
		}
	}

	cloneID := uuid.NewString()
	if err := frozenLayer.CreateChild(ctx, cloneID); err != nil {
		return nil, fmt.Errorf("create clone: %w", err)
	}

	if !v.readOnly.Load() {
		if err := frozenLayer.Close(ctx); err != nil {
			return nil, fmt.Errorf("close frozen layer: %w", err)
		}
	}

	if err := v.vm.putVolumeRef(ctx, cloneName, cloneID); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	cloneLayer, err := v.vm.loadLayer(ctx, cloneID)
	if err != nil {
		return nil, fmt.Errorf("load clone layer: %w", err)
	}

	cloneVolume := &Volume{
		name:  cloneName,
		layer: cloneLayer,
		vm:    v.vm,
	}
	cloneVolume.refs.Store(1)

	v.vm.volumes.Store(cloneName, cloneVolume)
	metrics.OpenVolumes.Inc()
	return cloneVolume, nil
}

// CopyFrom performs a CoW copy of data from src volume into this volume.
func (v *Volume) CopyFrom(ctx context.Context, src *Volume, srcOff, dstOff, length uint64) (uint64, error) {
	v.mustBeOpen()
	src.mustBeOpen()
	if v.readOnly.Load() {
		return 0, fmt.Errorf("volume %q is read-only", v.name)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	src.mu.RLock()
	defer src.mu.RUnlock()
	return v.layer.CopyFrom(ctx, src.layer, srcOff, dstOff, length)
}

// Freeze makes this volume permanently read-only by freezing the
// underlying layer. All dirty blocks are flushed to S3 first.
func (v *Volume) Freeze(ctx context.Context) error {
	v.mustBeOpen()
	if v.readOnly.Load() {
		return nil // already frozen
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.layer.Freeze(ctx); err != nil {
		return err
	}
	v.readOnly.Store(true)
	return nil
}

// freezeAndContinue freezes the current layer, creates a child for this
// volume to keep writing to, and swaps v.layer to the continuation.
// Returns the now-frozen old layer and the continuation layer ID.
// Caller must hold v.mu exclusively.
func (v *Volume) freezeAndContinue(ctx context.Context) (frozen *Layer, continuationID string, err error) {
	frozen = v.layer
	continuationID = uuid.NewString()

	if err := frozen.Freeze(ctx); err != nil {
		return nil, "", fmt.Errorf("freeze: %w", err)
	}

	if err := frozen.CreateChild(ctx, continuationID); err != nil {
		return nil, "", fmt.Errorf("create continuation: %w", err)
	}

	continuation, err := v.vm.loadLayer(ctx, continuationID)
	if err != nil {
		return nil, "", fmt.Errorf("load continuation: %w", err)
	}

	v.layer = continuation
	return frozen, continuationID, nil
}
