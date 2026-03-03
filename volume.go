package loophole

import (
	"context"
	"errors"
	"fmt"
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
// AcquireRef, and drop via ReleaseRef. Whoever brings refs to 0
// triggers destroy(), which drains I/O and calls closeVolume.
// All ref operations are lock-free (atomic).
type legacyVolume struct {
	name     string
	size     uint64 // volume size in bytes; 0 = default
	readOnly atomic.Bool
	refs     atomic.Int32 // starts at 1; destroy when 0

	// layerMu protects layer swaps. Read-lock for normal I/O, write-lock for snapshot/clone (operations that swizzle the layer)
	layerMu sync.RWMutex
	layer   *Layer

	vm *legacyVolumeManager

	log *slog.Logger
}

func newVolume(name string, size uint64, layer *Layer, vm *legacyVolumeManager) *legacyVolume {
	v := &legacyVolume{
		name:  name,
		size:  size,
		layer: layer,
		vm:    vm,
		log:   slog.With("volume", name),
	}
	v.refs.Store(1)
	v.readOnly.Store(layer.Frozen())
	return v
}

var ErrVolumeClosed = errors.New("volume closed")

type volErr struct {
	op   string
	name string
	err  error
}

func (ve volErr) Error() string {
	return fmt.Sprintf("error volume=[%s] op=%s: %s", ve.name, ve.op, ve.err.Error())
}

func (ve volErr) Unwrap() error {
	return ve.err
}

func (v *legacyVolume) wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	v.log.Warn("volume error", "op", op, "err", err)
	// metrics.VolumeErrors.WithTag("volname", v.name).Inc() XXX - track
	return volErr{op: op, err: err, name: v.name}
}

func (v *legacyVolume) checkRefs() {
	if v.refs.Load() <= 0 {
		panic(fmt.Sprintf("volume %q used after destroy", v.name))
	}
}

// publish checks that the volume is still alive and accepting operations.
// Used by OpenVolume to verify a cached volume is still usable.
func (v *legacyVolume) publish() error { // XXX: this is a very weird name
	if v.refs.Load() <= 0 {
		return v.wrapErr("check_published", ErrVolumeClosed)
	}
	return nil
}

// AcquireRef pins this volume for a live kernel reference
// (inode cache or open file handle).
func (v *legacyVolume) AcquireRef() error {
	for range 128 {
		n := v.refs.Load()
		if n <= 0 {
			return v.wrapErr("acquire_ref", ErrVolumeClosed)
		}
		if v.refs.CompareAndSwap(n, n+1) {
			return nil
		}
	}
	panic("refs cas contention")
}

// ReleaseRef drops one reference acquired via AcquireRef.
func (v *legacyVolume) ReleaseRef(ctx context.Context) error {
	if v.refs.Add(-1) == 0 {
		return v.destroy(ctx)
	}
	return nil
}

// destroy drains in-flight I/O and releases the layer.
// Called by whoever brings refs to 0.
func (v *legacyVolume) destroy(ctx context.Context) error {
	v.log.Info("closing")
	v.layerMu.Lock()
	defer v.layerMu.Unlock()
	return v.wrapErr("close", v.vm.closeVolume(ctx, v)) // XXX: it is weird that this just calls back to VM with this as param
}

func (v *legacyVolume) Name() string {
	v.checkRefs()
	return v.name
}

// Size returns the volume size in bytes. Returns DefaultVolumeSize if not set.
func (v *legacyVolume) Size() uint64 {
	if v.size > 0 {
		return v.size
	}
	return DefaultVolumeSize
}

func (v *legacyVolume) ReadOnly() bool {
	v.checkRefs()
	return v.readOnly.Load()
}

func (v *legacyVolume) Read(ctx context.Context, buf []byte, offset uint64) (int, error) {
	v.checkRefs()
	v.layerMu.RLock()
	defer v.layerMu.RUnlock()
	res, err := v.layer.Read(ctx, buf, offset)
	return res, v.wrapErr("read", err)
}

func (v *legacyVolume) Write(ctx context.Context, data []byte, offset uint64) error {
	v.checkRefs()
	if v.readOnly.Load() {
		return v.wrapErr("write", ErrNotWriteable)
	}
	v.layerMu.RLock()
	defer v.layerMu.RUnlock()
	return v.wrapErr("write", v.layer.Write(ctx, data, offset))
}

func (v *legacyVolume) PunchHole(ctx context.Context, offset, length uint64) error {
	v.checkRefs()
	if v.readOnly.Load() {
		return v.wrapErr("punch_hole", ErrNotWriteable) // XXX - extract var
	}
	v.layerMu.RLock()
	defer v.layerMu.RUnlock()
	return v.wrapErr("punch_hole", v.layer.PunchHole(ctx, offset, length))
}

var ErrNotWriteable = errors.New("volume is not writeable")

// XXX: ensure and document that this context is used only to control how long the caller WAITS
// and that its cancellation DOES NOT propagate to the actual flush operation
func (v *legacyVolume) Flush(ctx context.Context) error {
	v.checkRefs()
	v.layerMu.RLock()
	defer v.layerMu.RUnlock()
	return v.wrapErr("flush", v.layer.Flush(ctx))
}

// Snapshot freezes the current layer, creates a child layer for this
// volume to continue writing to, and returns the frozen layer's ID
// as the snapshot handle. The volume remains writable afterward.
func (v *legacyVolume) Snapshot(ctx context.Context, snapshotName string) error {
	v.checkRefs()
	t := metrics.NewTimer(metrics.SnapshotDuration)
	defer t.ObserveDuration()

	v.layerMu.Lock()
	defer v.layerMu.Unlock()

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
		if err := v.vm.putVolumeRef(ctx, snapshotName, frozenLayer.id, v.size); err != nil {
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
func (v *legacyVolume) Clone(ctx context.Context, cloneName string) (Volume, error) {
	v.checkRefs()
	t := metrics.NewTimer(metrics.CloneDuration)
	defer t.ObserveDuration()

	v.layerMu.Lock()
	defer v.layerMu.Unlock()

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

	if err := v.vm.putVolumeRef(ctx, cloneName, cloneID, v.size); err != nil {
		return nil, fmt.Errorf("create clone ref: %w", err)
	}

	cloneLayer, err := v.vm.loadLayer(ctx, cloneID)
	if err != nil {
		return nil, fmt.Errorf("load clone layer: %w", err)
	}

	cloneVolume := newVolume(cloneName, v.size, cloneLayer, v.vm)
	v.vm.volumes.Store(cloneName, cloneVolume)
	metrics.OpenVolumes.Inc()
	return cloneVolume, nil
}

// CopyFrom performs a CoW copy of data from src volume into this volume.
func (v *legacyVolume) CopyFrom(ctx context.Context, src Volume, srcOff, dstOff, length uint64) (uint64, error) {
	s := src.(*legacyVolume)
	v.checkRefs()
	s.checkRefs()
	if v.readOnly.Load() {
		return 0, v.wrapErr("copy_from", ErrNotWriteable)
	}

	// lock in predictable order to avoid deadlock
	if v.name < s.name {
		v.layerMu.RLock()
		defer v.layerMu.RUnlock()
		s.layerMu.RLock()
		defer s.layerMu.RUnlock()
	} else {
		s.layerMu.RLock()
		defer s.layerMu.RUnlock()
		v.layerMu.RLock()
		defer v.layerMu.RUnlock()
	}

	return v.layer.CopyFrom(ctx, s.layer, srcOff, dstOff, length)
}

// Freeze makes this volume permanently read-only by freezing the
// underlying layer. All dirty blocks are flushed to S3 first.

// XXX - kernel may have open FDs that it think are writeable - is this really a good idea
// for us to support this operation ?
// XXX - Snapshot seems like a much better option

func (v *legacyVolume) Freeze(ctx context.Context) error {
	v.checkRefs()
	if v.readOnly.Load() {
		return nil // already frozen
	}
	v.layerMu.Lock()
	defer v.layerMu.Unlock()
	if err := v.layer.Freeze(ctx); err != nil {
		return v.wrapErr("freeze", err)
	}
	v.readOnly.Store(true)
	return nil
}

// freezeAndContinue freezes the current layer, creates a child for this
// volume to keep writing to, and swaps v.layer to the continuation.
// Returns the now-frozen old layer and the continuation layer ID.
// Caller must hold v.mu exclusively.
func (v *legacyVolume) freezeAndContinue(ctx context.Context) (frozen *Layer, continuationID string, err error) {
	if v.readOnly.Load() {
		// There is no point in snapshotting a read-only volume - it is permanently read-only
		// XXX - theoretically user could be wanting to create different names for it but overall
		// doesn't seem worth supporting although it would be easy ...
		return nil, "", v.wrapErr("freeze", ErrNotWriteable)
	}

	frozen = v.layer
	continuationID = uuid.NewString()

	if err := frozen.Freeze(ctx); err != nil {
		return nil, "", v.wrapErr("freeze", err)
	}

	if err := frozen.CreateChild(ctx, continuationID); err != nil {
		return nil, "", v.wrapErr("freeze", err)
	}

	continuation, err := v.vm.loadLayer(ctx, continuationID)
	if err != nil {
		return nil, "", v.wrapErr("freeze", err)
	}

	v.layer = continuation
	return frozen, continuationID, nil
}
