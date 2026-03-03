package loophole

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"

	"github.com/semistrict/loophole/metrics"
)

// ErrVolumeExists is returned when creating a volume that already exists.
var ErrVolumeExists = errors.New("volume already exists")

// SystemState is the root-level state.json for the entire system.
type SystemState struct {
	BlockSize uint64 `json:"block_size"`
}

type volumeRef struct { // XXX: volumeRef is a weird name for this, maybe volumeInfo ?
	LayerID string `json:"layer_id"`       // XXX: rename this to topLayer
	Size    string `json:"size,omitempty"` // volume size in bytes as string; empty = default (100 GB)
}

// VolumeManager creates and tracks open volumes.
// Set public fields before calling Connect.
type legacyVolumeManager struct {
	Store          ObjectStore // root of the bucket/prefix
	CacheDir       string
	MaxUploads     int // default 20
	MaxDownloads   int // default 200
	MaxDirtyBlocks int // default 100; global cap on dirty blocks across all layers

	id    string
	lease *LeaseManager

	layers          ObjectStore // Store.At("layers")
	volumeRefs      ObjectStore // Store.At("volumes")
	cache           *BlockCache
	BlockDownloader        // XXX: do not embed like this, make unexported and do bd blockDownloader
	mutableDir      string // per-process mutable block files, wiped on startup
	blockSize       uint64

	uploadSem *semaphore.Weighted
	flushPool sync.Pool // XXX: what are the elements? extract a wrapper struct here to make it clear

	openLayers SyncMap[string, *Layer] // XXX: unclear to me why we need to track layers independently of volumes
	volumes    SyncMap[string, *legacyVolume]
}

// NewLegacyVolumeManager creates and connects a legacy VolumeManager.
func NewLegacyVolumeManager(ctx context.Context, store ObjectStore, cacheDir string) (VolumeManager, error) {
	vm := &legacyVolumeManager{Store: store, CacheDir: cacheDir}
	if err := vm.Connect(ctx); err != nil {
		return nil, err
	}
	return vm, nil
}

// FormatSystem writes the root state.json with the system-wide block size.
// Must be called once before VolumeManager.Connect on a fresh store.
func FormatSystem(ctx context.Context, base ObjectStore, blockSize uint64) error {
	state := SystemState{BlockSize: blockSize}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	created, err := base.PutIfNotExists(ctx, "state.json", data)
	if err != nil {
		return fmt.Errorf("write system state: %w", err)
	}
	if !created {
		return fmt.Errorf("system already formatted")
	}
	return nil
}

// Connect reads the root state.json and initializes the system.
// Zero-valued public fields are replaced with defaults.
func (vm *legacyVolumeManager) Connect(ctx context.Context) error {
	if vm.MaxUploads == 0 {
		vm.MaxUploads = DefaultMaxUploads
	}
	if vm.MaxDownloads == 0 {
		vm.MaxDownloads = DefaultMaxDownloads
	}
	if vm.MaxDirtyBlocks == 0 {
		vm.MaxDirtyBlocks = DefaultMaxDirtyBlocks
	}

	state, _, err := ReadJSON[SystemState](ctx, vm.Store, "state.json")
	if err != nil {
		return fmt.Errorf("read system state: %w (has FormatSystem been called?)", err)
	}

	vm.layers = vm.Store.At("layers")
	vm.BlockDownloader = BlockDownloader{
		blockSize: state.BlockSize,
		sem:       semaphore.NewWeighted(int64(vm.MaxDownloads)),
	}
	vm.cache, err = NewBlockCache(vm.CacheDir, vm.layers, &vm.BlockDownloader)
	if err != nil {
		return fmt.Errorf("init block cache: %w", err)
	}

	// Each startup gets a fresh mutable dir with a random name so we
	// never read stale data even if cleanup of old dirs fails.
	mutableBase := filepath.Join(vm.CacheDir, "mutable")
	if err := os.RemoveAll(mutableBase); err != nil {
		slog.Warn("remove old mutable dirs", "dir", mutableBase, "error", err)
	}
	vm.mutableDir = filepath.Join(mutableBase, uuid.NewString())
	if err := os.MkdirAll(vm.mutableDir, 0o755); err != nil {
		return fmt.Errorf("create mutable dir: %w", err)
	}

	hostname, _ := os.Hostname()
	vm.lease = NewLeaseManager(vm.Store.At("leases"))
	vm.id = fmt.Sprintf("%s:%d:%s", hostname, os.Getpid(), vm.lease.Token())

	vm.blockSize = state.BlockSize
	vm.volumeRefs = vm.Store.At("volumes")
	vm.uploadSem = semaphore.NewWeighted(int64(vm.MaxUploads))
	vm.flushPool = sync.Pool{
		New: func() any {
			b := make([]byte, vm.blockSize)
			return &b
		},
	}
	return nil
}

// NewVolume creates a new volume with an empty layer in S3.
// Size is the volume size in bytes; 0 means use DefaultVolumeSize.
func (vm *legacyVolumeManager) NewVolume(ctx context.Context, name string, size uint64) (Volume, error) {
	layerID := uuid.NewString()

	state := LayerState{}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if _, err := vm.layers.At(layerID).PutIfNotExists(ctx, "state.json", data); err != nil {
		return nil, fmt.Errorf("format layer: %w", err)
	}

	if err := vm.putVolumeRef(ctx, name, layerID, size); err != nil {
		return nil, fmt.Errorf("create volume ref: %w", err)
	}

	return vm.OpenVolume(ctx, name)
}

// OpenVolume loads a volume by name from S3.
func (vm *legacyVolumeManager) OpenVolume(ctx context.Context, name string) (Volume, error) {
	if v, ok := vm.volumes.Load(name); ok {
		if err := v.publish(); err == nil {
			return v, nil
		}
		vm.volumes.Delete(name)
	}

	ref, err := vm.getVolumeRef(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolve volume %q: %w", name, err)
	}

	layer, err := vm.loadLayer(ctx, ref.LayerID)
	if err != nil {
		return nil, fmt.Errorf("load layer %q: %w", ref.LayerID, err)
	}

	var size uint64
	if ref.Size != "" {
		size, err = strconv.ParseUint(ref.Size, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse volume size %q: %w", ref.Size, err)
		}
	}

	v := newVolume(name, size, layer, vm)

	if existing, loaded := vm.volumes.LoadOrStore(name, v); loaded {
		if err := layer.Close(ctx); err != nil {
			slog.Warn("close duplicate layer", "layer", ref.LayerID, "error", err)
		}
		if err := existing.publish(); err != nil {
			vm.volumes.Delete(name)
			return vm.OpenVolume(ctx, name)
		}
		return existing, nil
	}
	metrics.OpenVolumes.Inc()

	return v, nil
}

// GetVolume returns an already-opened volume, or nil.
func (vm *legacyVolumeManager) GetVolume(name string) Volume {
	v, ok := vm.volumes.Load(name)
	if !ok {
		return nil
	}
	return v
}

// Volumes returns all published volume names.
func (vm *legacyVolumeManager) Volumes() []string {
	var names []string
	vm.volumes.Range(func(name string, v *legacyVolume) bool {
		if v.publish() != nil {
			return true
		}
		names = append(names, name)
		return true
	})
	return names
}

// ListAllVolumes returns all volume names from the store (not just open ones).
func (vm *legacyVolumeManager) ListAllVolumes(ctx context.Context) ([]string, error) {
	objects, err := vm.volumeRefs.ListKeys(ctx, "")
	if err != nil {
		return nil, err
	}
	names := make([]string, len(objects))
	for i, obj := range objects {
		names[i] = obj.Key
	}
	return names, nil
}

// DeleteVolume removes a volume reference from the store.
// The volume must not be currently open/mounted.
// If the layer is mutable (not frozen, no children), its blocks are
// deleted in the background.
func (vm *legacyVolumeManager) DeleteVolume(ctx context.Context, name string) error {
	if v, ok := vm.volumes.Load(name); ok {
		if v.publish() == nil {
			return fmt.Errorf("volume %q is currently open; unmount it first", name)
		}
		vm.volumes.Delete(name) // XXX: what if this Volume is referenced from somewhere (has positive refcount)?
		// XXX: we need to handle this. Volume are just turned into normal files so we need to use the same approach
		// a real filesystem would use.
		// When instructed to delete a volume if the volume has refs, we need to not actually delete it but instead
		// add it to some VM-wide orphan volumes list that gets cleaned up on startup.
	}

	// Read the layer ID before deleting the ref.
	ref, err := vm.getVolumeRef(ctx, name)
	if err != nil {
		return fmt.Errorf("read volume ref %q: %w", name, err)
	}
	layerID := ref.LayerID

	// Delete the volume ref (this is the authoritative delete).
	if err := vm.volumeRefs.DeleteObject(ctx, name); err != nil {
		return fmt.Errorf("delete volume ref %q: %w", name, err)
	}

	// If the layer is mutable and has no children, garbage-collect it
	// in the background. Frozen layers may be referenced by clones.
	go vm.gcLayer(layerID)

	return nil
}

// gcLayer deletes a layer and all its blocks if it is mutable and has
// no children. Errors are logged but not returned.
func (vm *legacyVolumeManager) gcLayer(layerID string) { // XXX: gc is a weird thing to call this, rename - we are going to add real GC later
	// XXX: we should probably move this onto Layer
	ctx := context.Background()
	layerStore := vm.layers.At(layerID)

	state, _, err := ReadJSON[LayerState](ctx, layerStore, "state.json")
	if err != nil {
		slog.Warn("gc layer: read state", "layer", layerID, "error", err)
		return
	}
	if state.FrozenAt != "" {
		slog.Debug("gc layer: skipping frozen layer", "layer", layerID)
		return
	}

	objects, err := layerStore.ListKeys(ctx, "")
	if err != nil {
		slog.Warn("gc layer: list keys", "layer", layerID, "error", err)
		return
	}

	for _, obj := range objects {
		if err := layerStore.DeleteObject(ctx, obj.Key); err != nil {
			slog.Warn("gc layer: delete object", "layer", layerID, "key", obj.Key, "error", err)
		}
	}
	slog.Info("gc layer: deleted", "layer", layerID, "objects", len(objects))
}

// PageSize returns the system-wide block (page) size in bytes.
func (vm *legacyVolumeManager) PageSize() int {
	return int(vm.blockSize)
}

// Close releases any volumes still tracked by this manager, then
// releases the process lease.
func (vm *legacyVolumeManager) Close(ctx context.Context) error {
	var firstErr error
	vm.volumes.Range(func(name string, v *legacyVolume) bool {
		if err := v.ReleaseRef(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	if err := vm.lease.Close(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (vm *legacyVolumeManager) closeVolume(ctx context.Context, v *legacyVolume) error {
	if cur, ok := vm.volumes.Load(v.name); ok && cur == v { // XXX: when would this not be the case?
		vm.volumes.Delete(v.name)
	}
	if cur, ok := vm.openLayers.Load(v.layer.id); ok && cur == v.layer {
		vm.openLayers.Delete(v.layer.id)
	}
	metrics.OpenVolumes.Dec()
	return v.layer.Close(ctx)
}

// --- internal ---

func (vm *legacyVolumeManager) loadLayer(ctx context.Context, layerID string) (*Layer, error) {
	if l, ok := vm.openLayers.Load(layerID); ok { // XXX: why do we need to keep track of open layers?
		return l, nil
	}

	layer, err := NewLayer(ctx, vm, layerID)
	if err != nil {
		return nil, err
	}

	if err := layer.Mount(ctx); err != nil {
		return nil, fmt.Errorf("mount layer %q: %w", layerID, err)
	}

	if existing, loaded := vm.openLayers.LoadOrStore(layerID, layer); loaded {
		if err := layer.Unmount(ctx); err != nil {
			slog.Warn("unmount duplicate layer", "layer", layerID, "error", err)
		}
		return existing, nil
	}

	return layer, nil
}

func (vm *legacyVolumeManager) getVolumeRef(ctx context.Context, name string) (volumeRef, error) {
	ref, _, err := ReadJSON[volumeRef](ctx, vm.volumeRefs, name)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func (vm *legacyVolumeManager) putVolumeRef(ctx context.Context, name, layerID string, size uint64) error {
	sizeStr := ""
	if size > 0 {
		sizeStr = fmt.Sprintf("%d", size)
	}
	data, err := json.Marshal(volumeRef{LayerID: layerID, Size: sizeStr})
	if err != nil {
		return fmt.Errorf("marshal volume ref: %w", err)
	}
	created, err := vm.volumeRefs.PutIfNotExists(ctx, name, data)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("volume %q: %w", name, ErrVolumeExists)
	}
	return nil
}

func (vm *legacyVolumeManager) updateVolumeRef(ctx context.Context, name, layerID string) error {
	return ModifyJSON(ctx, vm.volumeRefs, name, func(ref *volumeRef) error {
		ref.LayerID = layerID
		return nil
	})
}
