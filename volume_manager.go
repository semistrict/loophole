package loophole

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

type volumeRef struct {
	LayerID string `json:"layer_id"`
}

// VolumeManager creates and tracks open volumes.
type VolumeManager struct {
	id    string      // unique instance ID: "hostname:pid:uuid"
	base  ObjectStore // root of the bucket/prefix
	lease *LeaseManager

	layers     ObjectStore // base.At("layers")
	volumeRefs ObjectStore // base.At("volumes")
	cache      *BlockCache
	blockSize  uint64

	uploadSem *semaphore.Weighted
	flushPool sync.Pool // pool of []byte buffers (blockSize each) for flush snapshots

	openLayers SyncMap[string, *Layer]
	volumes    SyncMap[string, *Volume]
}

// FormatSystem writes the root state.json with the system-wide block size.
// Must be called once before NewVolumeManager on a fresh store.
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

// NewVolumeManager reads the root state.json and initializes the system.
func NewVolumeManager(ctx context.Context, base ObjectStore, cacheDir string, maxUploads, maxDownloads int) (*VolumeManager, error) {
	state, _, err := ReadJSON[SystemState](ctx, base, "state.json")
	if err != nil {
		return nil, fmt.Errorf("read system state: %w (has FormatSystem been called?)", err)
	}

	layers := base.At("layers")
	cache, err := NewBlockCache(cacheDir, layers, state.BlockSize, maxDownloads)
	if err != nil {
		return nil, fmt.Errorf("init block cache: %w", err)
	}

	hostname, _ := os.Hostname()
	lm := NewLeaseManager(base.At("leases"))
	id := fmt.Sprintf("%s:%d:%s", hostname, os.Getpid(), lm.Token())

	blockSize := state.BlockSize
	return &VolumeManager{
		id:         id,
		base:       base,
		lease:      lm,
		layers:     layers,
		volumeRefs: base.At("volumes"),
		cache:      cache,
		blockSize:  blockSize,
		uploadSem:  semaphore.NewWeighted(int64(maxUploads)),
		flushPool: sync.Pool{
			New: func() any {
				b := make([]byte, blockSize)
				return &b
			},
		},
	}, nil
}

// NewVolume creates a new volume with an empty layer in S3.
func (vm *VolumeManager) NewVolume(ctx context.Context, name string) (*Volume, error) {
	layerID := uuid.NewString()

	state := LayerState{}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if _, err := vm.layers.At(layerID).PutIfNotExists(ctx, "state.json", data); err != nil {
		return nil, fmt.Errorf("format layer: %w", err)
	}

	if err := vm.putVolumeRef(ctx, name, layerID); err != nil {
		return nil, fmt.Errorf("create volume ref: %w", err)
	}

	return vm.OpenVolume(ctx, name)
}

// OpenVolume loads a volume by name from S3.
func (vm *VolumeManager) OpenVolume(ctx context.Context, name string) (*Volume, error) {
	if v, ok := vm.volumes.Load(name); ok {
		if err := v.publish(); err == nil {
			return v, nil
		}
		vm.volumes.Delete(name)
	}

	layerID, err := vm.getVolumeRef(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolve volume %q: %w", name, err)
	}

	layer, err := vm.loadLayer(ctx, layerID)
	if err != nil {
		return nil, fmt.Errorf("load layer %q: %w", layerID, err)
	}

	v := &Volume{
		name:  name,
		layer: layer,
		vm:    vm,
	}
	v.refs.Store(1)
	v.readOnly.Store(layer.Frozen())

	if existing, loaded := vm.volumes.LoadOrStore(name, v); loaded {
		if err := layer.Close(ctx); err != nil {
			slog.Warn("close duplicate layer", "layer", layerID, "error", err)
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
func (vm *VolumeManager) GetVolume(name string) *Volume {
	v, ok := vm.volumes.Load(name)
	if !ok {
		return nil
	}
	return v
}

// Volumes returns all published volume names.
func (vm *VolumeManager) Volumes() []string {
	var names []string
	vm.volumes.Range(func(name string, v *Volume) bool {
		if v.publish() != nil {
			return true
		}
		names = append(names, name)
		return true
	})
	return names
}

// ListAllVolumes returns all volume names from the store (not just open ones).
func (vm *VolumeManager) ListAllVolumes(ctx context.Context) ([]string, error) {
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

// Close shuts down all open volumes and releases the process lease.
func (vm *VolumeManager) Close(ctx context.Context) error {
	var firstErr error
	vm.volumes.Range(func(name string, v *Volume) bool {
		if err := v.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	if err := vm.lease.Close(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (vm *VolumeManager) closeVolume(ctx context.Context, v *Volume) error {
	if cur, ok := vm.volumes.Load(v.name); ok && cur == v {
		vm.volumes.Delete(v.name)
	}
	if cur, ok := vm.openLayers.Load(v.layer.id); ok && cur == v.layer {
		vm.openLayers.Delete(v.layer.id)
	}
	metrics.OpenVolumes.Dec()
	return v.layer.Close(ctx)
}

// --- internal ---

func (vm *VolumeManager) loadLayer(ctx context.Context, layerID string) (*Layer, error) {
	if l, ok := vm.openLayers.Load(layerID); ok {
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

func (vm *VolumeManager) getVolumeRef(ctx context.Context, name string) (string, error) {
	ref, _, err := ReadJSON[volumeRef](ctx, vm.volumeRefs, name)
	if err != nil {
		return "", err
	}
	return ref.LayerID, nil
}

func (vm *VolumeManager) putVolumeRef(ctx context.Context, name, layerID string) error {
	data, err := json.Marshal(volumeRef{LayerID: layerID})
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

func (vm *VolumeManager) updateVolumeRef(ctx context.Context, name, layerID string) error {
	return ModifyJSON(ctx, vm.volumeRefs, name, func(ref *volumeRef) error {
		ref.LayerID = layerID
		return nil
	})
}
