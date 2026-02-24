package loophole

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/semistrict/loophole/metrics"
)

// LayerState is the state.json persisted in S3 for each layer.
type LayerState struct {
	RefLayers     []string         `json:"ref_layers,omitempty"`      // referenced layers: [parent, grandparent, ..., root] plus any CopyFrom sources
	RefBlockIndex map[BlockIdx]int `json:"ref_block_index,omitempty"` // block index → index into RefLayers
	FrozenAt      string           `json:"frozen_at,omitempty"`
	LeaseToken    string           `json:"lease_token,omitempty"` // token of the process holding the write lease
	Children      []string         `json:"children,omitempty"`
}

// NewLayer fetches state.json, lists blocks for this layer and all
// ancestors in parallel, and builds the block ownership map.
func NewLayer(ctx context.Context, vm *VolumeManager, id string) (*Layer, error) {
	t := metrics.NewTimer(metrics.LayerLoadDuration)
	defer t.ObserveDuration()
	base := vm.layers.At(id)

	state, _, err := readLayerState(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("read state for %q: %w", id, err)
	}

	// List this layer's own blocks.
	ownBlocks, ownTombstones, err := listBlocks(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("list blocks for %q: %w", id, err)
	}

	// Build localIndex from this layer's non-tombstone blocks.
	localIndex := make(map[BlockIdx]struct{})
	for blk := range ownBlocks {
		if _, isTombstone := ownTombstones[blk]; !isTombstone {
			localIndex[blk] = struct{}{}
		}
	}

	// RefBlockIndex is precomputed by CreateChild. Resolve compact indexes
	// into full layer IDs for fast runtime lookups.
	refBlockIndex := make(map[BlockIdx]string, len(state.RefBlockIndex))
	for blk, idx := range state.RefBlockIndex {
		refBlockIndex[blk] = state.RefLayers[idx]
	}

	// Remove entries that this layer overrides with its own blocks or tombstones.
	for blk := range ownBlocks {
		delete(refBlockIndex, blk)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	l := &Layer{
		vm:            vm,
		base:          base,
		cache:         vm.cache,
		id:            id,
		refLayers:     state.RefLayers,
		refBlockIndex: refBlockIndex,
		dirty:         make(map[BlockIdx]struct{}),
		localIndex:    localIndex,
		openBlocks:    make(map[BlockIdx]*os.File),
		stopFlush:     cancel,
		flushStopped:  make(chan struct{}),
	}
	l.frozen.Store(state.FrozenAt != "")

	if !l.frozen.Load() {
		go l.backgroundFlush(ctx2)
	} else {
		close(l.flushStopped)
	}

	return l, nil
}

// Mount acquires the write lease on this layer using CAS.
// Succeeds if no lease exists or the existing lease has expired.
func (l *Layer) Mount(ctx context.Context) error {
	if l.frozen.Load() {
		return nil // frozen layers are read-only, no lease needed
	}
	lm := l.vm.lease
	if err := lm.EnsureStarted(ctx); err != nil {
		return err
	}
	return ModifyJSON(ctx, l.base, "state.json", func(state *LayerState) error {
		if err := lm.CheckAvailable(ctx, state.LeaseToken); err != nil {
			return fmt.Errorf("layer %q: %w", l.id, err)
		}
		state.LeaseToken = lm.Token()
		return nil
	})
}

// Unmount releases the write lease if this layer still holds it.
func (l *Layer) Unmount(ctx context.Context) error {
	if l.frozen.Load() {
		return nil
	}
	token := l.vm.lease.Token()
	return ModifyJSON(ctx, l.base, "state.json", func(state *LayerState) error {
		if state.LeaseToken == token {
			state.LeaseToken = ""
		}
		return nil
	})
}

// Freeze flushes all dirty blocks and marks this layer as frozen in S3.
func (l *Layer) Freeze(ctx context.Context) error {
	if err := l.checkWritable(); err != nil {
		return err
	}

	// Stop background flush before the final flush.
	l.stopFlush()
	<-l.flushStopped

	if err := l.Flush(ctx); err != nil {
		return err
	}

	if err := ModifyJSON(ctx, l.base, "state.json", func(state *LayerState) error {
		state.FrozenAt = time.Now().UTC().Format(time.RFC3339)
		return nil
	}); err != nil {
		return fmt.Errorf("write frozen state: %w", err)
	}
	l.frozen.Store(true)
	return nil
}

// CreateChild writes a new child state.json and adds the child ID to
// this layer's children list using read-modify-write with CAS.
func (l *Layer) CreateChild(ctx context.Context, childID string) error {
	// Build the child's ref layers list. Start with this layer, then
	// include all of this layer's ref layers. Additionally, include any
	// layers referenced by refBlockIndex that aren't already ancestors
	// (e.g. CopyFrom sources).
	seen := make(map[string]bool)
	refLayers := make([]string, 0, len(l.refLayers)+1)
	refLayers = append(refLayers, l.id)
	seen[l.id] = true
	for _, id := range l.refLayers {
		if !seen[id] {
			refLayers = append(refLayers, id)
			seen[id] = true
		}
	}
	// Add any CopyFrom source layers not already in the chain.
	for _, owner := range l.refBlockIndex {
		if !seen[owner] {
			refLayers = append(refLayers, owner)
			seen[owner] = true
		}
	}

	// Build reverse lookup: layer ID → index in child's RefLayers array.
	layerIdx := make(map[string]int, len(refLayers))
	for i, id := range refLayers {
		layerIdx[id] = i
	}

	// Precompute the child's ref block index: this layer's refBlockIndex
	// plus this layer's own local blocks, stored as compact array indexes.
	childRefBlockIndex := make(map[BlockIdx]int, len(l.refBlockIndex)+len(l.localIndex))
	for blk, owner := range l.refBlockIndex {
		childRefBlockIndex[blk] = layerIdx[owner]
	}
	l.mu.Lock()
	for blk := range l.localIndex {
		childRefBlockIndex[blk] = 0 // index 0 = this layer (the child's parent)
	}
	l.mu.Unlock()

	childState := LayerState{
		RefLayers:     refLayers,
		RefBlockIndex: childRefBlockIndex,
	}
	data, err := json.Marshal(childState)
	if err != nil {
		return err
	}
	if _, err := l.vm.layers.At(childID).PutIfNotExists(ctx, "state.json", data); err != nil {
		return fmt.Errorf("write child state: %w", err)
	}

	if err := ModifyJSON(ctx, l.base, "state.json", func(state *LayerState) error {
		state.Children = append(state.Children, childID)
		return nil
	}); err != nil {
		return fmt.Errorf("add child to parent: %w", err)
	}
	return nil
}

func (l *Layer) Close(ctx context.Context) error {
	if l.closed.Load() {
		return nil
	}
	l.closed.Store(true)
	l.stopFlush()
	<-l.flushStopped
	err := l.Flush(ctx)

	// Close all open mutable block file handles.
	l.mu.Lock()
	for _, f := range l.openBlocks {
		f.Close()
	}
	l.openBlocks = nil
	l.mu.Unlock()

	if unmountErr := l.Unmount(ctx); err == nil {
		err = unmountErr
	}
	return err
}

// readLayerState reads a state.json from an ObjectStore scoped to a layer.
func readLayerState(ctx context.Context, objects ObjectStore) (LayerState, string, error) {
	return ReadJSON[LayerState](ctx, objects, "state.json")
}

// listBlocks lists all block keys under a layer prefix, returning the set
// of block indices and the subset that are tombstones (size == 0).
func listBlocks(ctx context.Context, objects ObjectStore) (blocks, tombstones map[BlockIdx]struct{}, err error) {
	infos, err := objects.ListKeys(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	blocks = make(map[BlockIdx]struct{})
	tombstones = make(map[BlockIdx]struct{})
	for _, info := range infos {
		name := info.Key
		if strings.Contains(name, ".") {
			continue // skip state.json etc.
		}
		idx, err := ParseBlockIdx(name)
		if err != nil {
			continue
		}
		blocks[idx] = struct{}{}
		if info.Size == 0 {
			tombstones[idx] = struct{}{}
		}
	}
	return blocks, tombstones, nil
}
