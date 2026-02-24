package loophole

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func newTestVM(t *testing.T, store *MemStore) *VolumeManager {
	t.Helper()
	_ = FormatSystem(t.Context(), store, 64)
	vm, err := NewVolumeManager(t.Context(), store, t.TempDir(), 20, 200)
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	return vm
}

func seedLayer(t interface {
	Helper()
	Fatalf(string, ...any)
}, store *MemStore, layerID string, state LayerState, blocks map[BlockIdx][]byte) {
	t.Helper()

	if state.RefBlockIndex == nil && len(state.RefLayers) > 0 {
		state.RefBlockIndex = buildTestRefBlockIndex(store, state.RefLayers)
	}

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	store.shared.mu.Lock()
	defer store.shared.mu.Unlock()
	store.shared.objects[fmt.Sprintf("layers/%s/state.json", layerID)] = data
	for idx, bdata := range blocks {
		key := "layers/" + layerID + "/" + idx.String()
		cp := make([]byte, len(bdata))
		copy(cp, bdata)
		store.shared.objects[key] = cp
	}
}

func buildTestRefBlockIndex(store *MemStore, refLayers []string) map[BlockIdx]int {
	resolved := make(map[BlockIdx]string)
	for i := len(refLayers) - 1; i >= 0; i-- {
		layerID := refLayers[i]
		prefix := fmt.Sprintf("layers/%s/", layerID)
		store.shared.mu.Lock()
		for key, data := range store.shared.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			name := strings.TrimPrefix(key, prefix)
			if strings.Contains(name, ".") {
				continue
			}
			idx, err := ParseBlockIdx(name)
			if err != nil {
				continue
			}
			if len(data) == 0 {
				delete(resolved, idx)
			} else {
				resolved[idx] = layerID
			}
		}
		store.shared.mu.Unlock()
	}

	layerIdx := make(map[string]int, len(refLayers))
	for i, id := range refLayers {
		layerIdx[id] = i
	}
	index := make(map[BlockIdx]int, len(resolved))
	for blk, owner := range resolved {
		index[blk] = layerIdx[owner]
	}
	return index
}

func defaultLayerState() LayerState {
	return LayerState{}
}

func frozenLayerState() LayerState {
	return LayerState{
		FrozenAt: "2025-01-01T00:00:00Z",
	}
}
