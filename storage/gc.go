package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/semistrict/loophole/objstore"
)

// GCResult contains the results of a garbage collection run.
type GCResult struct {
	ReachableLayers int
	OrphanedLayers  int
	DeletedObjects  int
	DeletedBytes    int64
}

// GarbageCollect finds and deletes orphaned layers that are not referenced by
// any volume or checkpoint. A layer is reachable if it is directly referenced
// by a volume or checkpoint ref, or transitively referenced via blockRange
// entries in a reachable layer's index.
func GarbageCollect(ctx context.Context, store objstore.ObjectStore, dryRun bool) (GCResult, error) {
	volRefs := store.At("volumes")

	// 1. Collect all directly referenced layer IDs from volumes and checkpoints.
	reachable := map[string]bool{}

	volumes, err := ListAllVolumes(ctx, store)
	if err != nil {
		return GCResult{}, fmt.Errorf("list volumes: %w", err)
	}

	for _, name := range volumes {
		// Read volume ref.
		ref, _, err := objstore.ReadJSON[volumeRef](ctx, volRefs, name+"/index.json")
		if err != nil {
			slog.Warn("gc: skip volume ref", "volume", name, "error", err)
			continue
		}
		if ref.LayerID != "" {
			reachable[ref.LayerID] = true
		}

		// Read checkpoint refs.
		cpPrefix := name + "/checkpoints/"
		cpObjects, err := volRefs.ListKeys(ctx, cpPrefix)
		if err != nil {
			slog.Warn("gc: list checkpoints", "volume", name, "error", err)
			continue
		}
		for _, obj := range cpObjects {
			if !strings.HasSuffix(obj.Key, "/index.json") {
				continue
			}
			cpRef, _, err := objstore.ReadJSON[checkpointRef](ctx, volRefs, obj.Key)
			if err != nil {
				slog.Warn("gc: skip checkpoint ref", "key", obj.Key, "error", err)
				continue
			}
			if cpRef.LayerID != "" {
				reachable[cpRef.LayerID] = true
			}
		}
	}

	// 2. BFS: transitively follow layer index blockRanges.
	queue := make([]string, 0, len(reachable))
	for id := range reachable {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		idx, _, err := objstore.ReadJSON[layerIndex](ctx, store.At("layers/"+id), "index.json")
		if err != nil {
			slog.Warn("gc: read layer index", "layer", id, "error", err)
			continue
		}
		for _, br := range idx.L1 {
			if br.Layer != "" && !reachable[br.Layer] {
				reachable[br.Layer] = true
				queue = append(queue, br.Layer)
			}
		}
		for _, br := range idx.L2 {
			if br.Layer != "" && !reachable[br.Layer] {
				reachable[br.Layer] = true
				queue = append(queue, br.Layer)
			}
		}
	}

	// 3. List all layers by scanning keys under layers/ prefix.
	allLayers := map[string]bool{}
	layerObjects, err := store.At("layers").ListKeys(ctx, "")
	if err != nil {
		return GCResult{}, fmt.Errorf("list layers: %w", err)
	}
	for _, obj := range layerObjects {
		id, _, ok := strings.Cut(obj.Key, "/")
		if ok && id != "" {
			allLayers[id] = true
		}
	}

	// 4. Find and delete orphans.
	var result GCResult
	result.ReachableLayers = len(reachable)

	for id := range allLayers {
		if reachable[id] {
			continue
		}
		result.OrphanedLayers++

		if dryRun {
			slog.Info("gc: orphaned layer (dry-run)", "layer", id)
			continue
		}

		// List and delete all objects under layers/{id}/.
		objects, err := store.At("layers/"+id).ListKeys(ctx, "")
		if err != nil {
			slog.Warn("gc: list layer objects", "layer", id, "error", err)
			continue
		}
		for _, obj := range objects {
			if err := store.At("layers/"+id).DeleteObject(ctx, obj.Key); err != nil {
				slog.Warn("gc: delete object", "layer", id, "key", obj.Key, "error", err)
				continue
			}
			result.DeletedObjects++
			result.DeletedBytes += obj.Size
		}
		slog.Info("gc: deleted orphaned layer", "layer", id)
	}

	return result, nil
}
