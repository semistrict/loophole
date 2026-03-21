package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole/objstore"
)

// GCResult contains the results of a garbage collection run.
type GCResult struct {
	ReachableLayers int
	OrphanedLayers  int
	DeletedObjects  int
	DeletedBytes    int64
}

// DefaultGCConcurrency is the default number of concurrent goroutines for GC.
const DefaultGCConcurrency = 128

// GarbageCollect finds and deletes orphaned layers that are not referenced by
// any volume or checkpoint. A layer is reachable if it is directly referenced
// by a volume or checkpoint ref, or transitively referenced via blockRange
// entries in a reachable layer's index.
//
// concurrency controls the maximum number of parallel goroutines for I/O.
// If <= 0, DefaultGCConcurrency is used.
func GarbageCollect(ctx context.Context, store objstore.ObjectStore, dryRun bool, concurrency int) (GCResult, error) {
	if concurrency <= 0 {
		concurrency = DefaultGCConcurrency
	}

	volRefs := store.At("volumes")

	// 1. Collect all directly referenced layer IDs from volumes and checkpoints.
	volumes, err := ListAllVolumes(ctx, store)
	if err != nil {
		return GCResult{}, fmt.Errorf("list volumes: %w", err)
	}

	var mu sync.Mutex
	reachable := map[string]bool{}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, name := range volumes {
		g.Go(func() error {
			ref, _, err := objstore.ReadJSON[volumeRef](gctx, volRefs, name+"/index.json")
			if err != nil {
				slog.Warn("gc: skip volume ref", "volume", name, "error", err)
				return nil
			}

			cpPrefix := name + "/checkpoints/"
			cpObjects, err := volRefs.ListKeys(gctx, cpPrefix)
			if err != nil {
				slog.Warn("gc: list checkpoints", "volume", name, "error", err)
				// Still record the volume's own layer.
				if ref.LayerID != "" {
					mu.Lock()
					reachable[ref.LayerID] = true
					mu.Unlock()
				}
				return nil
			}

			// Collect this volume's layer + all checkpoint layers.
			localLayers := make([]string, 0, 1+len(cpObjects))
			if ref.LayerID != "" {
				localLayers = append(localLayers, ref.LayerID)
			}
			for _, obj := range cpObjects {
				if !strings.HasSuffix(obj.Key, "/index.json") {
					continue
				}
				cpRef, _, err := objstore.ReadJSON[checkpointRef](gctx, volRefs, obj.Key)
				if err != nil {
					slog.Warn("gc: skip checkpoint ref", "key", obj.Key, "error", err)
					continue
				}
				if cpRef.LayerID != "" {
					localLayers = append(localLayers, cpRef.LayerID)
				}
			}

			mu.Lock()
			for _, id := range localLayers {
				reachable[id] = true
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return GCResult{}, err
	}

	// 2. BFS: transitively follow layer index blockRanges.
	// Each BFS level is read in parallel; discovered layers feed the next level.
	queue := make([]string, 0, len(reachable))
	for id := range reachable {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		type indexResult struct {
			children []string
		}
		results := make([]indexResult, len(queue))

		g, gctx = errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		for i, id := range queue {
			g.Go(func() error {
				idx, _, err := objstore.ReadJSON[layerIndex](gctx, store.At("layers/"+id), "index.json")
				if err != nil {
					slog.Warn("gc: read layer index", "layer", id, "error", err)
					return nil
				}
				var children []string
				for _, br := range idx.L1 {
					if br.Layer != "" {
						children = append(children, br.Layer)
					}
				}
				for _, br := range idx.L2 {
					if br.Layer != "" {
						children = append(children, br.Layer)
					}
				}
				results[i] = indexResult{children: children}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return GCResult{}, err
		}

		queue = queue[:0]
		for _, r := range results {
			for _, child := range r.children {
				if !reachable[child] {
					reachable[child] = true
					queue = append(queue, child)
				}
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

	// 4. Find orphans.
	var orphanIDs []string
	for id := range allLayers {
		if !reachable[id] {
			orphanIDs = append(orphanIDs, id)
		}
	}

	var result GCResult
	result.ReachableLayers = len(reachable)
	result.OrphanedLayers = len(orphanIDs)

	if dryRun || len(orphanIDs) == 0 {
		for _, id := range orphanIDs {
			slog.Info("gc: orphaned layer (dry-run)", "layer", id)
		}
		return result, nil
	}

	// 5. Delete orphans in parallel.
	var deletedObjects atomic.Int64
	var deletedBytes atomic.Int64

	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, id := range orphanIDs {
		g.Go(func() error {
			objects, err := store.At("layers/"+id).ListKeys(gctx, "")
			if err != nil {
				slog.Warn("gc: list layer objects", "layer", id, "error", err)
				return nil
			}
			keys := make([]string, len(objects))
			for i, obj := range objects {
				keys[i] = obj.Key
			}
			if err := store.At("layers/"+id).DeleteObjects(gctx, keys); err != nil {
				slog.Warn("gc: delete objects", "layer", id, "error", err)
				return nil
			}
			for _, obj := range objects {
				deletedObjects.Add(1)
				deletedBytes.Add(obj.Size)
			}
			slog.Info("gc: deleted orphaned layer", "layer", id)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return GCResult{}, err
	}

	result.DeletedObjects = int(deletedObjects.Load())
	result.DeletedBytes = deletedBytes.Load()
	return result, nil
}
