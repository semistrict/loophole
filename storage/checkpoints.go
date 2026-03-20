package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole/objstore"
)

// checkpointRef is the S3-persisted metadata for a volume checkpoint.
type checkpointRef struct {
	LayerID   string `json:"layer_id"`
	CreatedAt string `json:"created_at"`
}

func checkpointPrefix(volumeName string) (string, error) {
	if err := ValidateVolumeName(volumeName); err != nil {
		return "", err
	}
	return volumeName + "/checkpoints/", nil
}

func checkpointIndexKey(volumeName, checkpointID string) (string, error) {
	prefix, err := checkpointPrefix(volumeName)
	if err != nil {
		return "", err
	}
	if err := ValidateCheckpointID(checkpointID); err != nil {
		return "", err
	}
	return prefix + checkpointID + "/index.json", nil
}

func putCheckpoint(ctx context.Context, volRefs objstore.ObjectStore, volumeName string, layerID string) (string, error) {
	if err := ValidateVolumeName(volumeName); err != nil {
		return "", err
	}
	now := time.Now().UTC()
	ts := now.Format("20060102150405")

	key, err := checkpointIndexKey(volumeName, ts)
	if err != nil {
		return "", err
	}
	for attempt := range 60 {
		ref := checkpointRef{
			LayerID:   layerID,
			CreatedAt: now.Format(time.RFC3339),
		}
		data, err := json.Marshal(ref)
		if err != nil {
			return "", err
		}
		err = volRefs.PutIfNotExists(ctx, key, data)
		if err == nil {
			return ts, nil
		}
		if !errors.Is(err, objstore.ErrExists) {
			return "", fmt.Errorf("write checkpoint ref: %w", err)
		}
		now = now.Add(time.Second)
		ts = now.Format("20060102150405")
		key, err = checkpointIndexKey(volumeName, ts)
		if err != nil {
			return "", err
		}
		_ = attempt
	}
	return "", fmt.Errorf("checkpoint timestamp collision after 60 attempts")
}

// ListCheckpoints returns all checkpoints for a volume, sorted by ID (oldest first).
func ListCheckpoints(ctx context.Context, store objstore.ObjectStore, volumeName string) ([]CheckpointInfo, error) {
	volRefs := store.At("volumes")
	prefix, err := checkpointPrefix(volumeName)
	if err != nil {
		return nil, err
	}
	objects, err := volRefs.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var checkpoints []CheckpointInfo
	for _, obj := range objects {
		// Key format: "{vol}/checkpoints/{ts}/index.json"
		rest := strings.TrimPrefix(obj.Key, prefix)
		ts, _, ok := strings.Cut(rest, "/")
		if !ok || ts == "" {
			continue
		}
		t, err := time.Parse("20060102150405", ts)
		if err != nil {
			continue
		}
		checkpoints = append(checkpoints, CheckpointInfo{
			ID:        ts,
			CreatedAt: t,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].ID < checkpoints[j].ID
	})
	return checkpoints, nil
}

// CloneFromCheckpoint creates a new volume by cloning from a checkpoint's frozen layer.
// This is a metadata-only operation: it reads the checkpoint's layer index and writes
// a new layer index + volume ref that reference the same block ranges.
func CloneFromCheckpoint(ctx context.Context, store objstore.ObjectStore, volumeName, checkpointID, cloneName string) error {
	if err := ValidateVolumeName(cloneName); err != nil {
		return err
	}

	volRefs := store.At("volumes")
	cpKey, err := checkpointIndexKey(volumeName, checkpointID)
	if err != nil {
		return err
	}
	cpRef, _, err := objstore.ReadJSON[checkpointRef](ctx, volRefs, cpKey)
	if err != nil {
		return fmt.Errorf("read checkpoint %s/%s: %w", volumeName, checkpointID, err)
	}

	volRef, err := getVolumeRef(ctx, volRefs, volumeName)
	if err != nil {
		return fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}

	idx, _, err := objstore.ReadJSON[layerIndex](ctx, store.At("layers/"+cpRef.LayerID), "index.json")
	if err != nil {
		return fmt.Errorf("read checkpoint layer index %q: %w", cpRef.LayerID, err)
	}

	childID := newLayerID()
	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index for clone: %w", err)
	}

	ref := volumeRef{
		LayerID: childID,
		Size:    volRef.Size,
		Type:    volRef.Type,
	}
	refData, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("marshal volume ref: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := store.At("layers/"+childID).PutIfNotExists(gctx, "index.json", idxData, map[string]string{
			"created_at": time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			return fmt.Errorf("create clone index: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := volRefs.PutIfNotExists(gctx, cloneName+"/index.json", refData); err != nil {
			return fmt.Errorf("create clone ref: %w", err)
		}
		return nil
	})
	return g.Wait()
}
