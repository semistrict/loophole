package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/semistrict/loophole/internal/blob"
)

// volumeRef is the S3-persisted mapping from volume name to layer ID.
type volumeRef struct {
	LayerID       string            `json:"layer_id"`
	Size          uint64            `json:"size,omitempty"`
	Type          string            `json:"type,omitempty"`
	LeaseToken    string            `json:"lease_token,omitempty"`
	WriteLeaseSeq uint64            `json:"write_lease_seq,omitempty"`
	Parent        string            `json:"parent,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

func volumeIndexKey(name string) (string, error) {
	if err := ValidateVolumeName(name); err != nil {
		return "", err
	}
	return name + "/index.json", nil
}

func getVolumeRef(ctx context.Context, volRefs *blob.Store, name string) (volumeRef, error) {
	key, err := volumeIndexKey(name)
	if err != nil {
		return volumeRef{}, err
	}
	ref, _, err := blob.ReadJSON[volumeRef](ctx, volRefs, key)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func putVolumeRefNew(ctx context.Context, volRefs *blob.Store, name string, ref volumeRef) error {
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := volRefs.PutIfNotExists(ctx, key, data); err != nil {
		if errors.Is(err, blob.ErrExists) {
			return fmt.Errorf("volume %q already exists", name)
		}
		return fmt.Errorf("create volume ref: %w", err)
	}
	return nil
}

func relayerVolumeRef(ctx context.Context, volRefs *blob.Store, name string, newLayerID string) (uint64, error) {
	key, err := volumeIndexKey(name)
	if err != nil {
		return 0, err
	}
	var seq uint64
	err = blob.ModifyJSON[volumeRef](ctx, volRefs, key, func(ref *volumeRef) error {
		ref.LayerID = newLayerID
		ref.WriteLeaseSeq++
		seq = ref.WriteLeaseSeq
		return nil
	})
	return seq, err
}
