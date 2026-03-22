package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/semistrict/loophole/internal/blob"
)

const volumeSetDescriptorKey = "loophole.json"

type volumeSetDescriptor struct {
	PageSize int    `json:"page_size"`
	VolsetID string `json:"volset_id"`
}

func FormatVolumeSet(ctx context.Context, store *blob.Store) (volumeSetDescriptor, bool, error) {
	desc := volumeSetDescriptor{
		PageSize: PageSize,
		VolsetID: uuid.NewString(),
	}
	data, err := json.Marshal(desc)
	if err != nil {
		return volumeSetDescriptor{}, false, fmt.Errorf("marshal volume set descriptor: %w", err)
	}
	if err := store.PutIfNotExists(ctx, volumeSetDescriptorKey, data); err != nil {
		if !errors.Is(err, blob.ErrExists) {
			return volumeSetDescriptor{}, false, fmt.Errorf("create volume set descriptor: %w", err)
		}
		existing, err := CheckVolumeSet(ctx, store)
		if err != nil {
			return volumeSetDescriptor{}, false, err
		}
		return existing, false, nil
	}
	return desc, true, nil
}

func CheckVolumeSet(ctx context.Context, store *blob.Store) (volumeSetDescriptor, error) {
	desc, _, err := blob.ReadJSON[volumeSetDescriptor](ctx, store, volumeSetDescriptorKey)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return volumeSetDescriptor{}, fmt.Errorf("store is not formatted: missing %s (run `loophole format`)", volumeSetDescriptorKey)
		}
		return volumeSetDescriptor{}, fmt.Errorf("read volume set descriptor: %w", err)
	}
	if desc.PageSize != PageSize {
		return volumeSetDescriptor{}, fmt.Errorf("store page size %d does not match compiled page size %d", desc.PageSize, PageSize)
	}
	if _, err := uuid.Parse(desc.VolsetID); err != nil {
		return volumeSetDescriptor{}, fmt.Errorf("invalid volset_id %q: %w", desc.VolsetID, err)
	}
	return desc, nil
}
