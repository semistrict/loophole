package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/semistrict/loophole/internal/blob"
	"github.com/stretchr/testify/require"
)

func TestFormatVolumeSetCreatesDescriptor(t *testing.T) {
	mem := blob.NewMemDriver()
	store := blob.New(mem)

	desc, created, err := FormatVolumeSet(context.Background(), store)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, PageSize, desc.PageSize)
	_, err = uuid.Parse(desc.VolsetID)
	require.NoError(t, err)

	raw, ok := mem.GetObject(volumeSetDescriptorKey)
	require.True(t, ok)

	var stored volumeSetDescriptor
	require.NoError(t, json.Unmarshal(raw, &stored))
	require.Equal(t, desc, stored)
}

func TestCheckVolumeSetRequiresDescriptor(t *testing.T) {
	store := blob.New(blob.NewMemDriver())

	_, err := CheckVolumeSet(context.Background(), store)
	require.ErrorContains(t, err, "store is not formatted")
}

func TestCheckVolumeSetRejectsPageSizeMismatch(t *testing.T) {
	store := blob.New(blob.NewMemDriver())
	data, err := json.Marshal(volumeSetDescriptor{
		PageSize: 65536,
		VolsetID: uuid.NewString(),
	})
	require.NoError(t, err)
	require.NoError(t, store.PutIfNotExists(context.Background(), volumeSetDescriptorKey, data))

	_, err = CheckVolumeSet(context.Background(), store)
	require.ErrorContains(t, err, "does not match compiled page size")
}
