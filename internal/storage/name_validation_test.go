package storage

import (
	"encoding/json"
	"testing"

	"github.com/semistrict/loophole/internal/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerRejectsInvalidVolumeNames(t *testing.T) {
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	_, err := m.NewVolume(CreateParams{Volume: "bad..name"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain \"..\"")
}

func TestManagerRejectsInvalidCheckpointIDs(t *testing.T) {
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	err := Clone(t.Context(), m.Store(), "sandbox-1", "../evil", "clone-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint id")
}

func TestCloneRequiresCheckpointID(t *testing.T) {
	m := newTestManager(t, objstore.NewMemStore(), testConfig)

	err := Clone(t.Context(), m.Store(), "sandbox-1", "", "clone-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing checkpoint ID")
}

func TestValidateCheckpointID(t *testing.T) {
	assert.NoError(t, ValidateCheckpointID("20260313112233"))
	assert.Error(t, ValidateCheckpointID("../evil"))
	assert.Error(t, ValidateCheckpointID("20260313"))
	assert.Error(t, ValidateCheckpointID("2026031311223a"))
}

func TestValidateVolumeName(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		assert.NoError(t, ValidateVolumeName("sandbox-1"))
	})

	t.Run("rejects traversal", func(t *testing.T) {
		err := ValidateVolumeName("sandbox..1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must not contain \"..\"")
	})

	t.Run("rejects separators", func(t *testing.T) {
		assert.Error(t, ValidateVolumeName("sandbox/1"))
		assert.Error(t, ValidateVolumeName(`sandbox\1`))
	})
}

func TestVolumeInfoOmitsReadOnly(t *testing.T) {
	data, err := json.Marshal(VolumeInfo{Name: "sandbox-1", Size: 123, Type: VolumeTypeExt4})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "read_only")
}
