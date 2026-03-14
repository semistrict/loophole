package storage2

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerRejectsInvalidVolumeNames(t *testing.T) {
	m := newTestManager(t, loophole.NewMemStore(), testConfig)

	_, err := m.NewVolume(loophole.CreateParams{Volume: "bad..name"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain \"..\"")
}

func TestManagerRejectsInvalidCheckpointIDs(t *testing.T) {
	m := newTestManager(t, loophole.NewMemStore(), testConfig)

	err := m.CloneFromCheckpoint(t.Context(), "sandbox-1", "../evil", "clone-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint id")
}
