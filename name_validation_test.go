package loophole

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestValidateCheckpointID(t *testing.T) {
	assert.NoError(t, ValidateCheckpointID("20260313112233"))
	assert.Error(t, ValidateCheckpointID("../evil"))
	assert.Error(t, ValidateCheckpointID("20260313"))
	assert.Error(t, ValidateCheckpointID("2026031311223a"))
}
