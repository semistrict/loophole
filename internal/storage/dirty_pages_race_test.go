package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirtyBatchCopyPageReturnsStableSnapshot(t *testing.T) {
	mt := testDirtyBatch(8)

	first := filledPage(0xAA)
	second := filledPage(0xBB)
	require.NoError(t, mt.stagePage(0, first))

	var snapshot Page
	ok, tombstone := mt.copyPage(0, &snapshot)
	require.True(t, ok)
	require.False(t, tombstone)
	require.Equal(t, bytes.Repeat([]byte{0xAA}, PageSize), snapshot[:])

	require.NoError(t, mt.stagePage(0, second))

	// The copied snapshot remains valid after the overwrite because readers copy
	// while holding the batch read lock rather than retaining dirtyRecord
	// pointers after unlock.
	require.Equal(t, bytes.Repeat([]byte{0xAA}, PageSize), snapshot[:])

	var fresh Page
	ok, tombstone = mt.copyPage(0, &fresh)
	require.True(t, ok)
	require.False(t, tombstone)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, PageSize), fresh[:])
}
