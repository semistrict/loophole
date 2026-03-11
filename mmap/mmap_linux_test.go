//go:build linux

package mmap_test

import (
	"testing"

	"github.com/semistrict/loophole/mmap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapVolume_MissingBackendBasicRead(t *testing.T) {
	vol := newTestVolume(t, "missing-basic-read", 4)

	page0 := make([]byte, pageSize)
	for i := range page0 {
		page0[i] = 0xAA
	}
	require.NoError(t, vol.Write(page0, 0))
	require.NoError(t, vol.Flush())

	mr, err := mmap.MapVolume(vol, 0, 4*pageSize, mmap.WithBackend(mmap.BackendMissing))
	require.NoError(t, err)
	defer mr.Close()

	assert.Equal(t, mmap.BackendMissing, mr.Backend())
	assert.Equal(t, byte(0xAA), mr.Bytes()[0])
	assert.Equal(t, byte(0xAA), mr.Bytes()[pageSize-1])
	assert.NoError(t, mr.Flush())
}
