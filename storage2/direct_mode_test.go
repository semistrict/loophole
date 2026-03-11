package storage2

import (
	"bytes"
	"testing"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeDirectWritebackRejectsNormalWrite(t *testing.T) {
	m := newTestManager(t, loophole.NewMemStore(), Config{
		FlushThreshold:  4 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	})
	vol, err := m.NewVolume(loophole.CreateParams{
		Volume: "direct-mode-rejects-normal-write",
		Size:   16 * PageSize,
	})
	require.NoError(t, err)

	require.NoError(t, vol.EnableDirectWriteback())

	err = vol.Write(bytes.Repeat([]byte{0xAB}, PageSize), 0)
	require.Error(t, err)
	assert.ErrorContains(t, err, "direct writeback mode")

	require.NoError(t, vol.DisableDirectWriteback())
	require.NoError(t, vol.Write(bytes.Repeat([]byte{0xCD}, PageSize), 0))
}

func TestVolumeDirectWritebackFlushesExistingMemtableBeforeEntering(t *testing.T) {
	ctx := t.Context()
	m := newTestManager(t, loophole.NewMemStore(), Config{
		FlushThreshold:  4 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
	})
	vol, err := m.NewVolume(loophole.CreateParams{
		Volume: "direct-mode-flushes-existing-memtable",
		Size:   16 * PageSize,
	})
	require.NoError(t, err)

	page0 := bytes.Repeat([]byte{0x11}, PageSize)
	require.NoError(t, vol.Write(page0, 0))

	require.NoError(t, vol.EnableDirectWriteback())
	defer func() {
		require.NoError(t, vol.DisableDirectWriteback())
	}()

	page1 := bytes.Repeat([]byte{0x22}, PageSize)
	require.NoError(t, vol.WritePagesDirect([]loophole.DirectPage{{
		Offset: PageSize,
		Data:   page1,
	}}))

	buf := make([]byte, 2*PageSize)
	_, err = vol.Read(ctx, buf, 0)
	require.NoError(t, err)
	assert.Equal(t, page0, buf[:PageSize])
	assert.Equal(t, page1, buf[PageSize:2*PageSize])

	storageVol, ok := vol.(*volume)
	require.True(t, ok)
	storageVol.mu.RLock()
	l0Count := len(storageVol.layer.index.L0)
	storageVol.mu.RUnlock()
	assert.Equal(t, 2, l0Count, "expected one flushed memtable L0 and one direct L0")
}
