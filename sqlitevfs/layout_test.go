package sqlitevfs

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/storage2"
	"github.com/stretchr/testify/require"
)

func testManager(t *testing.T) loophole.VolumeManager {
	t.Helper()
	store := loophole.NewMemStore()
	m := storage2.NewVolumeManager(store, t.TempDir(), storage2.Config{
		FlushThreshold:  16 * storage2.PageSize,
		MaxFrozenTables: 2,
	}, nil, nil)
	t.Cleanup(func() { m.Close(t.Context()) })
	return m
}

func testVolume(t *testing.T, size uint64) loophole.Volume {
	t.Helper()
	m := testManager(t)
	vol, err := m.NewVolume(t.Context(), t.Name(), size, "")
	require.NoError(t, err)
	t.Cleanup(func() { vol.ReleaseRef(t.Context()) })
	return vol
}

func TestHeaderRoundTrip(t *testing.T) {
	vol := testVolume(t, 8*1024*1024) // 8MB

	err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	h, err := ReadHeader(t.Context(), vol)
	require.NoError(t, err)
	require.Equal(t, uint64(0), h.MainDBSize)

	// Update and re-read.
	h.MainDBSize = 4096
	require.NoError(t, writeHeader(t.Context(), vol, h))

	h2, err := ReadHeader(t.Context(), vol)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), h2.MainDBSize)
}

func TestFormatVolumeTooSmall(t *testing.T) {
	vol := testVolume(t, 2*1024*1024) // 2MB — below 4MB minimum.
	err := FormatVolume(t.Context(), vol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "volume too small")
}

func TestReadHeaderUnformatted(t *testing.T) {
	vol := testVolume(t, 8*1024*1024)
	// Volume is all zeros — should fail with bad magic.
	_, err := ReadHeader(t.Context(), vol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid header magic")
}

func TestMemBuffer(t *testing.T) {
	var buf memBuffer

	// Not exists initially.
	require.False(t, buf.isExists())
	require.Equal(t, int64(0), buf.fileSize())

	// Create and write.
	buf.setExists()
	require.True(t, buf.isExists())

	n := buf.writeAt([]byte("hello"), 0)
	require.Equal(t, 5, n)
	require.Equal(t, int64(5), buf.fileSize())

	// Read back.
	p := make([]byte, 5)
	nn, err := buf.readAt(p, 0)
	require.NoError(t, err)
	require.Equal(t, 5, nn)
	require.Equal(t, []byte("hello"), p)

	// Read past EOF.
	p2 := make([]byte, 10)
	nn, err = buf.readAt(p2, 3)
	require.Error(t, err) // io.EOF
	require.Equal(t, 2, nn)
	require.Equal(t, []byte("lo"), p2[:2])

	// Truncate down.
	buf.truncate(3)
	require.Equal(t, int64(3), buf.fileSize())

	// Truncate up.
	buf.truncate(10)
	require.Equal(t, int64(10), buf.fileSize())

	// Reset.
	buf.reset()
	require.False(t, buf.isExists())
	require.Equal(t, int64(0), buf.fileSize())
}
