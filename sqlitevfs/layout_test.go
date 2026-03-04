package sqlitevfs

import (
	"testing"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lsm"
	"github.com/stretchr/testify/require"
)

func testManager(t *testing.T) loophole.VolumeManager {
	t.Helper()
	store := loophole.NewMemStore()
	m := lsm.NewVolumeManager(store, t.TempDir(), lsm.Config{
		FlushThreshold:  16 * lsm.PageSize,
		MaxFrozenLayers: 2,
		PageCacheBytes:  16 * lsm.PageSize,
	}, nil)
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

func TestDefaultRegions(t *testing.T) {
	// 1GB volume.
	size := uint64(1024 * 1024 * 1024)
	regions := DefaultRegions(size)

	// Check that regions don't overlap and cover the volume.
	require.Equal(t, uint64(superblockSize), regions[RegionMainDB].RegionStart)

	for i := range numRegions {
		require.NotEmpty(t, regions[i].Name)
		require.True(t, regions[i].RegionCapacity > 0, "region %d has zero capacity", i)
		end := regions[i].RegionStart + regions[i].RegionCapacity
		require.True(t, end <= size, "region %d exceeds volume size", i)
	}

	// SHM is at the very end.
	require.Equal(t, size-shmRegionSize, regions[RegionSHM].RegionStart)
	require.Equal(t, uint64(shmRegionSize), regions[RegionSHM].RegionCapacity)

	// WAL and journal each get size/16 = 64MB.
	require.Equal(t, size/16, regions[RegionWAL].RegionCapacity)
	require.Equal(t, size/16, regions[RegionJournal].RegionCapacity)

	// MainDB gets the rest.
	expectedMainDB := size - uint64(superblockSize) - regions[RegionJournal].RegionCapacity - regions[RegionWAL].RegionCapacity - regions[RegionSHM].RegionCapacity
	require.Equal(t, expectedMainDB, regions[RegionMainDB].RegionCapacity)

	// Check non-overlapping: each region starts where the previous one ends.
	require.Equal(t, regions[RegionMainDB].RegionStart+regions[RegionMainDB].RegionCapacity, regions[RegionJournal].RegionStart)
	require.Equal(t, regions[RegionJournal].RegionStart+regions[RegionJournal].RegionCapacity, regions[RegionWAL].RegionStart)
	require.Equal(t, regions[RegionWAL].RegionStart+regions[RegionWAL].RegionCapacity, regions[RegionSHM].RegionStart)
	require.Equal(t, regions[RegionSHM].RegionStart+regions[RegionSHM].RegionCapacity, size)
}

func TestDefaultRegionsMinSize(t *testing.T) {
	// 256MB — minimum size.
	size := uint64(MinVolumeSize)
	regions := DefaultRegions(size)

	// Journal and WAL get minimum 64MB each.
	require.Equal(t, uint64(minJournalSize), regions[RegionJournal].RegionCapacity)
	require.Equal(t, uint64(minWALSize), regions[RegionWAL].RegionCapacity)

	// MainDB gets the rest: 256MB - 64KB - 64MB - 64MB - 1MB.
	expectedMainDB := size - uint64(superblockSize) - uint64(minJournalSize) - uint64(minWALSize) - uint64(shmRegionSize)
	require.Equal(t, expectedMainDB, regions[RegionMainDB].RegionCapacity)
}

func TestSuperblockRoundTrip(t *testing.T) {
	vol := testVolume(t, 512*1024*1024) // 512MB

	sb, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	// Read it back.
	sb2, err := ReadSuperblock(t.Context(), vol)
	require.NoError(t, err)

	for i := range numRegions {
		require.Equal(t, sb.Regions[i].Name, sb2.Regions[i].Name)
		require.Equal(t, sb.Regions[i].RegionStart, sb2.Regions[i].RegionStart)
		require.Equal(t, sb.Regions[i].RegionCapacity, sb2.Regions[i].RegionCapacity)
		require.Equal(t, sb.Regions[i].FileSize, sb2.Regions[i].FileSize)
		require.Equal(t, sb.Regions[i].Flags, sb2.Regions[i].Flags)
	}
}

func TestFormatVolumeTooSmall(t *testing.T) {
	vol := testVolume(t, 128*1024*1024) // 128MB — below minimum.
	_, err := FormatVolume(t.Context(), vol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "volume too small")
}

func TestReadSuperblockUnformatted(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	// Volume is all zeros — should fail with bad magic.
	_, err := ReadSuperblock(t.Context(), vol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid superblock magic")
}

func TestSuperblockFileTableCRUD(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)

	sb, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	// Initially no files exist.
	for i := range numRegions {
		require.False(t, sb.Regions[i].Exists())
		require.Equal(t, uint64(0), sb.Regions[i].FileSize)
	}

	// Mark main.db as existing with some size.
	sb.Regions[RegionMainDB].Flags |= flagExists
	sb.Regions[RegionMainDB].FileSize = 4096

	require.NoError(t, writeSuperblock(t.Context(), vol, sb))

	sb2, err := ReadSuperblock(t.Context(), vol)
	require.NoError(t, err)
	require.True(t, sb2.Regions[RegionMainDB].Exists())
	require.Equal(t, uint64(4096), sb2.Regions[RegionMainDB].FileSize)
	require.False(t, sb2.Regions[RegionJournal].Exists())

	// Clear the main.db entry.
	sb2.Regions[RegionMainDB].Flags = 0
	sb2.Regions[RegionMainDB].FileSize = 0
	require.NoError(t, writeSuperblock(t.Context(), vol, sb2))

	sb3, err := ReadSuperblock(t.Context(), vol)
	require.NoError(t, err)
	require.False(t, sb3.Regions[RegionMainDB].Exists())
}
