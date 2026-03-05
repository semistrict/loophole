package sqlitevfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenCreateReadWrite(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	// Open main.db with CREATE.
	f, flags, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	require.NotZero(t, flags&OpenReadWrite)

	// File size starts at 0.
	sz, err := f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(0), sz)

	// Write some data.
	data := []byte("hello world, this is a test page")
	n, err := f.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// File size extended.
	sz, err = f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), sz)

	// Read it back.
	buf := make([]byte, len(data))
	n, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	require.NoError(t, f.Close())
}

func TestReadPastEOF(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	// Write 16 bytes.
	_, err = f.WriteAt([]byte("0123456789abcdef"), 0)
	require.NoError(t, err)

	// Read 32 bytes starting at offset 8 — crosses EOF.
	buf := make([]byte, 32)
	n, err := f.ReadAt(buf, 8)
	require.Error(t, err) // io.EOF
	require.Equal(t, 8, n)
	require.Equal(t, []byte("89abcdef"), buf[:8])
	// Remainder should be zeros.
	for i := 8; i < 32; i++ {
		require.Equal(t, byte(0), buf[i])
	}

	require.NoError(t, f.Close())
}

func TestTruncate(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	// Write 4096 bytes.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err = f.WriteAt(data, 0)
	require.NoError(t, err)

	sz, err := f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(4096), sz)

	// Truncate to 1024.
	require.NoError(t, f.Truncate(1024))

	sz, err = f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(1024), sz)

	// Extend via truncate.
	require.NoError(t, f.Truncate(8192))
	sz, err = f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(8192), sz)

	require.NoError(t, f.Close())
}

func TestDeleteAndAccess(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	// File doesn't exist yet.
	exists, err := vfs.Access("main.db-journal", AccessExists)
	require.NoError(t, err)
	require.False(t, exists)

	// Create it.
	f, _, err := vfs.Open("main.db-journal", OpenReadWrite|OpenCreate|OpenMainJournal)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("journal data"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Now it exists.
	exists, err = vfs.Access("main.db-journal", AccessExists)
	require.NoError(t, err)
	require.True(t, exists)

	// Delete it.
	require.NoError(t, vfs.Delete("main.db-journal", false))

	// Gone.
	exists, err = vfs.Access("main.db-journal", AccessExists)
	require.NoError(t, err)
	require.False(t, exists)

	// Delete again is idempotent.
	require.NoError(t, vfs.Delete("main.db-journal", false))
}

func TestOpenNonexistent(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	// Open without CREATE should fail.
	_, _, err = vfs.Open("main.db", OpenReadWrite|OpenMainDB)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestOpenUnknownFile(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	_, _, err = vfs.Open("unknown.db", OpenReadWrite|OpenCreate)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown file")
}

func TestFullPathname(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	name := vfs.FullPathname("main.db")
	require.Equal(t, "main.db", name)
}

func TestSyncFlushes(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	_, err = f.WriteAt([]byte("some data"), 0)
	require.NoError(t, err)

	// Sync should not error.
	require.NoError(t, f.Sync(SyncFull))

	require.NoError(t, f.Close())
}

func TestAsyncSyncIsNoop(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeAsync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	_, err = f.WriteAt([]byte("some data"), 0)
	require.NoError(t, err)

	// Sync is a no-op in async mode.
	require.NoError(t, f.Sync(SyncFull))

	require.NoError(t, f.Close())
}

func TestDeviceCharacteristics(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)

	// Sync mode.
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	chars := f.DeviceCharacteristics()
	require.NotZero(t, chars&IocapAtomic4K)
	require.NotZero(t, chars&IocapSafeAppend)
	require.NotZero(t, chars&IocapSequential)
	require.Zero(t, chars&IocapPowersafeOverwrite) // not set in sync mode

	require.NoError(t, f.Close())
}

func TestDeviceCharacteristicsAsync(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeAsync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	chars := f.DeviceCharacteristics()
	require.NotZero(t, chars&IocapPowersafeOverwrite) // set in async mode

	require.NoError(t, f.Close())
}

func TestSectorSize(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	require.Equal(t, int64(4096), f.SectorSize())
	require.NoError(t, f.Close())
}

func TestLockUnlock(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)

	reserved, err := f.CheckReservedLock()
	require.NoError(t, err)
	require.False(t, reserved)

	require.NoError(t, f.Lock(LockShared))
	reserved, err = f.CheckReservedLock()
	require.NoError(t, err)
	require.False(t, reserved)

	require.NoError(t, f.Lock(LockReserved))
	reserved, err = f.CheckReservedLock()
	require.NoError(t, err)
	require.True(t, reserved)

	require.NoError(t, f.Lock(LockExclusive))
	require.NoError(t, f.Unlock(LockShared))

	reserved, err = f.CheckReservedLock()
	require.NoError(t, err)
	require.False(t, reserved)

	require.NoError(t, f.Unlock(LockNone))
	require.NoError(t, f.Close())
}

func TestMultipleRegions(t *testing.T) {
	vol := testVolume(t, 512*1024*1024)
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	// Open all four region files.
	mainF, _, err := vfs.Open("main.db", OpenReadWrite|OpenCreate|OpenMainDB)
	require.NoError(t, err)
	journalF, _, err := vfs.Open("main.db-journal", OpenReadWrite|OpenCreate|OpenMainJournal)
	require.NoError(t, err)
	walF, _, err := vfs.Open("main.db-wal", OpenReadWrite|OpenCreate|OpenWAL)
	require.NoError(t, err)
	shmF, _, err := vfs.Open("main.db-shm", OpenReadWrite|OpenCreate|OpenSubJournal)
	require.NoError(t, err)

	// Write distinct data to each.
	_, err = mainF.WriteAt([]byte("MAIN"), 0)
	require.NoError(t, err)
	_, err = journalF.WriteAt([]byte("JOURNAL"), 0)
	require.NoError(t, err)
	_, err = walF.WriteAt([]byte("WAL"), 0)
	require.NoError(t, err)
	_, err = shmF.WriteAt([]byte("SHM"), 0)
	require.NoError(t, err)

	// Read back and verify isolation.
	buf := make([]byte, 7)
	n, err := mainF.ReadAt(buf[:4], 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("MAIN"), buf[:4])

	n, err = journalF.ReadAt(buf[:7], 0)
	require.NoError(t, err)
	require.Equal(t, 7, n)
	require.Equal(t, []byte("JOURNAL"), buf[:7])

	require.NoError(t, mainF.Close())
	require.NoError(t, journalF.Close())
	require.NoError(t, walF.Close())
	require.NoError(t, shmF.Close())
}

func TestWriteExceedsCapacity(t *testing.T) {
	vol := testVolume(t, 256*1024*1024) // minimum size
	_, err := FormatVolume(t.Context(), vol)
	require.NoError(t, err)

	vfs, err := NewVolumeVFS(t.Context(), vol, SyncModeSync)
	require.NoError(t, err)

	f, _, err := vfs.Open("main.db-shm", OpenReadWrite|OpenCreate|OpenSubJournal)
	require.NoError(t, err)

	// SHM region is only 1MB. Writing 2MB should fail.
	big := make([]byte, 2*1024*1024)
	_, err = f.WriteAt(big, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds region capacity")

	require.NoError(t, f.Close())
}
