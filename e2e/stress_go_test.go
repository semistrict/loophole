package e2e

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"io"
	mrand "math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/fsbackend"
)

// Go reimplementations of the fsx/fio stress tests so they run without
// external tools and work in all modes (including lwext4fuse).

// rngFill fills p with pseudo-random bytes from rng.
func rngFill(rng *mrand.Rand, p []byte) {
	for i := 0; i < len(p); i += 8 {
		v := rng.Uint64()
		for j := range min(8, len(p)-i) {
			p[i+j] = byte(v >> (j * 8))
		}
	}
}

// stressMountGo creates a volume and returns the testFS.
func stressMountGo(t *testing.T, name string) testFS {
	t.Helper()
	tfs, _ := mountVolume(t, name)
	return tfs
}

// createFile creates a file via fsbackend.FS and returns the handle.
func createFile(t *testing.T, tfs testFS, name string) fsbackend.File {
	t.Helper()
	f, err := tfs.fs.Create(name)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// openFile opens a file via fsbackend.FS and returns the handle.
func openFile(t *testing.T, tfs testFS, name string) fsbackend.File {
	t.Helper()
	f, err := tfs.fs.Open(name)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// --- fsx-like tests ---

// fsxRun performs N random file operations (read, write, truncate) on a single
// file, verifying the file contents match an in-memory reference after each op.
func fsxRun(t *testing.T, f fsbackend.File, fileSize, ops int, seed int64) {
	t.Helper()

	rng := mrand.New(mrand.NewPCG(uint64(seed), 0))

	// Reference buffer (ground truth).
	ref := make([]byte, 0, fileSize)

	for i := range ops {
		op := rng.IntN(4) // 0=write, 1=read, 2=truncate, 3=write
		curLen := len(ref)

		switch op {
		case 0, 3: // write
			offset := 0
			if curLen > 0 {
				offset = rng.IntN(curLen)
			}
			maxLen := fileSize - offset
			if maxLen <= 0 {
				continue
			}
			writeLen := rng.IntN(maxLen) + 1
			data := make([]byte, writeLen)
			rngFill(rng, data)

			// Extend ref if needed.
			endPos := offset + writeLen
			for len(ref) < endPos {
				ref = append(ref, 0)
			}
			copy(ref[offset:], data)

			_, err := f.WriteAt(data, int64(offset))
			require.NoError(t, err, "op %d: write at %d len %d", i, offset, writeLen)

		case 1: // read + verify
			if curLen == 0 {
				continue
			}
			offset := rng.IntN(curLen)
			maxLen := curLen - offset
			if maxLen <= 0 {
				continue
			}
			readLen := rng.IntN(maxLen) + 1
			buf := make([]byte, readLen)
			n, err := f.ReadAt(buf, int64(offset))
			if err != nil && err != io.EOF {
				require.NoError(t, err, "op %d: read at %d len %d", i, offset, readLen)
			}
			require.Equal(t, ref[offset:offset+n], buf[:n],
				"op %d: data mismatch at offset %d len %d", i, offset, n)

		case 2: // truncate
			newLen := rng.IntN(fileSize)
			require.NoError(t, f.Truncate(int64(newLen)), "op %d: truncate to %d", i, newLen)
			if newLen < len(ref) {
				ref = ref[:newLen]
			} else {
				for len(ref) < newLen {
					ref = append(ref, 0)
				}
			}
		}
	}

	// Final full-file verify.
	_, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	actual, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, len(ref), len(actual), "final size mismatch")
	require.Equal(t, ref, actual, "final data mismatch")
}

func TestE2E_GoFsxBasic(t *testing.T) {
	tfs := stressMountGo(t, "gofsx-basic")
	f := createFile(t, tfs, "fsx-testfile")
	fsxRun(t, f, 1048576, 5000, 42)
}

func TestE2E_GoFsxHeavy(t *testing.T) {
	tfs := stressMountGo(t, "gofsx-heavy")
	f := createFile(t, tfs, "fsx-heavy-testfile")
	fsxRun(t, f, 4194304, 20000, 999)
}

// --- fio-like tests ---

// TestE2E_GoFioRandomRW does random 4K reads and writes across multiple
// goroutines with CRC32 verification (approximates fio --rw=randrw --verify=crc32c).
func TestE2E_GoFioRandomRW(t *testing.T) {
	tfs := stressMountGo(t, "gofio-randrw")
	size := 8 * 1024 * 1024
	numJobs := 2
	numBlocks := size / 4096

	var wg sync.WaitGroup
	for job := range numJobs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			f, err := tfs.fs.Create(fmt.Sprintf("randrw.%d", job))
			require.NoError(t, err)
			defer f.Close()

			// Pre-fill file.
			buf := make([]byte, size)
			_, err = rand.Read(buf)
			require.NoError(t, err)
			_, err = f.Write(buf)
			require.NoError(t, err)

			// Track CRC per block.
			crcs := make([]uint32, numBlocks)
			for i := range numBlocks {
				crcs[i] = crc32.ChecksumIEEE(buf[i*4096 : (i+1)*4096])
			}

			rng := mrand.New(mrand.NewPCG(uint64(job), 0))
			blk := make([]byte, 4096)

			for range 2000 {
				idx := rng.IntN(numBlocks)
				off := int64(idx) * 4096

				if rng.IntN(2) == 0 {
					// Write
					rngFill(rng, blk)
					_, err := f.WriteAt(blk, off)
					require.NoError(t, err)
					crcs[idx] = crc32.ChecksumIEEE(blk)
				} else {
					// Read + verify
					_, err := f.ReadAt(blk, off)
					require.NoError(t, err)
					got := crc32.ChecksumIEEE(blk)
					require.Equal(t, crcs[idx], got,
						"job %d: CRC mismatch at block %d", job, idx)
				}
			}
		}()
	}
	wg.Wait()
}

// TestE2E_GoFioSequentialWriteVerify writes a file sequentially in 64K blocks
// with SHA256 checksums, then reads back and verifies each block
// (approximates fio --rw=write --verify=sha256 + --verify_only).
func TestE2E_GoFioSequentialWriteVerify(t *testing.T) {
	tfs := stressMountGo(t, "gofio-seqver")
	blockSize := 65536
	size := 16 * 1024 * 1024
	numBlocks := size / blockSize

	f, err := tfs.fs.Create("seqwrite.0")
	require.NoError(t, err)

	hashes := make([][32]byte, numBlocks)
	buf := make([]byte, blockSize)

	// Write phase.
	for i := range numBlocks {
		_, err := rand.Read(buf)
		require.NoError(t, err)
		hashes[i] = sha256.Sum256(buf)
		_, err = f.Write(buf)
		require.NoError(t, err)
	}
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Verify phase.
	f2 := openFile(t, tfs, "seqwrite.0")

	for i := range numBlocks {
		_, err := io.ReadFull(f2, buf)
		require.NoError(t, err)
		got := sha256.Sum256(buf)
		require.Equal(t, hashes[i], got, "block %d SHA256 mismatch", i)
	}
}

// TestE2E_GoFioRandomWriteVerify writes random 4K blocks then reads them all
// back with CRC32 verification (approximates fio --fallocate=keep --verify=crc32c).
func TestE2E_GoFioRandomWriteVerify(t *testing.T) {
	tfs := stressMountGo(t, "gofio-randwr")
	blockSize := 4096
	size := 4 * 1024 * 1024
	numBlocks := size / blockSize

	f := createFile(t, tfs, "randwrite.0")

	// Pre-allocate by writing zeros.
	require.NoError(t, f.Truncate(int64(size)))

	crcs := make([]uint32, numBlocks)
	buf := make([]byte, blockSize)

	rng := mrand.New(mrand.NewPCG(77, 0))

	// Random write phase.
	for range 5000 {
		idx := rng.IntN(numBlocks)
		rngFill(rng, buf)
		_, err := f.WriteAt(buf, int64(idx)*int64(blockSize))
		require.NoError(t, err)
		crcs[idx] = crc32.ChecksumIEEE(buf)
	}
	require.NoError(t, f.Sync())

	// Verify phase — read every block, check the ones we wrote.
	for i := range numBlocks {
		if crcs[i] == 0 {
			continue // never written, skip
		}
		_, err := f.ReadAt(buf, int64(i)*int64(blockSize))
		require.NoError(t, err)
		got := crc32.ChecksumIEEE(buf)
		require.Equal(t, crcs[i], got, "block %d CRC mismatch", i)
	}
}
