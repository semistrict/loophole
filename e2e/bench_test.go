package e2e

import (
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var benchSeq atomic.Int64

// benchVolumeName returns a unique volume name per benchmark invocation.
// Go's benchmark framework calls the function multiple times with increasing
// b.N, and the shared daemon can't reuse volume names within a process.
func benchVolumeName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, benchSeq.Add(1))
}

// BenchmarkSequentialWrite writes 64K blocks sequentially, wrapping within a fixed file.
func BenchmarkSequentialWrite(b *testing.B) {
	tfs, _ := mountVolume(b, benchVolumeName("bench-seqw"))

	const fileSize = 64 * 1024 * 1024
	const blockSize = 65536
	numBlocks := fileSize / blockSize

	buf := make([]byte, blockSize)
	rand.Read(buf)

	f, err := tfs.fs.Create("seqwrite.dat")
	require.NoError(b, err)
	b.Cleanup(func() { f.Close() })

	// Pre-allocate to avoid growing the file during the benchmark.
	require.NoError(b, f.Truncate(fileSize))

	b.SetBytes(int64(blockSize))
	b.ResetTimer()
	for i := range b.N {
		off := int64(i%numBlocks) * int64(blockSize)
		_, err := f.WriteAt(buf, off)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	f.Sync()
}

// BenchmarkSequentialRead reads 64K blocks sequentially from a pre-written file.
func BenchmarkSequentialRead(b *testing.B) {
	tfs, _ := mountVolume(b, benchVolumeName("bench-seqr"))

	const fileSize = 64 * 1024 * 1024
	const blockSize = 65536
	numBlocks := fileSize / blockSize

	func() {
		f, err := tfs.fs.Create("seqread.dat")
		require.NoError(b, err)
		defer f.Close()
		buf := make([]byte, blockSize)
		rand.Read(buf)
		for i := range numBlocks {
			_, err := f.WriteAt(buf, int64(i)*int64(blockSize))
			require.NoError(b, err)
		}
		f.Sync()
	}()

	f, err := tfs.fs.Open("seqread.dat")
	require.NoError(b, err)
	b.Cleanup(func() { f.Close() })

	buf := make([]byte, blockSize)
	b.SetBytes(int64(blockSize))
	b.ResetTimer()
	for i := range b.N {
		off := int64(i%numBlocks) * int64(blockSize)
		_, err := f.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}

// BenchmarkRandom4KWrite writes random 4K blocks.
func BenchmarkRandom4KWrite(b *testing.B) {
	tfs, _ := mountVolume(b, benchVolumeName("bench-r4kw"))

	const fileSize = 32 * 1024 * 1024
	const blockSize = 4096
	numBlocks := fileSize / blockSize

	f, err := tfs.fs.Create("rand4kw.dat")
	require.NoError(b, err)
	b.Cleanup(func() { f.Close() })

	require.NoError(b, f.Truncate(fileSize))

	buf := make([]byte, blockSize)
	rng := mrand.New(mrand.NewPCG(42, 0))

	b.SetBytes(int64(blockSize))
	b.ResetTimer()
	for range b.N {
		idx := rng.IntN(numBlocks)
		rand.Read(buf)
		_, err := f.WriteAt(buf, int64(idx)*int64(blockSize))
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	f.Sync()
}

// BenchmarkRandom4KRead reads random 4K blocks from a pre-written file.
func BenchmarkRandom4KRead(b *testing.B) {
	tfs, _ := mountVolume(b, benchVolumeName("bench-r4kr"))

	const fileSize = 32 * 1024 * 1024
	const blockSize = 4096
	numBlocks := fileSize / blockSize

	func() {
		f, err := tfs.fs.Create("rand4kr.dat")
		require.NoError(b, err)
		defer f.Close()
		buf := make([]byte, fileSize)
		rand.Read(buf)
		_, err = f.Write(buf)
		require.NoError(b, err)
		f.Sync()
	}()

	f, err := tfs.fs.Open("rand4kr.dat")
	require.NoError(b, err)
	b.Cleanup(func() { f.Close() })

	buf := make([]byte, blockSize)
	rng := mrand.New(mrand.NewPCG(42, 0))

	b.SetBytes(int64(blockSize))
	b.ResetTimer()
	for range b.N {
		idx := rng.IntN(numBlocks)
		_, err := f.ReadAt(buf, int64(idx)*int64(blockSize))
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}
