package storage

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// sparseBlockIter iterates over data-bearing blocks in a file, skipping
// sparse holes via SEEK_HOLE/SEEK_DATA. Falls back to linear iteration
// if the filesystem doesn't support these syscalls.
type sparseBlockIter struct {
	f         *os.File
	blockSize int64
	fileSize  int64
	offset    int64 // current file offset
	sparse    bool  // true if SEEK_DATA/SEEK_HOLE are supported
}

func newSparseBlockIter(f *os.File, blockSize int) (*sparseBlockIter, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	it := &sparseBlockIter{
		f:         f,
		blockSize: int64(blockSize),
		fileSize:  info.Size(),
	}

	// Probe for SEEK_DATA support by seeking from offset 0.
	_, err = f.Seek(0, unix.SEEK_DATA)
	if err == nil {
		it.sparse = true
		// Reset to start.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}
	// If SEEK_DATA failed, sparse=false and we do linear reads.

	return it, nil
}

// next returns the block index and data for the next data-bearing block.
// Returns io.EOF when the file is exhausted.
func (it *sparseBlockIter) next(buf []byte) (blockIdx BlockIdx, n int, err error) {
	if it.offset >= it.fileSize {
		return 0, 0, io.EOF
	}

	if it.sparse {
		// Find next data region at or after current offset.
		dataStart, err := it.f.Seek(it.offset, unix.SEEK_DATA)
		if err != nil {
			// ENXIO means no more data after this offset — we're done.
			return 0, 0, io.EOF
		}

		// Align to block boundary.
		alignedStart := (dataStart / it.blockSize) * it.blockSize
		if alignedStart != it.offset {
			it.offset = alignedStart
			if _, err := it.f.Seek(it.offset, io.SeekStart); err != nil {
				return 0, 0, err
			}
		}
	}

	blockAddr := BlockIdx(it.offset / it.blockSize)
	n, readErr := io.ReadFull(it.f, buf)
	it.offset += it.blockSize

	if n > 0 {
		return blockAddr, n, nil
	}
	if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
		return 0, 0, io.EOF
	}
	return 0, 0, readErr
}
