package nbd

// Trimmer is an optional interface that a Device may implement to support
// the NBD_CMD_TRIM command. Trim hints that the byte range is no longer
// needed and the backend may deallocate it.
type Trimmer interface {
	Trim(offset, length int64) error
}

// WriteZeroer is an optional interface that a Device may implement to support
// the NBD_CMD_WRITE_ZEROES command. WriteZeroes writes zeros to the given range.
// If punch is true, the backend may deallocate the range (sparse hole).
type WriteZeroer interface {
	WriteZeroes(offset, length int64, punch bool) error
}

// FUAWriter is an optional interface that a Device may implement to support
// the Force Unit Access flag. WriteAtFUA writes data and ensures it is on
// stable storage before returning.
type FUAWriter interface {
	WriteAtFUA(p []byte, off int64) (n int, err error)
}
