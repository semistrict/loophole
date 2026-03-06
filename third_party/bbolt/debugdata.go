package bbolt

// DebugData wraps a Data implementation and zeroes buffers returned by
// ReadAt when their release function is called.  This catches
// use-after-release bugs: any code that reads from a buffer after
// releasing it will see zeros instead of stale data, making the
// problem immediately visible.
type DebugData struct {
	Data
}

func (d *DebugData) ReadAt(off int64, n int) ([]byte, func(), error) {
	buf, release, err := d.Data.ReadAt(off, n)
	if err != nil {
		return nil, nil, err
	}
	return buf, func() {
		release()
		go clear(buf)
	}, nil
}
