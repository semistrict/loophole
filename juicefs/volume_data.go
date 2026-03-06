package juicefs

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/semistrict/loophole"
)

// headerSize is the fixed header at the start of the volume that stores
// the logical database size. The rest of the volume is bbolt data.
const headerSize = 4096

var volumeDataMagic = [8]byte{'L', 'H', 'B', 'O', 'L', 'T', '0', '1'}

// VolumeData implements bolt.Data backed by a loophole Volume.
// The first 4096 bytes store a header with the logical size of the
// bbolt database. The database content starts at offset headerSize.
type VolumeData struct {
	vol  loophole.Volume
	size atomic.Int64 // logical size of the bolt database
}

// NewVolumeData creates a VolumeData adapter for the given volume.
// If the volume has been formatted (has the magic header), the stored
// size is loaded. Otherwise the size starts at 0.
func NewVolumeData(ctx context.Context, vol loophole.Volume) (*VolumeData, error) {
	vd := &VolumeData{vol: vol}

	hdr := make([]byte, headerSize)
	if _, err := vol.Read(ctx, hdr, 0); err != nil {
		return nil, fmt.Errorf("read volume header: %w", err)
	}

	var magic [8]byte
	copy(magic[:], hdr[:8])
	switch magic {
	case volumeDataMagic:
		vd.size.Store(int64(binary.LittleEndian.Uint64(hdr[8:16])))
	case [8]byte{}:
		// Fresh volume — size starts at 0.
	default:
		return nil, fmt.Errorf("unrecognized volume header magic: %x", magic)
	}

	return vd, nil
}

// ReadAt implements bolt.Data.ReadAt. Returns a slice of n bytes at the
// given offset within the logical database. Uses Volume.ReadAt for
// zero-copy when possible.
func (vd *VolumeData) ReadAt(off int64, n int) ([]byte, func(), error) {
	sz := vd.size.Load()
	if off < 0 || off+int64(n) > sz {
		return nil, nil, fmt.Errorf("read at %d+%d exceeds size %d", off, n, sz)
	}

	buf, release, err := vd.vol.ReadAt(context.Background(), uint64(off)+headerSize, n)
	if err != nil {
		return nil, nil, err
	}
	return buf, release, nil
}

// WriteAt implements bolt.Data.WriteAt.
func (vd *VolumeData) WriteAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}

	if err := vd.vol.Write(context.Background(), b, uint64(off)+headerSize); err != nil {
		return 0, err
	}

	// Track the logical size as the high-water mark of writes (atomic CAS loop).
	end := off + int64(len(b))
	for {
		cur := vd.size.Load()
		if end <= cur {
			break
		}
		if vd.size.CompareAndSwap(cur, end) {
			break
		}
	}

	return len(b), nil
}

// Size implements bolt.Data.Size.
func (vd *VolumeData) Size() (int64, error) {
	return vd.size.Load(), nil
}

// Grow implements bolt.Data.Grow.
func (vd *VolumeData) Grow(sz int64) error {
	cur := vd.size.Load()
	if sz <= cur {
		return nil
	}

	maxDB := int64(vd.vol.Size()) - headerSize
	if sz > maxDB {
		return fmt.Errorf("grow to %d exceeds volume capacity %d", sz, maxDB)
	}

	vd.size.Store(sz)
	return nil
}

// Sync implements bolt.Data.Sync. Flushes the header (with current size)
// and the volume.
func (vd *VolumeData) Sync() error {
	var hdr [headerSize]byte
	copy(hdr[:8], volumeDataMagic[:])
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(vd.size.Load()))

	ctx := context.Background()
	if err := vd.vol.Write(ctx, hdr[:], 0); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	return vd.vol.Flush(ctx)
}
