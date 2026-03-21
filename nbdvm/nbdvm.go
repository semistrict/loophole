// Package nbdvm provides an NBD device backed by a loophole Volume.
//
// It implements the github.com/Merovius/nbd Device interface (and optional
// Trimmer/WriteZeroer interfaces) so a Volume can be exposed as a kernel
// block device via nbd.Loopback.
package nbdvm

import (
	"context"
	"log/slog"

	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole/storage"
)

// volumeNBD adapts a *Volume to the nbd.Device/Trimmer/WriteZeroer interfaces
// by passing context.Background() to every call.
type volumeNBD struct {
	vol *storage.Volume
}

var (
	_ nbd.Device      = (*volumeNBD)(nil)
	_ nbd.Trimmer     = (*volumeNBD)(nil)
	_ nbd.WriteZeroer = (*volumeNBD)(nil)
	_                 = nbd.ExportFlags // compile check: patched nbd library
)

// NewDevice wraps a Volume as an NBD device.
func NewDevice(vol *storage.Volume) volumeNBD {
	return volumeNBD{vol: vol}
}

func (d volumeNBD) ReadAt(p []byte, off int64) (int, error) {
	return d.vol.Read(context.Background(), p, uint64(off))
}

func (d volumeNBD) WriteAt(p []byte, off int64) (int, error) {
	if err := d.vol.Write(p, uint64(off)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (d volumeNBD) Sync() error {
	err := d.vol.Flush()
	if err != nil {
		slog.Error("volumeNBD.Sync failed", "volume", d.vol.Name(), "error", err)
	}
	return err
}

func (d volumeNBD) Trim(offset, length int64) error {
	return d.vol.PunchHole(uint64(offset), uint64(length))
}

func (d volumeNBD) WriteZeroes(offset, length int64, _ bool) error {
	return d.vol.PunchHole(uint64(offset), uint64(length))
}
