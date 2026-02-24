// Package nbdvm provides an NBD device backed by a loophole Volume.
//
// It implements the github.com/Merovius/nbd Device interface (and optional
// Trimmer/WriteZeroer interfaces) so a Volume can be exposed as a kernel
// block device via nbd.Loopback.
package nbdvm

import (
	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole"
)

// VolumeIO satisfies the nbd interfaces (Device, Trimmer, WriteZeroer).
var (
	_ nbd.Device      = (*loophole.VolumeIO)(nil)
	_ nbd.Trimmer     = (*loophole.VolumeIO)(nil)
	_ nbd.WriteZeroer = (*loophole.VolumeIO)(nil)
)
