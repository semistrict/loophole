package loophole

import "os"

// Mode selects the block device / mount mechanism.
type Mode string

const (
	ModeFUSE       Mode = "fuse"       // FUSE block dev → loop → kernel FS (Linux, root)
	ModeNBD        Mode = "nbd"        // NBD → kernel FS (Linux, root)
	ModeTestNBDTCP Mode = "testnbdtcp" // NBD over TCP → kernel FS (Linux, root)
	ModeInProcess  Mode = "inprocess"  // Userspace FS, no kernel mount
	ModeFuseFS     Mode = "fusefs"     // Userspace FS exposed via FUSE mount (no root)
)

// NeedsRoot reports whether the mode requires root privileges.
func (m Mode) NeedsRoot() bool {
	switch m {
	case ModeInProcess, ModeFuseFS:
		return false
	default:
		return true
	}
}

// FSType selects the filesystem engine.
// For block-device modes (fuse, nbd, testnbdtcp): ext4 or xfs (kernel filesystem).
// For userspace modes (inprocess, fusefs): ext4 (lwext4) or juicefs.
type FSType string

const (
	FSExt4    FSType = "ext4"    // ext4 (kernel or lwext4 userspace)
	FSXFS     FSType = "xfs"     // XFS (kernel only, block-device modes)
	FSJuiceFS FSType = "juicefs" // JuiceFS (userspace modes only)
)

// DefaultFSType returns the default filesystem type from LOOPHOLE_DEFAULT_FS.
func DefaultFSType() FSType {
	if v := os.Getenv("LOOPHOLE_DEFAULT_FS"); v != "" {
		return FSType(v)
	}
	return FSExt4
}
