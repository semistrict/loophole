package loophole

import "os"

// DefaultMode returns the default mode for Linux.
// Reads LOOPHOLE_MODE from the environment; defaults to "fuse".
func DefaultMode() Mode {
	switch os.Getenv("LOOPHOLE_MODE") {
	case "nbd":
		return ModeNBD
	case "testnbdtcp":
		return ModeTestNBDTCP
	case "lwext4fuse":
		return ModeLwext4FUSE
	case "juicefs":
		return ModeJuiceFS
	case "juicefsfuse":
		return ModeJuiceFSFuse
	default:
		return ModeFUSE
	}
}
