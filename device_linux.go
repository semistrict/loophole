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
	case "inprocess":
		return ModeInProcess
	case "fusefs":
		return ModeFuseFS
	default:
		return ModeFUSE
	}
}
