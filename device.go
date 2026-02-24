package loophole

import "os"

// Mode selects the block device mechanism.
type Mode string

const (
	ModeFUSE Mode = "fuse"
	ModeNBD  Mode = "nbd"
)

// ModeFromEnv reads LOOPHOLE_MODE from the environment. Defaults to "fuse".
func ModeFromEnv() Mode {
	if os.Getenv("LOOPHOLE_MODE") == "nbd" {
		return ModeNBD
	}
	return ModeFUSE
}
