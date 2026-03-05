package loophole

import (
	"fmt"
	"os"
)

// DefaultMode returns the default mode for macOS.
// Only userspace modes are supported. fusefs requires macFUSE.
func DefaultMode() Mode {
	switch os.Getenv("LOOPHOLE_MODE") {
	case string(ModeInProcess):
		return ModeInProcess
	case string(ModeFuseFS), "":
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil {
			fmt.Fprintln(os.Stderr, "error: macFUSE is not installed (expected /Library/Filesystems/macfuse.fs)")
			fmt.Fprintln(os.Stderr, "Install it from https://osxfuse.github.io/")
			os.Exit(1)
		}
		return ModeFuseFS
	default:
		fmt.Fprintf(os.Stderr, "error: unsupported LOOPHOLE_MODE=%q on macOS\n", os.Getenv("LOOPHOLE_MODE"))
		os.Exit(1)
		return "" // unreachable
	}
}
