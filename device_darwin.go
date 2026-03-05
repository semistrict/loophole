package loophole

import (
	"fmt"
	"os"
)

// DefaultMode returns the default mode for macOS.
// Only userspace (FUSE+lwext4) is supported. Requires macFUSE to be installed.
func DefaultMode() Mode {
	switch os.Getenv("LOOPHOLE_MODE") {
	case string(ModeInProcess):
		return ModeInProcess
	case string(ModeLwext4FUSE):
		return ModeLwext4FUSE
	case string(ModeJuiceFS):
		return ModeJuiceFS
	case "":
		// macOS only supports userspace mode; verify macFUSE is present.
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil {
			fmt.Fprintln(os.Stderr, "error: macFUSE is not installed (expected /Library/Filesystems/macfuse.fs)")
			fmt.Fprintln(os.Stderr, "Install it from https://osxfuse.github.io/")
			os.Exit(1)
		}
		return ModeLwext4FUSE
	default:
		fmt.Fprintf(os.Stderr, "error: unsupported LOOPHOLE_MODE=%q on macOS\n", os.Getenv("LOOPHOLE_MODE"))
		os.Exit(1)
		return "" // unreachable
	}
}
