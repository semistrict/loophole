package loophole

import (
	"log/slog"
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile creates dst as a reflink clone of src (btrfs, xfs, etc.).
func cloneFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := src.Close(); err != nil {
			slog.Warn("close failed", "error", err)
		}
	}()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dst.Close(); cerr != nil {
			slog.Warn("close failed", "error", cerr)
		}
		if err != nil {
			if rerr := os.Remove(dstPath); rerr != nil {
				slog.Warn("remove failed", "path", dstPath, "error", rerr)
			}
		}
	}()

	err = unix.IoctlFileClone(int(dst.Fd()), int(src.Fd()))
	return err
}
