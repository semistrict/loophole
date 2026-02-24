package loophole

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile creates dst as a reflink clone of src (btrfs, xfs, etc.).
func cloneFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer func() {
		dst.Close()
		if err != nil {
			os.Remove(dstPath)
		}
	}()

	err = unix.IoctlFileClone(int(dst.Fd()), int(src.Fd()))
	return err
}
