package loophole

import "golang.org/x/sys/unix"

// cloneFile creates dst as a reflink clone of src (APFS only).
func cloneFile(src, dst string) error {
	return unix.Clonefile(src, dst, 0)
}
