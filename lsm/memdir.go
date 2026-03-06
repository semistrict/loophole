//go:build !js

package lsm

import "os"

func ensureMemDir(dir string) error {
	return os.MkdirAll(dir, 0o755)
}
