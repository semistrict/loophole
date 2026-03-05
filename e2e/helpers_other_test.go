//go:build !linux

package e2e

import "testing"

func syncFS(t *testing.T, mountpoint string) {
	t.Helper()
	t.Fatal("syncFS not supported on this platform")
}
