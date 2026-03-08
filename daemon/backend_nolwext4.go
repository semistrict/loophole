//go:build linux && nolwext4

package daemon

import (
	"fmt"

	"github.com/semistrict/loophole/fsbackend"
)

func lwext4Driver() (fsbackend.AnyDriver, error) {
	return nil, fmt.Errorf("lwext4 support not compiled in (built with nolwext4 tag)")
}

func lwext4FUSEDriver() (fsbackend.AnyDriver, error) {
	return nil, fmt.Errorf("lwext4 FUSE support not compiled in (built with nolwext4 tag)")
}

// Ensure fsbackend is imported even when lwext4 types are excluded.
var _ fsbackend.AnyDriver
