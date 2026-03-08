//go:build linux && !nolwext4

package daemon

import "github.com/semistrict/loophole/fsbackend"

func lwext4Driver() (fsbackend.AnyDriver, error) {
	return fsbackend.NewLwext4Driver(), nil
}

func lwext4FUSEDriver() (fsbackend.AnyDriver, error) {
	return fsbackend.NewLwext4FUSEDriver(), nil
}
