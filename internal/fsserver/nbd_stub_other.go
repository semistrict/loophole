//go:build !linux

package fsserver

import (
	"context"

	"github.com/semistrict/loophole/internal/storage"
)

type nbdMount struct{}

type NBDDriver struct{}

func NewNBDDriver(_ *storage.Manager, _ any) (*NBDDriver, error) {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Format(context.Context, *storage.Volume) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Mount(context.Context, *storage.Volume, string) (nbdMount, error) {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Unmount(context.Context, nbdMount) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Freeze(context.Context, nbdMount) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Thaw(context.Context, nbdMount) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) Close(context.Context) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) ConnectDevice(context.Context, *storage.Volume) (string, error) {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) DisconnectDevice(context.Context, string) error {
	panic("NBD backend is only supported on Linux")
}

func (n *NBDDriver) DevicePath(string) string {
	panic("NBD backend is only supported on Linux")
}
