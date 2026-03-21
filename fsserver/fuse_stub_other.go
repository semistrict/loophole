//go:build !linux

package fsserver

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/storage"
)

type fuseMount struct{}

type FUSEDriver struct{}

func NewFUSEDriver(_ string, _ *storage.Manager, _ *fuseblockdev.Options) (*FUSEDriver, error) {
	return nil, fmt.Errorf("FUSE backend is only supported on Linux")
}

func (f *FUSEDriver) Format(context.Context, *storage.Volume) error {
	return fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Mount(context.Context, *storage.Volume, string) (fuseMount, error) {
	return fuseMount{}, fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Unmount(context.Context, fuseMount) error {
	return fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Freeze(context.Context, fuseMount) error {
	return fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Thaw(context.Context, fuseMount) error {
	return fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Close(context.Context) error            { return nil }
func (f *FUSEDriver) RegisterVolume(string, *storage.Volume) {}
func (f *FUSEDriver) UnregisterVolume(string)                {}
func (f *FUSEDriver) DevicePath(string) string               { return "" }
