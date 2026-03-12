//go:build !linux

package fsbackend

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fuseblockdev"
)

type fuseMount struct{}

type FUSEDriver struct{}

func NewFUSEDriver(_ string, _ loophole.VolumeManager, _ *fuseblockdev.Options) (*FUSEDriver, error) {
	return nil, fmt.Errorf("FUSE backend is only supported on Linux")
}

func NewFUSE(_ string, _ loophole.VolumeManager, _ *fuseblockdev.Options) (Service, error) {
	return nil, fmt.Errorf("FUSE backend is only supported on Linux")
}

func (f *FUSEDriver) Format(context.Context, loophole.Volume) error {
	return fmt.Errorf("FUSE backend is only supported on Linux")
}
func (f *FUSEDriver) Mount(context.Context, loophole.Volume, string) (fuseMount, error) {
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
func (f *FUSEDriver) RegisterVolume(string, loophole.Volume) {}
func (f *FUSEDriver) UnregisterVolume(string)                {}
func (f *FUSEDriver) DevicePath(string) string               { return "" }
func (f *FUSEDriver) FS(fuseMount) (FS, error) {
	return nil, fmt.Errorf("FUSE backend is only supported on Linux")
}
