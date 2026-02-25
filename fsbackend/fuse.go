//go:build linux

package fsbackend

import (
	"context"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/ext4"
	"github.com/semistrict/loophole/fuseblockdev"
)

// fuseMount is the per-mount handle for FUSEDriver.
type fuseMount struct {
	mountpoint string
	loopDev    string
}

// FUSEDriver implements Driver using FUSE block device files + loop devices + kernel ext4.
type FUSEDriver struct {
	fuse *fuseblockdev.Server
}

var _ Driver[fuseMount] = (*FUSEDriver)(nil)
var _ DevicePather = (*FUSEDriver)(nil)
var _ VolumeRegistrar = (*FUSEDriver)(nil)

// NewFUSE starts the FUSE block device server and returns a Service.
func NewFUSE(fuseDir string, vm *loophole.VolumeManager, opts *fuseblockdev.Options) (Service, error) {
	fuseblockdev.UnmountStale(fuseDir)
	srv, err := fuseblockdev.Start(fuseDir, opts)
	if err != nil {
		return nil, err
	}
	return New[fuseMount](&FUSEDriver{fuse: srv}, vm), nil
}

func (f *FUSEDriver) Format(ctx context.Context, vol *loophole.Volume) error {
	return ext4.Format(ctx, f.fuse.DevicePath(vol.Name()))
}

func (f *FUSEDriver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) (fuseMount, error) {
	loopDev, err := ext4.Mount(ctx, f.fuse.DevicePath(vol.Name()), mountpoint)
	if err != nil {
		return fuseMount{}, err
	}
	return fuseMount{mountpoint: mountpoint, loopDev: loopDev}, nil
}

func (f *FUSEDriver) Unmount(ctx context.Context, h fuseMount) error {
	if err := ext4.Unmount(ctx, h.mountpoint); err != nil {
		return err
	}
	if h.loopDev != "" {
		ext4.LosetupDetach(ctx, h.loopDev)
	}
	return nil
}

func (f *FUSEDriver) Close(ctx context.Context) error {
	return f.fuse.Unmount()
}

func (f *FUSEDriver) Freeze(ctx context.Context, h fuseMount) error {
	return ext4.Freeze(ctx, h.mountpoint)
}

func (f *FUSEDriver) Thaw(ctx context.Context, h fuseMount) error {
	return ext4.Thaw(ctx, h.mountpoint)
}

func (f *FUSEDriver) RegisterVolume(name string, vol *loophole.Volume) {
	f.fuse.Add(name, vol)
}

func (f *FUSEDriver) UnregisterVolume(name string) {
	f.fuse.Remove(name)
}

func (f *FUSEDriver) DevicePath(volumeName string) string {
	return f.fuse.DevicePath(volumeName)
}

func (f *FUSEDriver) FS(h fuseMount) (FS, error) {
	return newOSFS(h.mountpoint), nil
}
