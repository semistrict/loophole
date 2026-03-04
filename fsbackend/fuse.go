//go:build linux

package fsbackend

import (
	"context"
	"log/slog"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/linuxutil"
)

// fuseMount is the per-mount handle for FUSEDriver.
type fuseMount struct {
	mountpoint string
	loopDev    string
}

// FUSEDriver implements Driver using FUSE block device files + loop devices + kernel ext4.
type FUSEDriver struct {
	fuse     *fuseblockdev.Server
	pageSize int
}

var _ Driver[fuseMount] = (*FUSEDriver)(nil)
var _ DevicePather = (*FUSEDriver)(nil)
var _ VolumeRegistrar = (*FUSEDriver)(nil)

// NewFUSE starts the FUSE block device server and returns a Service.
func NewFUSE(fuseDir string, vm loophole.VolumeManager, opts *fuseblockdev.Options) (Service, error) {
	fuseblockdev.UnmountStale(fuseDir)
	srv, err := fuseblockdev.Start(fuseDir, opts)
	if err != nil {
		return nil, err
	}
	return New[fuseMount](&FUSEDriver{fuse: srv, pageSize: vm.PageSize()}, vm), nil
}

func (f *FUSEDriver) Format(ctx context.Context, vol loophole.Volume) error {
	devPath := f.fuse.DevicePath(vol.Name())
	slog.Info("fuse: formatting", "volume", vol.Name(), "device", devPath, "pageSize", f.pageSize)
	err := linuxutil.Ext4Format(ctx, devPath, f.pageSize)
	slog.Info("fuse: format done", "volume", vol.Name(), "error", err)
	return err
}

func (f *FUSEDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (fuseMount, error) {
	devPath := f.fuse.DevicePath(vol.Name())
	slog.Info("fuse: mounting", "volume", vol.Name(), "device", devPath, "mountpoint", mountpoint)
	loopDev, err := linuxutil.Ext4Mount(ctx, devPath, mountpoint, f.pageSize)
	if err != nil {
		slog.Info("fuse: mount failed", "volume", vol.Name(), "error", err)
		return fuseMount{}, err
	}
	slog.Info("fuse: mount done", "volume", vol.Name(), "loopDev", loopDev)
	return fuseMount{mountpoint: mountpoint, loopDev: loopDev}, nil
}

func (f *FUSEDriver) Unmount(ctx context.Context, h fuseMount) error {
	slog.Info("fuse: unmounting", "mountpoint", h.mountpoint)
	if err := linuxutil.Ext4Unmount(ctx, h.mountpoint); err != nil {
		return err
	}
	if h.loopDev != "" {
		if err := linuxutil.LoopDetachPath(h.loopDev); err != nil {
			slog.Warn("loop detach failed", "device", h.loopDev, "error", err)
		}
	}
	slog.Info("fuse: unmount done", "mountpoint", h.mountpoint)
	return nil
}

func (f *FUSEDriver) Close(ctx context.Context) error {
	return f.fuse.Unmount()
}

func (f *FUSEDriver) Freeze(ctx context.Context, h fuseMount) error {
	slog.Info("fuse: freezing", "mountpoint", h.mountpoint)
	err := linuxutil.Freeze(h.mountpoint)
	slog.Info("fuse: freeze done", "mountpoint", h.mountpoint, "error", err)
	return err
}

func (f *FUSEDriver) Thaw(ctx context.Context, h fuseMount) error {
	slog.Info("fuse: thawing", "mountpoint", h.mountpoint)
	err := linuxutil.Thaw(h.mountpoint)
	slog.Info("fuse: thaw done", "mountpoint", h.mountpoint, "error", err)
	return err
}

func (f *FUSEDriver) RegisterVolume(name string, vol loophole.Volume) {
	f.fuse.Add(name, vol)
}

func (f *FUSEDriver) UnregisterVolume(name string) {
	f.fuse.Remove(name)
}

func (f *FUSEDriver) DevicePath(volumeName string) string {
	return f.fuse.DevicePath(volumeName)
}

func (f *FUSEDriver) FS(h fuseMount) (FS, error) {
	return NewOSFS(h.mountpoint), nil
}
