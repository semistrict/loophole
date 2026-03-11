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

// FUSEDriver implements Driver using FUSE block device files + loop devices + kernel FS.
type FUSEDriver struct {
	fuse     *fuseblockdev.Server
	pageSize int
}

var _ Driver[fuseMount] = (*FUSEDriver)(nil)
var _ DevicePather = (*FUSEDriver)(nil)
var _ VolumeRegistrar = (*FUSEDriver)(nil)

// NewFUSEDriver starts the FUSE block device server and returns a type-erased driver.
func NewFUSEDriver(fuseDir string, vm loophole.VolumeManager, opts *fuseblockdev.Options) (AnyDriver, error) {
	fuseblockdev.UnmountStale(fuseDir)
	srv, err := fuseblockdev.Start(fuseDir, opts)
	if err != nil {
		return nil, err
	}
	return EraseDriver[fuseMount](&FUSEDriver{fuse: srv, pageSize: vm.PageSize()}), nil
}

// NewFUSE starts the FUSE block device server and returns a Service.
func NewFUSE(fuseDir string, vm loophole.VolumeManager, opts *fuseblockdev.Options) (Service, error) {
	d, err := NewFUSEDriver(fuseDir, vm, opts)
	if err != nil {
		return nil, err
	}
	return New(vm, d, loophole.VolumeTypeExt4, loophole.VolumeTypeXFS), nil
}

func (f *FUSEDriver) Format(ctx context.Context, vol loophole.Volume) error {
	devPath := f.fuse.DevicePath(vol.Name())
	slog.Debug("fuse: formatting", "volume", vol.Name(), "device", devPath, "pageSize", f.pageSize)
	err := linuxutil.FormatFS(ctx, devPath, vol.VolumeType(), f.pageSize)
	slog.Debug("fuse: format done", "volume", vol.Name(), "error", err)
	return err
}

func (f *FUSEDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (fuseMount, error) {
	devPath := f.fuse.DevicePath(vol.Name())
	slog.Debug("fuse: mounting", "volume", vol.Name(), "device", devPath, "mountpoint", mountpoint)
	loopDev, err := linuxutil.MountFS(ctx, devPath, mountpoint, vol.VolumeType(), linuxutil.LoopAttachOpts{
		OptimalIOSize: f.pageSize,
		ReadOnly:      vol.ReadOnly(),
	})
	if err != nil {
		slog.Debug("fuse: mount failed", "volume", vol.Name(), "error", err)
		return fuseMount{}, err
	}
	slog.Debug("fuse: mount done", "volume", vol.Name(), "loopDev", loopDev)
	return fuseMount{mountpoint: mountpoint, loopDev: loopDev}, nil
}

func (f *FUSEDriver) Unmount(ctx context.Context, h fuseMount) error {
	slog.Debug("fuse: unmounting", "mountpoint", h.mountpoint)
	if err := linuxutil.UnmountLoop(ctx, h.mountpoint); err != nil {
		return err
	}
	if h.loopDev != "" {
		if err := linuxutil.LoopDetachPath(h.loopDev); err != nil {
			slog.Warn("loop detach failed", "device", h.loopDev, "error", err)
		}
	}
	slog.Debug("fuse: unmount done", "mountpoint", h.mountpoint)
	return nil
}

func (f *FUSEDriver) Close(ctx context.Context) error {
	if err := f.fuse.Unmount(); err != nil {
		// fusermount3 can fail if a loop device or other consumer still
		// holds a FUSE file descriptor open. Fall back to lazy unmount
		// so the daemon can exit cleanly.
		slog.Warn("fuse unmount failed, trying lazy unmount", "dir", f.fuse.MountDir, "error", err)
		fuseblockdev.UnmountStale(f.fuse.MountDir)
	}
	return nil
}

func (f *FUSEDriver) Freeze(ctx context.Context, h fuseMount) error {
	slog.Debug("fuse: freezing", "mountpoint", h.mountpoint)
	err := linuxutil.Freeze(h.mountpoint)
	slog.Debug("fuse: freeze done", "mountpoint", h.mountpoint, "error", err)
	return err
}

func (f *FUSEDriver) Thaw(ctx context.Context, h fuseMount) error {
	slog.Debug("fuse: thawing", "mountpoint", h.mountpoint)
	err := linuxutil.Thaw(h.mountpoint)
	slog.Debug("fuse: thaw done", "mountpoint", h.mountpoint, "error", err)
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
