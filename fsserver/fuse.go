//go:build linux

package fsserver

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	"github.com/semistrict/loophole/fuseblockdev"
	"github.com/semistrict/loophole/storage"
)

// fuseMount is the per-mount handle for FUSEDriver.
type fuseMount struct {
	mountpoint string
	loopDev    string
}

// FUSEDriver implements Driver using FUSE block device files + loop devices + kernel FS.
type FUSEDriver struct {
	baseDir  string
	opts     *fuseblockdev.Options
	pageSize int

	mu            sync.Mutex
	fuse          *fuseblockdev.Server
	managedVolume string
	mountDir      string
}

// NewFUSEDriver starts the FUSE block device server.
func NewFUSEDriver(fuseDir string, vm *storage.Manager, opts *fuseblockdev.Options) (*FUSEDriver, error) {
	return &FUSEDriver{baseDir: fuseDir, opts: opts, pageSize: vm.PageSize()}, nil
}

func (f *FUSEDriver) Format(ctx context.Context, vol *storage.Volume) error {
	if err := f.ensureServer(vol.Name(), vol); err != nil {
		return err
	}
	devPath := f.devicePathLocked()
	slog.Debug("fuse: formatting", "volume", vol.Name(), "device", devPath, "pageSize", f.pageSize)
	err := formatFS(ctx, devPath, f.pageSize)
	slog.Debug("fuse: format done", "volume", vol.Name(), "error", err)
	return err
}

func (f *FUSEDriver) Mount(ctx context.Context, vol *storage.Volume, mountpoint string) (fuseMount, error) {
	if err := f.ensureServer(vol.Name(), vol); err != nil {
		return fuseMount{}, err
	}
	devPath := f.devicePathLocked()
	slog.Debug("fuse: mounting", "volume", vol.Name(), "device", devPath, "mountpoint", mountpoint)
	loopDev, err := mountFS(ctx, devPath, mountpoint, loopAttachOpts{
		OptimalIOSize: f.pageSize,
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
	if err := unmountLoop(ctx, h.mountpoint); err != nil {
		return err
	}
	slog.Debug("fuse: unmount done", "mountpoint", h.mountpoint)
	return nil
}

func (f *FUSEDriver) Close(ctx context.Context) error {
	f.mu.Lock()
	srv := f.fuse
	f.mu.Unlock()
	if srv == nil {
		return nil
	}
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
	err := freezeFS(h.mountpoint)
	slog.Debug("fuse: freeze done", "mountpoint", h.mountpoint, "error", err)
	return err
}

func (f *FUSEDriver) Thaw(ctx context.Context, h fuseMount) error {
	slog.Debug("fuse: thawing", "mountpoint", h.mountpoint)
	err := thawFS(h.mountpoint)
	slog.Debug("fuse: thaw done", "mountpoint", h.mountpoint, "error", err)
	return err
}

func (f *FUSEDriver) RegisterVolume(name string, vol *storage.Volume) {
	if err := f.ensureServer(name, vol); err != nil {
		panic(err)
	}
}

func (f *FUSEDriver) UnregisterVolume(name string) {
	f.mu.Lock()
	if f.fuse == nil || f.managedVolume != name {
		f.mu.Unlock()
		return
	}
	srv := f.fuse
	mountDir := f.mountDir
	f.fuse = nil
	f.managedVolume = ""
	f.mountDir = ""
	f.mu.Unlock()

	srv.Remove("file")
	if err := srv.Unmount(); err != nil {
		slog.Warn("fuse unmount failed during unregister, trying lazy unmount", "dir", mountDir, "error", err)
		fuseblockdev.UnmountStale(mountDir)
	}
}

func (f *FUSEDriver) DevicePath(volumeName string) string {
	if err := f.ensureServer(volumeName, nil); err != nil {
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.devicePathLocked()
}

func (f *FUSEDriver) ensureServer(volume string, vol *storage.Volume) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fuse != nil {
		if f.managedVolume != volume {
			return &volumeScopedFUSEError{current: f.managedVolume, requested: volume}
		}
		if vol != nil {
			f.fuse.Add("file", vol)
		}
		return nil
	}

	mountDir := filepath.Join(f.baseDir, safeVolumeDir(volume))
	fuseblockdev.UnmountStale(mountDir)
	srv, err := fuseblockdev.Start(mountDir, f.opts)
	if err != nil {
		return err
	}
	f.fuse = srv
	f.managedVolume = volume
	f.mountDir = mountDir
	if vol != nil {
		f.fuse.Add("file", vol)
	}
	return nil
}

func (f *FUSEDriver) devicePathLocked() string {
	return filepath.Join(f.mountDir, "file")
}

type volumeScopedFUSEError struct {
	current   string
	requested string
}

func (e *volumeScopedFUSEError) Error() string {
	return "fuse driver already bound to volume " + e.current + " (requested " + e.requested + ")"
}

func safeVolumeDir(name string) string {
	name = strings.ReplaceAll(name, "%", "%25")
	name = strings.ReplaceAll(name, "/", "%2F")
	return name
}
