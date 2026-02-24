//go:build linux

package fsbackend

import (
	"context"
	"sync"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/ext4"
	"github.com/semistrict/loophole/fuseblockdev"
)

// FUSEDriver implements Driver using FUSE block device files + loop devices + kernel ext4.
type FUSEDriver struct {
	fuse *fuseblockdev.Server

	mu       sync.Mutex
	loopDevs map[string]string // mountpoint → /dev/loopN
}

var _ Driver = (*FUSEDriver)(nil)
var _ DevicePather = (*FUSEDriver)(nil)

// NewFUSE starts the FUSE block device server and returns a Backend.
func NewFUSE(fuseDir string, vm *loophole.VolumeManager, opts *fuseblockdev.Options) (*Backend, error) {
	fuseblockdev.UnmountStale(fuseDir)
	srv, err := fuseblockdev.Start(fuseDir, vm, opts)
	if err != nil {
		return nil, err
	}
	return New(&FUSEDriver{fuse: srv, loopDevs: make(map[string]string)}, vm), nil
}

func (f *FUSEDriver) Format(ctx context.Context, vol *loophole.Volume) error {
	return ext4.Format(ctx, f.fuse.DevicePath(vol.Name()))
}

func (f *FUSEDriver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) error {
	loopDev, err := ext4.Mount(ctx, f.fuse.DevicePath(vol.Name()), mountpoint)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.loopDevs[mountpoint] = loopDev
	f.mu.Unlock()
	return nil
}

func (f *FUSEDriver) Unmount(ctx context.Context, mountpoint string, _ string) error {
	f.mu.Lock()
	loopDev := f.loopDevs[mountpoint]
	delete(f.loopDevs, mountpoint)
	f.mu.Unlock()

	if err := ext4.Unmount(ctx, mountpoint); err != nil {
		return err
	}
	// Explicitly detach the loop device in case ext4.Unmount's findmnt-based
	// detection failed to find it.
	if loopDev != "" {
		ext4.LosetupDetach(ctx, loopDev)
	}
	return nil
}

func (f *FUSEDriver) Close(ctx context.Context) error {
	// Detach any remaining loop devices before tearing down FUSE.
	f.mu.Lock()
	remaining := make(map[string]string, len(f.loopDevs))
	for mp, dev := range f.loopDevs {
		remaining[mp] = dev
	}
	f.mu.Unlock()

	for _, loopDev := range remaining {
		ext4.LosetupDetach(ctx, loopDev)
	}

	return f.fuse.Unmount()
}

func (f *FUSEDriver) Freeze(ctx context.Context, mountpoint string) error {
	return ext4.Freeze(ctx, mountpoint)
}

func (f *FUSEDriver) Thaw(ctx context.Context, mountpoint string) error {
	return ext4.Thaw(ctx, mountpoint)
}

func (f *FUSEDriver) DevicePath(volumeName string) string {
	return f.fuse.DevicePath(volumeName)
}

func (f *FUSEDriver) FS(mountpoint string) (FS, error) {
	return newOSFS(mountpoint), nil
}
