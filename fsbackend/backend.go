// Package fsbackend abstracts the filesystem stack that sits on top of
// loophole volumes. The Backend struct owns a VolumeManager and a pluggable
// Driver, enforces mount tracking, and provides the full high-level API
// (mount, unmount, snapshot, clone). The daemon is a thin HTTP wrapper.
package fsbackend

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"

	"github.com/semistrict/loophole"
)

// Driver is the pluggable interface that backends implement.
// FUSE, NBD, and lwext4 each provide a Driver.
type Driver interface {
	// Format creates a new ext4 filesystem on the volume's block device.
	Format(ctx context.Context, vol *loophole.Volume) error
	Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) error
	Unmount(ctx context.Context, mountpoint string, volumeName string) error
	Freeze(ctx context.Context, mountpoint string) error
	Thaw(ctx context.Context, mountpoint string) error
	Close(ctx context.Context) error

	// FS returns a filesystem handle for the given mountpoint.
	FS(mountpoint string) (FS, error)
}

// FS provides path-based filesystem operations on a mounted volume.
// For kernel backends (FUSE, NBD), operations delegate to os.*.
// For in-process backends (lwext4), operations use the library directly.
type FS interface {
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte, perm fs.FileMode) error
	MkdirAll(name string, perm fs.FileMode) error
	Remove(name string) error
	Stat(name string) (fs.FileInfo, error)
	ReadDir(name string) ([]string, error)
	Open(name string) (File, error)
	Create(name string) (File, error)
}

// File is an open file handle.
type File interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}

// DevicePather is optionally implemented by drivers that expose a block
// device path for each volume (e.g. FUSE loop files, /dev/nbdN).
type DevicePather interface {
	DevicePath(volumeName string) string
}

// DeviceConnector is optionally implemented by drivers that need to
// explicitly connect/disconnect a volume to a block device (e.g. NBD).
// FUSE doesn't need this because the FUSE server exposes all open volumes.
type DeviceConnector interface {
	ConnectDevice(ctx context.Context, vol *loophole.Volume) (string, error)
	DisconnectDevice(ctx context.Context, volumeName string) error
}

// Backend owns a VolumeManager and Driver. It provides the complete
// high-level API: mount, unmount, snapshot, clone, and filesystem access.
// A volume may only be mounted once; mounting the same volume at the same
// mountpoint is idempotent.
type Backend struct {
	vm     *loophole.VolumeManager
	driver Driver

	mu     sync.Mutex
	mounts map[string]string // mountpoint → volume name
}

// New creates a Backend with the given driver and volume manager.
func New(driver Driver, vm *loophole.VolumeManager) *Backend {
	return &Backend{
		vm:     vm,
		driver: driver,
		mounts: make(map[string]string),
	}
}

// VM returns the underlying VolumeManager.
func (b *Backend) VM() *loophole.VolumeManager { return b.vm }

// Create creates a new volume and formats it with ext4.
func (b *Backend) Create(ctx context.Context, volume string) error {
	vol, err := b.vm.NewVolume(ctx, volume)
	if err != nil {
		return err
	}
	if err := b.driver.Format(ctx, vol); err != nil {
		return fmt.Errorf("format: %w", err)
	}
	return nil
}

// Mount opens an existing volume and mounts it at mountpoint.
// The volume must already exist and be formatted with ext4.
// Idempotent: if the volume is already mounted at this mountpoint, returns nil.
// Returns an error if the volume is mounted at a different mountpoint.
func (b *Backend) Mount(ctx context.Context, volume string, mountpoint string) error {
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return err
	}
	return b.mountVolume(ctx, vol, mountpoint)
}

// Unmount tears down the filesystem at mountpoint and closes the volume.
func (b *Backend) Unmount(ctx context.Context, mountpoint string) error {
	b.mu.Lock()
	volName := b.mounts[mountpoint]
	b.mu.Unlock()

	if volName == "" {
		return fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}

	if err := b.driver.Unmount(ctx, mountpoint, volName); err != nil {
		return err
	}

	b.mu.Lock()
	delete(b.mounts, mountpoint)
	b.mu.Unlock()

	vol := b.vm.GetVolume(volName)
	if vol != nil {
		return vol.Close(ctx)
	}
	return nil
}

// Snapshot freezes the filesystem, takes a snapshot, and thaws.
func (b *Backend) Snapshot(ctx context.Context, mountpoint string, name string) error {
	vol, err := b.volumeAt(mountpoint)
	if err != nil {
		return err
	}

	if err := b.driver.Freeze(ctx, mountpoint); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	err = vol.Snapshot(ctx, name)
	if thawErr := b.driver.Thaw(ctx, mountpoint); thawErr != nil {
		slog.Error("thaw failed after snapshot", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	return err
}

// Clone freezes the filesystem, clones the volume, thaws, and mounts the clone.
func (b *Backend) Clone(ctx context.Context, mountpoint string, cloneName string, cloneMountpoint string) error {
	vol, err := b.volumeAt(mountpoint)
	if err != nil {
		return err
	}

	if err := b.driver.Freeze(ctx, mountpoint); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	cloneVol, err := vol.Clone(ctx, cloneName)
	if thawErr := b.driver.Thaw(ctx, mountpoint); thawErr != nil {
		slog.Error("thaw failed after clone", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	if err != nil {
		return err
	}

	return b.mountVolume(ctx, cloneVol, cloneMountpoint)
}

// Freeze flushes dirty data and quiesces the filesystem.
func (b *Backend) Freeze(ctx context.Context, mountpoint string) error {
	return b.driver.Freeze(ctx, mountpoint)
}

// Thaw resumes the filesystem after Freeze.
func (b *Backend) Thaw(ctx context.Context, mountpoint string) error {
	return b.driver.Thaw(ctx, mountpoint)
}

// FS returns a filesystem handle for the given mountpoint.
func (b *Backend) FS(mountpoint string) (FS, error) {
	return b.driver.FS(mountpoint)
}

// --- Device-level operations (raw block device access) ---

// DeviceMount opens an existing volume and returns its block device path.
// No filesystem is mounted — the caller manages the raw device.
func (b *Backend) DeviceMount(ctx context.Context, volume string) (string, error) {
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return "", err
	}
	if dc, ok := b.driver.(DeviceConnector); ok {
		return dc.ConnectDevice(ctx, vol)
	}
	return b.DevicePath(volume)
}

// DeviceUnmount disconnects the block device and closes a volume opened via DeviceMount.
func (b *Backend) DeviceUnmount(ctx context.Context, volume string) error {
	if dc, ok := b.driver.(DeviceConnector); ok {
		if err := dc.DisconnectDevice(ctx, volume); err != nil {
			return err
		}
	}
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	return vol.Close(ctx)
}

// DeviceSnapshot takes a snapshot of a volume by name (no freeze/thaw).
func (b *Backend) DeviceSnapshot(ctx context.Context, volume string, snapshot string) error {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	return vol.Snapshot(ctx, snapshot)
}

// DeviceClone clones a volume and returns the clone's device path.
func (b *Backend) DeviceClone(ctx context.Context, volume string, clone string) (string, error) {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return "", fmt.Errorf("volume %q not open", volume)
	}
	cloneVol, err := vol.Clone(ctx, clone)
	if err != nil {
		return "", err
	}
	if dc, ok := b.driver.(DeviceConnector); ok {
		return dc.ConnectDevice(ctx, cloneVol)
	}
	return b.DevicePath(clone)
}

// DevicePath returns the block device path for a volume.
func (b *Backend) DevicePath(volume string) (string, error) {
	dp, ok := b.driver.(DevicePather)
	if !ok {
		return "", fmt.Errorf("backend does not expose device paths")
	}
	dev := dp.DevicePath(volume)
	if dev == "" {
		return "", fmt.Errorf("no device path for volume %q", volume)
	}
	return dev, nil
}

// --- Query methods ---

// IsMounted reports whether mountpoint is tracked as an active mount.
func (b *Backend) IsMounted(mountpoint string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.mounts[mountpoint]
	return ok
}

// VolumeAt returns the volume name mounted at mountpoint, or "" if none.
func (b *Backend) VolumeAt(mountpoint string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mounts[mountpoint]
}

// Mounts returns a snapshot of all active mountpoint → volume name mappings.
func (b *Backend) Mounts() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[string]string, len(b.mounts))
	for k, v := range b.mounts {
		cp[k] = v
	}
	return cp
}

// Close unmounts all volumes and releases all resources.
func (b *Backend) Close(ctx context.Context) error {
	var firstErr error
	for mountpoint := range b.Mounts() {
		if err := b.Unmount(ctx, mountpoint); err != nil {
			slog.Warn("unmount failed during close", "mountpoint", mountpoint, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := b.driver.Close(ctx); err != nil {
		slog.Warn("driver close failed", "error", err)
		if firstErr == nil {
			firstErr = err
		}
	}
	if err := b.vm.Close(ctx); err != nil {
		slog.Warn("volume manager close failed", "error", err)
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- internal ---

// mountpointFor returns the mountpoint for a volume, or "" if not mounted.
// Caller must hold b.mu.
func (b *Backend) mountpointFor(volume string) string {
	for mp, vol := range b.mounts {
		if vol == volume {
			return mp
		}
	}
	return ""
}

// mountVolume mounts an already-open *Volume. Used by Mount and Clone.
func (b *Backend) mountVolume(ctx context.Context, vol *loophole.Volume, mountpoint string) error {
	b.mu.Lock()
	if mp := b.mountpointFor(vol.Name()); mp != "" {
		b.mu.Unlock()
		if mp == mountpoint {
			return nil
		}
		return fmt.Errorf("volume %q is already mounted at %s", vol.Name(), mp)
	}
	b.mu.Unlock()

	if err := b.driver.Mount(ctx, vol, mountpoint); err != nil {
		return err
	}

	b.mu.Lock()
	b.mounts[mountpoint] = vol.Name()
	b.mu.Unlock()
	return nil
}

func (b *Backend) volumeAt(mountpoint string) (*loophole.Volume, error) {
	volName := b.VolumeAt(mountpoint)
	if volName == "" {
		return nil, fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}
	vol := b.vm.GetVolume(volName)
	if vol == nil {
		return nil, fmt.Errorf("volume %q not open", volName)
	}
	return vol, nil
}
