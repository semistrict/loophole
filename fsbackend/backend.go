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
	"github.com/semistrict/loophole/client"
)

// Driver is the pluggable interface that backends implement.
// H is the per-mount handle type returned by Mount. The Backend stores
// handles and passes them back to Unmount/Freeze/Thaw/FS — drivers need
// no internal mount tracking.
type Driver[H any] interface {
	// Format creates a new ext4 filesystem on the volume's block device.
	Format(ctx context.Context, vol loophole.Volume) error
	Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (H, error)
	Unmount(ctx context.Context, handle H) error
	Freeze(ctx context.Context, handle H) error
	Thaw(ctx context.Context, handle H) error
	Close(ctx context.Context) error

	// FS returns a filesystem handle for the given mount.
	FS(handle H) (FS, error)
}

// Service is the non-generic interface that Backend[H] implements.
// The daemon and HTTP handlers use this.
type Service interface {
	Create(ctx context.Context, p client.CreateParams) error
	Mount(ctx context.Context, volume, mountpoint string) error
	Unmount(ctx context.Context, mountpoint string) error
	Snapshot(ctx context.Context, mountpoint, name string) error
	Clone(ctx context.Context, mountpoint, cloneName, cloneMountpoint string) error
	Freeze(ctx context.Context, mountpoint string) error
	Thaw(ctx context.Context, mountpoint string) error
	FS(mountpoint string) (FS, error)
	// FSForVolume returns an FS for a volume by name. The volume must
	// already be mounted.
	FSForVolume(volume string) (FS, error)
	DeviceMount(ctx context.Context, volume string) (string, error)
	DeviceUnmount(ctx context.Context, volume string) error
	DeviceSnapshot(ctx context.Context, volume, snapshot string) error
	DeviceClone(ctx context.Context, volume, clone string) (string, error)
	DevicePath(volume string) (string, error)
	IsMounted(mountpoint string) bool
	VolumeAt(mountpoint string) string
	Mounts() map[string]string
	VM() loophole.VolumeManager
	Close(ctx context.Context) error
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
	Lstat(name string) (fs.FileInfo, error)
	ReadDir(name string) ([]string, error)
	Open(name string) (File, error)
	Create(name string) (File, error)
	Symlink(target, name string) error
	Readlink(name string) (string, error)
	Chmod(name string, mode fs.FileMode) error
	Lchown(name string, uid, gid int) error
	Chtimes(name string, mtime int64) error
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
type DeviceConnector interface {
	ConnectDevice(ctx context.Context, vol loophole.Volume) (string, error)
	DisconnectDevice(ctx context.Context, volumeName string) error
}

// VolumeRegistrar is optionally implemented by drivers that need volumes
// explicitly registered before their device files become accessible
// (e.g. FUSE block device server).
type VolumeRegistrar interface {
	RegisterVolume(name string, vol loophole.Volume)
	UnregisterVolume(name string)
}

// mountEntry stores the volume name and driver-specific handle for a mount.
type mountEntry[H any] struct {
	volume string // XXX: I would have expected to see a *Volume here
	handle H
}

// Backend owns a VolumeManager and Driver. It provides the complete
// high-level API: mount, unmount, snapshot, clone, and filesystem access.
// A volume may only be mounted once; mounting the same volume at the same
// mountpoint is idempotent.
type Backend[H any] struct {
	vm     loophole.VolumeManager
	driver Driver[H]
	vr     VolumeRegistrar // nil if driver doesn't implement it

	mu     sync.Mutex
	mounts map[string]mountEntry[H] // mountpoint → entry
}

// New creates a Backend with the given driver and volume manager.
func New[H any](driver Driver[H], vm loophole.VolumeManager) *Backend[H] {
	vr, _ := any(driver).(VolumeRegistrar)
	return &Backend[H]{
		vm:     vm,
		driver: driver,
		vr:     vr,
		mounts: make(map[string]mountEntry[H]),
	}
}

// VM returns the underlying VolumeManager.
func (b *Backend[H]) VM() loophole.VolumeManager { return b.vm }

// Create creates a new volume and formats it with ext4.
// Size is the volume size in bytes; 0 means use the default.
func (b *Backend[H]) Create(ctx context.Context, p client.CreateParams) error {
	slog.Info("backend: creating volume", "volume", p.Volume, "size", p.Size)
	vol, err := b.vm.NewVolume(ctx, p.Volume, p.Size)
	if err != nil {
		return err
	}
	if b.vr != nil {
		slog.Info("backend: registering volume", "volume", p.Volume)
		b.vr.RegisterVolume(p.Volume, vol)
	}
	if !p.NoFormat {
		slog.Info("backend: formatting volume", "volume", p.Volume)
		if err := b.driver.Format(ctx, vol); err != nil {
			if b.vr != nil {
				b.vr.UnregisterVolume(p.Volume)
			}
			return fmt.Errorf("format: %w", err)
		}
	}
	slog.Info("backend: create done", "volume", p.Volume)
	return nil
}

// Mount opens an existing volume and mounts it at mountpoint.
// The volume must already exist and be formatted with ext4.
// Idempotent: if the volume is already mounted at this mountpoint, returns nil.
// Returns an error if the volume is mounted at a different mountpoint.
func (b *Backend[H]) Mount(ctx context.Context, volume string, mountpoint string) error {
	slog.Info("backend: opening volume for mount", "volume", volume, "mountpoint", mountpoint)
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return err
	}
	return b.mountVolume(ctx, vol, mountpoint)
}

// Unmount tears down the filesystem at mountpoint and closes the volume.
func (b *Backend[H]) Unmount(ctx context.Context, mountpoint string) error {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}

	if err := b.driver.Unmount(ctx, entry.handle); err != nil {
		return err
	}

	b.mu.Lock()
	delete(b.mounts, mountpoint)
	b.mu.Unlock()

	if b.vr != nil {
		b.vr.UnregisterVolume(entry.volume)
	}

	vol := b.vm.GetVolume(entry.volume)
	if vol != nil {
		return vol.ReleaseRef(ctx)
	}
	return nil
}

// Snapshot freezes the filesystem, takes a snapshot, and thaws.
func (b *Backend[H]) Snapshot(ctx context.Context, mountpoint string, name string) error {
	vol, handle, err := b.volumeAndHandle(mountpoint)
	if err != nil {
		return err
	}

	if err := b.driver.Freeze(ctx, handle); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	err = vol.Snapshot(ctx, name)
	if thawErr := b.driver.Thaw(ctx, handle); thawErr != nil {
		slog.Error("thaw failed after snapshot", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	return err
}

// Clone freezes the filesystem, clones the volume, thaws, and mounts the clone.
func (b *Backend[H]) Clone(ctx context.Context, mountpoint string, cloneName string, cloneMountpoint string) error {
	slog.Info("backend: clone start", "mountpoint", mountpoint, "clone", cloneName)
	vol, handle, err := b.volumeAndHandle(mountpoint)
	if err != nil {
		return err
	}

	slog.Info("backend: freezing for clone", "mountpoint", mountpoint)
	if err := b.driver.Freeze(ctx, handle); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	slog.Info("backend: frozen, cloning volume", "clone", cloneName)
	cloneVol, err := vol.Clone(ctx, cloneName)
	slog.Info("backend: thawing after clone", "mountpoint", mountpoint, "cloneErr", err)
	if thawErr := b.driver.Thaw(ctx, handle); thawErr != nil {
		slog.Error("thaw failed after clone", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	slog.Info("backend: thawed", "mountpoint", mountpoint)
	if err != nil {
		return err
	}

	slog.Info("backend: mounting clone", "clone", cloneName, "mountpoint", cloneMountpoint)
	return b.mountVolume(ctx, cloneVol, cloneMountpoint)
}

// Freeze flushes dirty data and quiesces the filesystem.
func (b *Backend[H]) Freeze(ctx context.Context, mountpoint string) error {
	_, handle, err := b.volumeAndHandle(mountpoint)
	if err != nil {
		return err
	}
	return b.driver.Freeze(ctx, handle)
}

// Thaw resumes the filesystem after Freeze.
func (b *Backend[H]) Thaw(ctx context.Context, mountpoint string) error {
	_, handle, err := b.volumeAndHandle(mountpoint)
	if err != nil {
		return err
	}
	return b.driver.Thaw(ctx, handle)
}

// FS returns a filesystem handle for the given mountpoint.
func (b *Backend[H]) FS(mountpoint string) (FS, error) {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}
	return b.driver.FS(entry.handle)
}

// FSForVolume returns an FS for a volume by name. The volume must already
// be mounted.
func (b *Backend[H]) FSForVolume(volume string) (FS, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, entry := range b.mounts {
		if entry.volume == volume {
			return b.driver.FS(entry.handle)
		}
	}
	return nil, fmt.Errorf("volume %q is not mounted", volume)
}

// --- Device-level operations (raw block device access) ---

// DeviceMount opens an existing volume and returns its block device path.
// No filesystem is mounted — the caller manages the raw device.
func (b *Backend[H]) DeviceMount(ctx context.Context, volume string) (string, error) {
	// XXX: I don't like calling this "mount", we should consider renaming this operation to Connect
	// but it needs to be done consistently like in Volume
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return "", err
	}
	if b.vr != nil {
		b.vr.RegisterVolume(volume, vol)
	}
	if dc, ok := any(b.driver).(DeviceConnector); ok {
		path, err := dc.ConnectDevice(ctx, vol)
		if err != nil && b.vr != nil {
			b.vr.UnregisterVolume(volume)
		}
		return path, err
	}
	return b.DevicePath(volume)
}

// DeviceUnmount disconnects the block device and closes a volume opened via DeviceMount.
func (b *Backend[H]) DeviceUnmount(ctx context.Context, volume string) error {
	if dc, ok := any(b.driver).(DeviceConnector); ok {
		if err := dc.DisconnectDevice(ctx, volume); err != nil {
			return err
		}
	}
	if b.vr != nil {
		b.vr.UnregisterVolume(volume)
	}
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	return vol.ReleaseRef(ctx)
}

// DeviceSnapshot takes a snapshot of a volume by name (no freeze/thaw).
func (b *Backend[H]) DeviceSnapshot(ctx context.Context, volume string, snapshot string) error {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	return vol.Snapshot(ctx, snapshot)
}

// DeviceClone clones a volume and returns the clone's device path.
func (b *Backend[H]) DeviceClone(ctx context.Context, volume string, clone string) (string, error) {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return "", fmt.Errorf("volume %q not open", volume)
	}
	cloneVol, err := vol.Clone(ctx, clone)
	if err != nil {
		return "", err
	}
	if b.vr != nil {
		b.vr.RegisterVolume(clone, cloneVol)
	}
	if dc, ok := any(b.driver).(DeviceConnector); ok {
		path, err := dc.ConnectDevice(ctx, cloneVol)
		if err != nil && b.vr != nil {
			b.vr.UnregisterVolume(clone)
		}
		return path, err
	}
	return b.DevicePath(clone)
}

// DevicePath returns the block device path for a volume.
func (b *Backend[H]) DevicePath(volume string) (string, error) {
	dp, ok := any(b.driver).(DevicePather)
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
func (b *Backend[H]) IsMounted(mountpoint string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.mounts[mountpoint]
	return ok
}

// VolumeAt returns the volume name mounted at mountpoint, or "" if none.
func (b *Backend[H]) VolumeAt(mountpoint string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mounts[mountpoint].volume
}

// Mounts returns a snapshot of all active mountpoint → volume name mappings.
func (b *Backend[H]) Mounts() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[string]string, len(b.mounts))
	for k, v := range b.mounts {
		cp[k] = v.volume
	}
	return cp
}

// Close unmounts all volumes and releases all resources.
func (b *Backend[H]) Close(ctx context.Context) error {
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
func (b *Backend[H]) mountpointFor(volume string) string {
	for mp, entry := range b.mounts {
		if entry.volume == volume {
			return mp
		}
	}
	return ""
}

// mountVolume mounts an already-open *Volume. Used by Mount and Clone.
func (b *Backend[H]) mountVolume(ctx context.Context, vol loophole.Volume, mountpoint string) error {
	slog.Info("backend: mountVolume start", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	if mp := b.mountpointFor(vol.Name()); mp != "" {
		b.mu.Unlock()
		if mp == mountpoint {
			return nil
		}
		return fmt.Errorf("volume %q is already mounted at %s", vol.Name(), mp)
	}
	b.mu.Unlock()

	if b.vr != nil {
		slog.Info("backend: registering volume for mount", "volume", vol.Name())
		b.vr.RegisterVolume(vol.Name(), vol)
	}
	slog.Info("backend: calling driver.Mount", "volume", vol.Name(), "mountpoint", mountpoint)
	handle, err := b.driver.Mount(ctx, vol, mountpoint)
	if err != nil {
		slog.Info("backend: driver.Mount failed", "volume", vol.Name(), "error", err)
		if b.vr != nil {
			b.vr.UnregisterVolume(vol.Name())
		}
		return err
	}

	slog.Info("backend: mountVolume done", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	b.mounts[mountpoint] = mountEntry[H]{volume: vol.Name(), handle: handle}
	b.mu.Unlock()
	return nil
}

// volumeAndHandle returns the volume and mount handle for a mountpoint.
func (b *Backend[H]) volumeAndHandle(mountpoint string) (loophole.Volume, H, error) {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()
	if !ok {
		var zero H
		return nil, zero, fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}
	vol := b.vm.GetVolume(entry.volume)
	if vol == nil {
		var zero H
		return nil, zero, fmt.Errorf("volume %q not open", entry.volume)
	}
	return vol, entry.handle, nil
}
