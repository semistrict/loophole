// Package fsbackend abstracts the filesystem stack that sits on top of
// loophole volumes. The Backend struct owns a VolumeManager and a set of
// pluggable Drivers keyed by volume type, enforces mount tracking, and
// provides the full high-level API (mount, unmount, snapshot, clone).
// The daemon is a thin HTTP wrapper.
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

// AnyDriver is a type-erased driver. Use EraseDriver to create one.
type AnyDriver interface {
	Format(ctx context.Context, vol loophole.Volume) error
	Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (any, error)
	Unmount(ctx context.Context, handle any) error
	Freeze(ctx context.Context, handle any) error
	Thaw(ctx context.Context, handle any) error
	Close(ctx context.Context) error
	FS(handle any) (FS, error)
	Unwrap() any // returns underlying typed driver for optional interface checks
}

// EraseDriver wraps a typed Driver[H] into an AnyDriver.
func EraseDriver[H any](d Driver[H]) AnyDriver {
	return &erasedDriver[H]{d: d}
}

type erasedDriver[H any] struct{ d Driver[H] }

func (e *erasedDriver[H]) Format(ctx context.Context, vol loophole.Volume) error {
	return e.d.Format(ctx, vol)
}

func (e *erasedDriver[H]) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (any, error) {
	return e.d.Mount(ctx, vol, mountpoint)
}

func (e *erasedDriver[H]) Unmount(ctx context.Context, handle any) error {
	return e.d.Unmount(ctx, handle.(H))
}

func (e *erasedDriver[H]) Freeze(ctx context.Context, handle any) error {
	return e.d.Freeze(ctx, handle.(H))
}

func (e *erasedDriver[H]) Thaw(ctx context.Context, handle any) error {
	return e.d.Thaw(ctx, handle.(H))
}

func (e *erasedDriver[H]) Close(ctx context.Context) error {
	return e.d.Close(ctx)
}

func (e *erasedDriver[H]) FS(handle any) (FS, error) {
	return e.d.FS(handle.(H))
}

func (e *erasedDriver[H]) Unwrap() any { return e.d }

// Service is the non-generic interface that Backend implements.
// The daemon and HTTP handlers use this.
type Service interface {
	Create(ctx context.Context, p loophole.CreateParams) error
	Mount(ctx context.Context, volume, mountpoint string) error
	Unmount(ctx context.Context, mountpoint string) error
	Snapshot(ctx context.Context, mountpoint, name string) error
	Clone(ctx context.Context, mountpoint, cloneName, cloneMountpoint string) error
	Freeze(ctx context.Context, mountpoint string) error
	Thaw(ctx context.Context, mountpoint string) error
	FS(mountpoint string) (FS, error)
	// FSForVolume returns an FS for a volume by name, auto-mounting if needed.
	FSForVolume(ctx context.Context, volume string) (FS, error)
	DeviceAttach(ctx context.Context, volume string) (string, error)
	DeviceDetach(ctx context.Context, volume string) error
	DeviceSnapshot(ctx context.Context, volume, snapshot string) error
	DeviceClone(ctx context.Context, volume, clone string) (string, error)
	DevicePath(volume string) (string, error)
	IsMounted(mountpoint string) bool
	IsVolumeMounted(volume string) bool
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
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Truncate(size int64) error
	Sync() error
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

// mountEntry stores the volume, driver-specific handle, and the driver
// that created the mount.
type mountEntry struct {
	vol    loophole.Volume
	handle any
	driver AnyDriver
}

// Backend owns a VolumeManager and a set of AnyDrivers keyed by volume
// type. It provides the complete high-level API: mount, unmount, snapshot,
// clone, and filesystem access. A volume may only be mounted once; mounting
// the same volume at the same mountpoint is idempotent.
type Backend struct {
	vm      loophole.VolumeManager
	drivers map[string]AnyDriver // volType → driver

	mu     sync.Mutex
	mounts map[string]mountEntry // mountpoint → entry
}

// NewBackend creates a Backend with the given drivers keyed by volume type.
func NewBackend(vm loophole.VolumeManager, drivers map[string]AnyDriver) *Backend {
	return &Backend{
		vm:      vm,
		drivers: drivers,
		mounts:  make(map[string]mountEntry),
	}
}

// New creates a Backend with a single driver registered for the given
// volume types. If no volTypes are provided, the driver is not registered
// for any type (useful only for block-device-level operations).
func New(vm loophole.VolumeManager, driver AnyDriver, volTypes ...string) *Backend {
	drivers := make(map[string]AnyDriver, len(volTypes))
	for _, vt := range volTypes {
		drivers[vt] = driver
	}
	return NewBackend(vm, drivers)
}

// driverFor returns the driver for a volume type, or an error if none is registered.
func (b *Backend) driverFor(volType string) (AnyDriver, error) {
	d, ok := b.drivers[volType]
	if !ok {
		return nil, fmt.Errorf("no driver registered for volume type %q", volType)
	}
	return d, nil
}

// VM returns the underlying VolumeManager.
func (b *Backend) VM() loophole.VolumeManager { return b.vm }

// Create creates a new volume and formats it.
// Size is the volume size in bytes; 0 means use the default.
func (b *Backend) Create(ctx context.Context, p loophole.CreateParams) error {
	slog.Info("backend: creating volume", "volume", p.Volume, "size", p.Size, "type", p.Type)
	volType := p.Type
	if volType == "" {
		panic("fsbackend: CreateParams.Type must be set")
	}
	driver, err := b.driverFor(volType)
	if err != nil {
		return err
	}
	vol, err := b.vm.NewVolume(ctx, p.Volume, p.Size, volType)
	if err != nil {
		return err
	}
	if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
		slog.Debug("backend: registering volume", "volume", p.Volume)
		vr.RegisterVolume(p.Volume, vol)
	}
	if !p.NoFormat {
		slog.Debug("backend: formatting volume", "volume", p.Volume)
		if err := driver.Format(ctx, vol); err != nil {
			if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
				vr.UnregisterVolume(p.Volume)
			}
			return fmt.Errorf("format: %w", err)
		}
		if err := vol.Flush(ctx); err != nil {
			return fmt.Errorf("flush after format: %w", err)
		}
	}
	slog.Debug("backend: create done", "volume", p.Volume)
	return nil
}

// Mount opens an existing volume and mounts it at mountpoint.
// The volume must already exist and be formatted.
// Idempotent: if the volume is already mounted at this mountpoint, returns nil.
// Returns an error if the volume is mounted at a different mountpoint.
func (b *Backend) Mount(ctx context.Context, volume string, mountpoint string) error {
	slog.Info("backend: opening volume for mount", "volume", volume, "mountpoint", mountpoint)
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return err
	}
	return b.mountVolume(ctx, vol, mountpoint)
}

// Unmount tears down the filesystem at mountpoint and closes the volume.
func (b *Backend) Unmount(ctx context.Context, mountpoint string) error {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}

	if err := entry.driver.Unmount(ctx, entry.handle); err != nil {
		return err
	}

	b.mu.Lock()
	delete(b.mounts, mountpoint)
	b.mu.Unlock()

	if vr, ok := entry.driver.Unwrap().(VolumeRegistrar); ok {
		vr.UnregisterVolume(entry.vol.Name())
	}

	return entry.vol.ReleaseRef(ctx)
}

// Snapshot freezes the filesystem, takes a snapshot, and thaws.
func (b *Backend) Snapshot(ctx context.Context, mountpoint string, name string) error {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return err
	}

	if err := entry.driver.Freeze(ctx, entry.handle); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	err = entry.vol.Snapshot(ctx, name)
	if thawErr := entry.driver.Thaw(ctx, entry.handle); thawErr != nil {
		slog.Error("thaw failed after snapshot", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	return err
}

// Clone freezes the filesystem, clones the volume, thaws, and mounts the clone.
func (b *Backend) Clone(ctx context.Context, mountpoint string, cloneName string, cloneMountpoint string) error {
	slog.Info("backend: cloning", "mountpoint", mountpoint, "clone", cloneName)
	entry, err := b.entry(mountpoint)
	if err != nil {
		return err
	}

	slog.Debug("backend: freezing for clone", "mountpoint", mountpoint)
	if err := entry.driver.Freeze(ctx, entry.handle); err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	slog.Debug("backend: frozen, cloning volume", "clone", cloneName)
	cloneVol, err := entry.vol.Clone(ctx, cloneName)
	slog.Debug("backend: thawing after clone", "mountpoint", mountpoint, "cloneErr", err)
	if thawErr := entry.driver.Thaw(ctx, entry.handle); thawErr != nil {
		slog.Error("thaw failed after clone", "mountpoint", mountpoint, "error", thawErr)
		if err == nil {
			err = thawErr
		}
	}
	slog.Debug("backend: thawed", "mountpoint", mountpoint)
	if err != nil {
		return err
	}

	slog.Debug("backend: mounting clone", "clone", cloneName, "mountpoint", cloneMountpoint)
	return b.mountVolume(ctx, cloneVol, cloneMountpoint)
}

// Freeze flushes dirty data and quiesces the filesystem.
func (b *Backend) Freeze(ctx context.Context, mountpoint string) error {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return err
	}
	return entry.driver.Freeze(ctx, entry.handle)
}

// Thaw resumes the filesystem after Freeze.
func (b *Backend) Thaw(ctx context.Context, mountpoint string) error {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return err
	}
	return entry.driver.Thaw(ctx, entry.handle)
}

// FS returns a filesystem handle for the given mountpoint.
func (b *Backend) FS(mountpoint string) (FS, error) {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return nil, err
	}
	return entry.driver.FS(entry.handle)
}

// FSForVolume returns an FS for a volume by name, auto-mounting if needed.
func (b *Backend) FSForVolume(ctx context.Context, volume string) (FS, error) {
	b.mu.Lock()
	for _, entry := range b.mounts {
		if entry.vol.Name() == volume {
			b.mu.Unlock()
			return entry.driver.FS(entry.handle)
		}
	}
	b.mu.Unlock()

	slog.Info("auto-mounting volume", "volume", volume)
	if err := b.Mount(ctx, volume, volume); err != nil {
		return nil, fmt.Errorf("auto-mount %q: %w", volume, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, entry := range b.mounts {
		if entry.vol.Name() == volume {
			return entry.driver.FS(entry.handle)
		}
	}
	return nil, fmt.Errorf("volume %q not found after auto-mount", volume)
}

// --- Device-level operations (raw block device access) ---

// DeviceAttach opens an existing volume and returns its block device path.
// No filesystem is mounted — the caller manages the raw device.
func (b *Backend) DeviceAttach(ctx context.Context, volume string) (string, error) {
	vol, err := b.vm.OpenVolume(ctx, volume)
	if err != nil {
		return "", err
	}
	driver, err := b.driverFor(vol.VolumeType())
	if err != nil {
		return "", err
	}
	if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
		vr.RegisterVolume(volume, vol)
	}
	if dc, ok := driver.Unwrap().(DeviceConnector); ok {
		path, err := dc.ConnectDevice(ctx, vol)
		if err != nil {
			if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
				vr.UnregisterVolume(volume)
			}
		}
		return path, err
	}
	return b.DevicePath(volume)
}

// DeviceDetach disconnects the block device and closes a volume opened via DeviceAttach.
func (b *Backend) DeviceDetach(ctx context.Context, volume string) error {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	driver, err := b.driverFor(vol.VolumeType())
	if err != nil {
		return err
	}
	if dc, ok := driver.Unwrap().(DeviceConnector); ok {
		if err := dc.DisconnectDevice(ctx, volume); err != nil {
			return err
		}
	}
	if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
		vr.UnregisterVolume(volume)
	}
	return vol.ReleaseRef(ctx)
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
	driver, err := b.driverFor(vol.VolumeType())
	if err != nil {
		return "", err
	}
	cloneVol, err := vol.Clone(ctx, clone)
	if err != nil {
		return "", err
	}
	if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
		vr.RegisterVolume(clone, cloneVol)
	}
	if dc, ok := driver.Unwrap().(DeviceConnector); ok {
		path, err := dc.ConnectDevice(ctx, cloneVol)
		if err != nil {
			if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
				vr.UnregisterVolume(clone)
			}
		}
		return path, err
	}
	return b.DevicePath(clone)
}

// DevicePath returns the block device path for a volume.
func (b *Backend) DevicePath(volume string) (string, error) {
	// Try all drivers for DevicePather.
	for _, d := range b.drivers {
		if dp, ok := d.Unwrap().(DevicePather); ok {
			dev := dp.DevicePath(volume)
			if dev != "" {
				return dev, nil
			}
		}
	}
	return "", fmt.Errorf("backend does not expose device paths")
}

// --- Query methods ---

// IsMounted reports whether mountpoint is tracked as an active mount.
func (b *Backend) IsMounted(mountpoint string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.mounts[mountpoint]
	return ok
}

// IsVolumeMounted returns true if the named volume is currently mounted.
func (b *Backend) IsVolumeMounted(volume string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, entry := range b.mounts {
		if entry.vol.Name() == volume {
			return true
		}
	}
	return false
}

// VolumeAt returns the volume name mounted at mountpoint, or "" if none.
func (b *Backend) VolumeAt(mountpoint string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.mounts[mountpoint]
	if !ok {
		return ""
	}
	return entry.vol.Name()
}

// Mounts returns a snapshot of all active mountpoint → volume name mappings.
func (b *Backend) Mounts() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[string]string, len(b.mounts))
	for k, v := range b.mounts {
		cp[k] = v.vol.Name()
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
	// Close each unique driver once.
	closed := make(map[AnyDriver]bool)
	for _, d := range b.drivers {
		if closed[d] {
			continue
		}
		closed[d] = true
		if err := d.Close(ctx); err != nil {
			slog.Warn("driver close failed", "error", err)
			if firstErr == nil {
				firstErr = err
			}
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
	for mp, entry := range b.mounts {
		if entry.vol.Name() == volume {
			return mp
		}
	}
	return ""
}

// mountVolume mounts an already-open *Volume. Used by Mount and Clone.
func (b *Backend) mountVolume(ctx context.Context, vol loophole.Volume, mountpoint string) error {
	slog.Debug("backend: mountVolume start", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	if mp := b.mountpointFor(vol.Name()); mp != "" {
		b.mu.Unlock()
		if mp == mountpoint {
			return nil
		}
		return fmt.Errorf("volume %q is already mounted at %s", vol.Name(), mp)
	}
	b.mu.Unlock()

	driver, err := b.driverFor(vol.VolumeType())
	if err != nil {
		return err
	}
	if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
		slog.Debug("backend: registering volume for mount", "volume", vol.Name())
		vr.RegisterVolume(vol.Name(), vol)
	}
	slog.Debug("backend: calling driver.Mount", "volume", vol.Name(), "mountpoint", mountpoint)
	handle, err := driver.Mount(ctx, vol, mountpoint)
	if err != nil {
		slog.Info("backend: driver.Mount failed", "volume", vol.Name(), "error", err)
		if vr, ok := driver.Unwrap().(VolumeRegistrar); ok {
			vr.UnregisterVolume(vol.Name())
		}
		return err
	}

	slog.Debug("backend: mountVolume done", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	b.mounts[mountpoint] = mountEntry{vol: vol, handle: handle, driver: driver}
	b.mu.Unlock()
	return nil
}

// entry returns the mount entry for a mountpoint.
func (b *Backend) entry(mountpoint string) (mountEntry, error) {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()
	if !ok {
		return mountEntry{}, fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}
	return entry, nil
}
