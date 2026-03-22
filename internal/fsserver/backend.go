package fsserver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/util"
)

// Backend owns a Manager and a single Linux filesystem backend. On Linux that
// is either NBD or FUSE, chosen when the server starts.
type Backend struct {
	vm   *storage.Manager
	fuse *FUSEDriver
	nbd  *NBDDriver

	mu            sync.Mutex
	vol           *storage.Volume
	handle        any
	mountpoint    string
	beforeUnmount []func() // fired LIFO before driver.Unmount
}

// NewBackend creates a Backend with the selected filesystem driver. Both
// drivers may be nil for platforms without filesystem mounting support.
func NewBackend(vm *storage.Manager, fuse *FUSEDriver, nbd *NBDDriver) *Backend {
	return &Backend{
		vm:   vm,
		fuse: fuse,
		nbd:  nbd,
	}
}

func (b *Backend) installRemoteReleaseHook(vol *storage.Volume) {
	vol.SetOnRemoteRelease(func(ctx context.Context) {
		volume := vol.Name()
		if mp := b.MountpointForVolume(volume); mp != "" {
			slog.Info("release: unmounting", "volume", volume, "mountpoint", mp)
			if err := b.Unmount(ctx, mp); err != nil {
				slog.Warn("release: unmount failed", "volume", volume, "error", err)
			}
		}
		if b.vm.GetVolume(volume) != nil {
			slog.Info("release: detaching device", "volume", volume)
			if err := b.DeviceDetach(ctx, volume); err != nil {
				slog.Warn("release: device detach failed", "volume", volume, "error", err)
			}
		}
	})
}

func (b *Backend) SupportsFilesystem() bool {
	return b.fuse != nil || b.nbd != nil
}

// VM returns the underlying Manager.
func (b *Backend) VM() *storage.Manager { return b.vm }

func (b *Backend) Mode() string {
	switch {
	case b.nbd != nil:
		return "nbd"
	case b.fuse != nil:
		return "fuse"
	default:
		return "none"
	}
}

// Create creates a new volume and formats it.
// Size is the volume size in bytes; 0 means use the default.
func (b *Backend) Create(ctx context.Context, p storage.CreateParams) error {
	slog.Info("backend: creating volume", "volume", p.Volume, "size", p.Size, "type", p.Type)
	volType := p.Type
	if volType == "" {
		volType = storage.VolumeTypeExt4
	}
	p.Type = volType

	if p.FromDir != "" || p.FromRaw != "" {
		if p.NoFormat {
			return fmt.Errorf("imported create requires formatting")
		}
		if p.FromDir != "" {
			return b.createExt4FromDir(ctx, p)
		}
		return b.createFromRawImage(p)
	}

	if p.NoFormat {
		// Block-level import (e.g. device dd): just allocate storage, then
		// release the creator's ref so a later owner can attach it.
		vol, err := b.vm.NewVolume(p)
		if err != nil {
			return err
		}
		if err := vol.ReleaseRef(); err != nil {
			return fmt.Errorf("release create ref: %w", err)
		}
		slog.Debug("backend: create done (no format)", "volume", p.Volume)
		return nil
	}

	vol, err := b.vm.NewVolume(p)
	if err != nil {
		return err
	}
	b.registerVolume(p.Volume, vol)
	slog.Debug("backend: formatting volume", "volume", p.Volume)
	if err := b.formatVolume(ctx, vol); err != nil {
		b.unregisterVolume(p.Volume)
		if releaseErr := vol.ReleaseRef(); releaseErr != nil {
			slog.Warn("release create ref after format failure", "volume", p.Volume, "error", releaseErr)
		}
		return fmt.Errorf("format: %w", err)
	}
	if err := vol.Flush(); err != nil {
		b.unregisterVolume(p.Volume)
		if releaseErr := vol.ReleaseRef(); releaseErr != nil {
			slog.Warn("release create ref after flush failure", "volume", p.Volume, "error", releaseErr)
		}
		return fmt.Errorf("flush after format: %w", err)
	}
	b.unregisterVolume(p.Volume)
	if err := vol.ReleaseRef(); err != nil {
		return fmt.Errorf("release create ref: %w", err)
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
	vol, err := b.vm.OpenVolume(volume)
	if err != nil {
		return err
	}
	b.installRemoteReleaseHook(vol)
	return b.mountVolume(ctx, vol, mountpoint)
}

// Unmount tears down the filesystem and closes the volume.
func (b *Backend) Unmount(ctx context.Context, mountpoint string) error {
	b.mu.Lock()
	if b.vol == nil || b.mountpoint != mountpoint {
		b.mu.Unlock()
		return fmt.Errorf("mountpoint %q not tracked", mountpoint)
	}
	vol := b.vol
	b.mu.Unlock()

	b.doUnmount(ctx)
	return vol.ReleaseRef()
}

// Checkpoint freezes the filesystem, creates a checkpoint, and thaws.
func (b *Backend) Checkpoint(ctx context.Context, mountpoint string) (string, error) {
	b.mu.Lock()
	if b.vol == nil {
		b.mu.Unlock()
		return "", fmt.Errorf("no volume mounted")
	}
	vol, handle := b.vol, b.handle
	b.mu.Unlock()

	if err := b.freezeHandle(ctx, handle); err != nil {
		return "", fmt.Errorf("freeze: %w", err)
	}
	cpID, cpErr := vol.Checkpoint()
	if thawErr := b.thawHandle(ctx, handle); thawErr != nil {
		slog.Error("thaw failed after checkpoint", "mountpoint", mountpoint, "error", thawErr)
		if cpErr == nil {
			cpErr = thawErr
		}
	}
	return cpID, cpErr
}

// MountpointForVolume returns the mountpoint for a volume, or "" if not mounted.
func (b *Backend) MountpointForVolume(volume string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.vol != nil && b.vol.Name() == volume {
		return b.mountpoint
	}
	return ""
}

// OnBeforeUnmount registers a callback that fires (LIFO) before the
// filesystem is unmounted.
func (b *Backend) OnBeforeUnmount(mountpoint string, fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.vol != nil && b.mountpoint == mountpoint {
		b.beforeUnmount = append(b.beforeUnmount, fn)
	}
}

// doUnmount tears down the mount: fires beforeUnmount callbacks (LIFO),
// unmounts the FS via the driver, clears tracking, and unregisters the volume.
func (b *Backend) doUnmount(ctx context.Context) {
	b.mu.Lock()
	callbacks := b.beforeUnmount
	mountpoint := b.mountpoint
	handle := b.handle
	volName := b.vol.Name()
	b.beforeUnmount = nil
	b.vol = nil
	b.mountpoint = ""
	b.mu.Unlock()

	for i := len(callbacks) - 1; i >= 0; i-- {
		callbacks[i]()
	}
	if err := b.unmountHandle(ctx, handle); err != nil {
		slog.Warn("unmount failed", "mountpoint", mountpoint, "error", err)
	}
	b.unregisterVolume(volName)
}

// Thaw resumes the filesystem after Freeze.
func (b *Backend) Thaw(ctx context.Context, mountpoint string) error {
	b.mu.Lock()
	if b.vol == nil {
		b.mu.Unlock()
		return fmt.Errorf("no volume mounted")
	}
	handle := b.handle
	b.mu.Unlock()
	return b.thawHandle(ctx, handle)
}

// --- Device-level operations (raw block device access) ---

// DeviceAttach opens an existing volume and returns its block device path.
// No filesystem is mounted — the caller manages the raw device.
// Acquires a ref on the volume; DeviceDetach releases it.
func (b *Backend) DeviceAttach(ctx context.Context, volume string) (string, error) {
	vol, err := b.vm.OpenVolume(volume)
	if err != nil {
		return "", err
	}
	b.installRemoteReleaseHook(vol)
	if err := vol.AcquireRef(); err != nil {
		return "", fmt.Errorf("acquire ref for device attach: %w", err)
	}
	if b.nbd != nil {
		devicePath, err := b.nbd.ConnectDevice(ctx, vol)
		if err != nil {
			util.SafeRun(vol.ReleaseRef, "release ref after NBD attach failure")
			return "", err
		}
		return devicePath, nil
	}
	if b.fuse != nil {
		b.fuse.RegisterVolume(volume, vol)
		return b.DevicePath(volume)
	}
	util.SafeRun(vol.ReleaseRef, "release ref after driver lookup failure")
	return "", fmt.Errorf("filesystem backend is not available")
}

// DeviceDetach disconnects the block device and closes a volume opened via DeviceAttach.
func (b *Backend) DeviceDetach(ctx context.Context, volume string) error {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	if b.nbd != nil {
		if err := b.nbd.DisconnectDevice(ctx, volume); err != nil {
			return err
		}
	} else if b.fuse != nil {
		b.fuse.UnregisterVolume(volume)
	}
	return vol.ReleaseRef()
}

// DeviceCheckpoint creates a checkpoint of a volume by name (no freeze/thaw).
func (b *Backend) DeviceCheckpoint(ctx context.Context, volume string) (string, error) {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return "", fmt.Errorf("volume %q not open", volume)
	}
	return vol.Checkpoint()
}

// DevicePath returns the block device path for a volume.
func (b *Backend) DevicePath(volume string) (string, error) {
	var dev string
	switch {
	case b.nbd != nil:
		dev = b.nbd.DevicePath(volume)
	case b.fuse != nil:
		dev = b.fuse.DevicePath(volume)
	default:
		return "", fmt.Errorf("backend does not expose device paths")
	}
	if dev != "" {
		return dev, nil
	}
	return "", fmt.Errorf("backend does not expose device paths")
}

// --- Query methods ---

// IsMounted reports whether mountpoint is tracked as an active mount.
func (b *Backend) IsMounted(mountpoint string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.vol != nil && b.mountpoint == mountpoint
}

// IsVolumeMounted returns true if the named volume is currently mounted.
func (b *Backend) IsVolumeMounted(volume string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.vol != nil && b.vol.Name() == volume
}

// VolumeAt returns the volume name mounted at mountpoint, or "" if none.
func (b *Backend) VolumeAt(mountpoint string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.vol != nil && b.mountpoint == mountpoint {
		return b.vol.Name()
	}
	return ""
}

// Mounts returns a copy of all active mountpoint → volume name mappings.
func (b *Backend) Mounts() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[string]string, 1)
	if b.vol != nil {
		cp[b.mountpoint] = b.vol.Name()
	}
	return cp
}

// Close shuts down the mounted volume, then closes the driver and
// the volume manager.
//
//  1. fsfreeze (quiesce filesystem so no new writes land)
//  2. flush (persist dirty data to S3)
//  3. unmount (detach the filesystem)
//  4. flush again (catch any writes that landed between freeze and unmount)
//
// Every step is best-effort: failures are logged but never prevent later steps.
func (b *Backend) Close(ctx context.Context) error {
	b.mu.Lock()
	mounted := b.vol != nil
	var mountpoint, volume string
	if mounted {
		mountpoint = b.mountpoint
		volume = b.vol.Name()
	}
	b.mu.Unlock()

	if mounted {
		slog.Info("backend close: closing mount", "mountpoint", mountpoint, "volume", volume)
		b.closeMount(ctx, mountpoint, volume)
		slog.Info("backend close: mount closed", "mountpoint", mountpoint, "volume", volume)
	}

	if b.nbd != nil {
		slog.Info("backend close: closing NBD driver")
		if err := b.nbd.Close(ctx); err != nil {
			slog.Warn("backend close: NBD driver close failed", "error", err)
		}
	}
	if b.fuse != nil {
		slog.Info("backend close: closing driver")
		if err := b.fuse.Close(ctx); err != nil {
			slog.Warn("backend close: driver close failed", "error", err)
		}
	}
	slog.Info("backend close: closing volume manager")
	if err := b.vm.Close(); err != nil {
		slog.Warn("backend close: volume manager close failed", "error", err)
		return err
	}
	slog.Info("backend close: done")
	return nil
}

// closeMount shuts down the single mount. Every step is best-effort.
func (b *Backend) closeMount(ctx context.Context, mountpoint, volume string) {
	b.mu.Lock()
	if b.vol == nil {
		b.mu.Unlock()
		return
	}
	vol := b.vol
	handle := b.handle
	b.mu.Unlock()

	// 1. Quiesce FS (manual — shutdown doesn't use before_freeze hooks).
	slog.Info("closeMount: freezing", "mountpoint", mountpoint, "volume", volume)
	if err := b.freezeHandle(ctx, handle); err != nil {
		slog.Warn("freeze failed during close", "mountpoint", mountpoint, "error", err)
	}

	// 2. Flush to S3.
	slog.Info("closeMount: flushing", "mountpoint", mountpoint, "volume", volume)
	if err := vol.Flush(); err != nil {
		slog.Warn("flush failed during close", "volume", volume, "error", err)
	}

	// 3. Tear down mount (chroot teardown, FS unmount, map cleanup).
	slog.Info("closeMount: unmounting", "mountpoint", mountpoint, "volume", volume)
	b.doUnmount(ctx)

	// 4. Release ref (may trigger volume destruction which flushes again).
	slog.Info("closeMount: releasing ref", "mountpoint", mountpoint, "volume", volume)
	if err := vol.ReleaseRef(); err != nil {
		slog.Warn("release ref failed during close", "volume", volume, "error", err)
	}
}

// --- internal ---

// MountOpen mounts an already-open Volume at the given mountpoint.
func (b *Backend) MountOpen(ctx context.Context, vol *storage.Volume, mountpoint string) error {
	return b.mountVolume(ctx, vol, mountpoint)
}

// mountVolume mounts an already-open *Volume. Used by Mount and MountOpen.
// Acquires a ref on the volume; Unmount releases it.
func (b *Backend) mountVolume(ctx context.Context, vol *storage.Volume, mountpoint string) error {
	slog.Debug("backend: mountVolume start", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	if b.vol != nil && b.vol.Name() == vol.Name() {
		if b.mountpoint == mountpoint {
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()
		return fmt.Errorf("volume %q is already mounted at %s", vol.Name(), b.mountpoint)
	}
	b.mu.Unlock()

	if err := vol.AcquireRef(); err != nil {
		return fmt.Errorf("acquire ref for mount: %w", err)
	}

	b.registerVolume(vol.Name(), vol)
	slog.Debug("backend: calling driver.Mount", "volume", vol.Name(), "mountpoint", mountpoint)
	handle, err := b.mountWithDriver(ctx, vol, mountpoint)
	if err != nil {
		slog.Info("backend: driver.Mount failed", "volume", vol.Name(), "error", err)
		b.unregisterVolume(vol.Name())
		util.SafeRun(vol.ReleaseRef, "release ref after mount failure")
		return err
	}

	slog.Debug("backend: mountVolume done", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	b.vol = vol
	b.handle = handle
	b.mountpoint = mountpoint
	b.mu.Unlock()

	return nil
}

func (b *Backend) registerVolume(name string, vol *storage.Volume) {
	if b.fuse != nil {
		slog.Debug("backend: registering volume", "volume", name, "mode", b.Mode())
		b.fuse.RegisterVolume(name, vol)
	}
}

func (b *Backend) unregisterVolume(name string) {
	if b.fuse != nil {
		b.fuse.UnregisterVolume(name)
	}
}

func (b *Backend) formatVolume(ctx context.Context, vol *storage.Volume) error {
	switch {
	case b.nbd != nil:
		return b.nbd.Format(ctx, vol)
	case b.fuse != nil:
		return b.fuse.Format(ctx, vol)
	default:
		return fmt.Errorf("filesystem backend is not available")
	}
}

func (b *Backend) mountWithDriver(ctx context.Context, vol *storage.Volume, mountpoint string) (any, error) {
	switch {
	case b.nbd != nil:
		return b.nbd.Mount(ctx, vol, mountpoint)
	case b.fuse != nil:
		return b.fuse.Mount(ctx, vol, mountpoint)
	default:
		return nil, fmt.Errorf("filesystem backend is not available")
	}
}

func (b *Backend) unmountHandle(ctx context.Context, handle any) error {
	switch {
	case b.nbd != nil:
		return b.nbd.Unmount(ctx, handle.(nbdMount))
	case b.fuse != nil:
		return b.fuse.Unmount(ctx, handle.(fuseMount))
	default:
		return fmt.Errorf("filesystem backend is not available")
	}
}

func (b *Backend) freezeHandle(ctx context.Context, handle any) error {
	switch {
	case b.nbd != nil:
		return b.nbd.Freeze(ctx, handle.(nbdMount))
	case b.fuse != nil:
		return b.fuse.Freeze(ctx, handle.(fuseMount))
	default:
		return fmt.Errorf("filesystem backend is not available")
	}
}

func (b *Backend) thawHandle(ctx context.Context, handle any) error {
	switch {
	case b.nbd != nil:
		return b.nbd.Thaw(ctx, handle.(nbdMount))
	case b.fuse != nil:
		return b.fuse.Thaw(ctx, handle.(fuseMount))
	default:
		return fmt.Errorf("filesystem backend is not available")
	}
}
