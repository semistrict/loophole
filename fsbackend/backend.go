// Package fsbackend abstracts the filesystem stack that sits on top of
// loophole volumes. The remaining implementation is a single FUSE-backed
// kernel ext4 path with mount tracking and volume lifecycle helpers.
package fsbackend

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

// mountEntry stores the volume, driver-specific handle, and the driver
// that created the mount.
type mountEntry struct {
	vol           loophole.Volume
	handle        fuseMount
	driver        *FUSEDriver
	beforeUnmount []func() // fired LIFO before driver.Unmount
}

// Backend owns a VolumeManager and the single surviving FUSE driver.
// It provides the complete high-level API: mount, unmount, clone,
// and filesystem access.
type Backend struct {
	vm     loophole.VolumeManager
	driver *FUSEDriver

	mu     sync.Mutex
	mounts map[string]mountEntry // mountpoint → entry
}

// NewBackend creates a Backend with the remaining FUSE driver. The driver may
// be nil for embedded/raw-volume use cases that never mount filesystems.
func NewBackend(vm loophole.VolumeManager, driver *FUSEDriver) *Backend {
	return &Backend{
		vm:     vm,
		driver: driver,
		mounts: make(map[string]mountEntry),
	}
}

// driverFor returns the single surviving driver.
func (b *Backend) driverFor() (*FUSEDriver, error) {
	if b.driver == nil {
		return nil, fmt.Errorf("filesystem backend is not available")
	}
	return b.driver, nil
}

// VM returns the underlying VolumeManager.
func (b *Backend) VM() loophole.VolumeManager { return b.vm }

// Create creates a new volume and formats it.
// Size is the volume size in bytes; 0 means use the default.
func (b *Backend) Create(ctx context.Context, p loophole.CreateParams) error {
	slog.Info("backend: creating volume", "volume", p.Volume, "size", p.Size, "type", p.Type)
	volType := p.Type
	if volType == "" {
		volType = loophole.VolumeTypeExt4
	}
	p.Type = volType

	if p.NoFormat {
		// Block-level import (e.g. device dd): just allocate storage, no driver needed.
		if _, err := b.vm.NewVolume(p); err != nil {
			return err
		}
		slog.Debug("backend: create done (no format)", "volume", p.Volume)
		return nil
	}

	driver, err := b.driverFor()
	if err != nil {
		return err
	}
	vol, err := b.vm.NewVolume(p)
	if err != nil {
		return err
	}
	slog.Debug("backend: registering volume", "volume", p.Volume)
	driver.RegisterVolume(p.Volume, vol)
	slog.Debug("backend: formatting volume", "volume", p.Volume)
	if err := driver.Format(ctx, vol); err != nil {
		driver.UnregisterVolume(p.Volume)
		return fmt.Errorf("format: %w", err)
	}
	if err := vol.Flush(); err != nil {
		return fmt.Errorf("flush after format: %w", err)
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

	b.doUnmount(ctx, mountpoint, entry)
	return entry.vol.ReleaseRef()
}

// Checkpoint freezes the filesystem, creates a checkpoint, and thaws.
func (b *Backend) Checkpoint(ctx context.Context, mountpoint string) (string, error) {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return "", err
	}

	if err := entry.driver.Freeze(ctx, entry.handle); err != nil {
		return "", fmt.Errorf("freeze: %w", err)
	}
	cpID, cpErr := entry.vol.Checkpoint()
	if thawErr := entry.driver.Thaw(ctx, entry.handle); thawErr != nil {
		slog.Error("thaw failed after checkpoint", "mountpoint", mountpoint, "error", thawErr)
		if cpErr == nil {
			cpErr = thawErr
		}
	}
	return cpID, cpErr
}

// CloneFromCheckpoint creates an unmounted clone from a volume checkpoint.
func (b *Backend) CloneFromCheckpoint(ctx context.Context, volume, checkpointID, cloneName string) error {
	return b.vm.CloneFromCheckpoint(ctx, volume, checkpointID, cloneName)
}

// Clone freezes the filesystem, creates the clone, and thaws without mounting it.
func (b *Backend) Clone(ctx context.Context, mountpoint string, cloneName string) error {
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
	err = entry.vol.Clone(cloneName)
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

	return nil
}

// FreezeVolume permanently freezes a volume, making it immutable.
// vol.Freeze() fires before_freeze hooks (which flush the FS cache),
// then persists the volume. If mounted, the mount is explicitly torn
// down afterward (chroot teardown, FS unmount, volume ref release).
func (b *Backend) FreezeVolume(ctx context.Context, volume string, compact bool) error {
	vol, err := b.vm.OpenVolume(volume)
	if err != nil {
		return fmt.Errorf("open volume: %w", err)
	}
	if vol.ReadOnly() {
		return fmt.Errorf("volume %q is already frozen", volume)
	}

	slog.Info("freeze: persisting volume", "volume", volume, "compact", compact)
	if compact {
		type freezeCompactor interface {
			FreezeWithCompact() error
		}
		fc, ok := vol.(freezeCompactor)
		if !ok {
			return fmt.Errorf("volume %q does not support compact-freeze", volume)
		}
		if err := fc.FreezeWithCompact(); err != nil {
			return err
		}
	} else {
		if err := vol.Freeze(); err != nil {
			return err
		}
	}

	// Explicitly tear down mount if mounted.
	if mp := b.MountpointForVolume(volume); mp != "" {
		b.mu.Lock()
		entry, ok := b.mounts[mp]
		b.mu.Unlock()
		if ok {
			b.doUnmount(ctx, mp, entry)
			if err := entry.vol.ReleaseRef(); err != nil {
				return err
			}
			return b.vm.CloseVolume(volume)
		}
	}
	return b.vm.CloseVolume(volume)
}

// MountpointForVolume returns the mountpoint for a volume, or "" if not mounted.
func (b *Backend) MountpointForVolume(volume string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	for mp, entry := range b.mounts {
		if entry.vol.Name() == volume {
			return mp
		}
	}
	return ""
}

// OnBeforeUnmount registers a callback that fires (LIFO) before the
// filesystem at mountpoint is unmounted.
func (b *Backend) OnBeforeUnmount(mountpoint string, fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry, ok := b.mounts[mountpoint]; ok {
		entry.beforeUnmount = append(entry.beforeUnmount, fn)
		b.mounts[mountpoint] = entry
	}
}

// doUnmount tears down a single mount: fires beforeUnmount callbacks (LIFO),
// unmounts the FS via the driver, removes the mount from tracking, and
// unregisters the volume from the driver.
func (b *Backend) doUnmount(ctx context.Context, mountpoint string, entry mountEntry) {
	for i := len(entry.beforeUnmount) - 1; i >= 0; i-- {
		entry.beforeUnmount[i]()
	}
	if err := entry.driver.Unmount(ctx, entry.handle); err != nil {
		slog.Warn("unmount failed", "mountpoint", mountpoint, "error", err)
	}
	b.mu.Lock()
	delete(b.mounts, mountpoint)
	b.mu.Unlock()
	entry.driver.UnregisterVolume(entry.vol.Name())
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
func (b *Backend) FS(mountpoint string) (*RootFS, error) {
	entry, err := b.entry(mountpoint)
	if err != nil {
		return nil, err
	}
	return entry.driver.FS(entry.handle)
}

// FSForVolume returns a rooted filesystem for a volume by name, auto-mounting if needed.
func (b *Backend) FSForVolume(ctx context.Context, volume string) (*RootFS, error) {
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
// Acquires a ref on the volume; DeviceDetach releases it.
func (b *Backend) DeviceAttach(ctx context.Context, volume string) (string, error) {
	vol, err := b.vm.OpenVolume(volume)
	if err != nil {
		return "", err
	}
	if err := vol.AcquireRef(); err != nil {
		return "", fmt.Errorf("acquire ref for device attach: %w", err)
	}
	driver, err := b.driverFor()
	if err != nil {
		util.SafeRun(vol.ReleaseRef, "release ref after driver lookup failure")
		return "", err
	}
	driver.RegisterVolume(volume, vol)
	return b.DevicePath(volume)
}

// DeviceDetach disconnects the block device and closes a volume opened via DeviceAttach.
func (b *Backend) DeviceDetach(ctx context.Context, volume string) error {
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	driver, _ := b.driverFor()
	if driver != nil {
		driver.UnregisterVolume(volume)
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

// DeviceClone creates an unattached clone.
func (b *Backend) DeviceClone(ctx context.Context, volume, checkpointID, clone string) error {
	if checkpointID != "" {
		return b.vm.CloneFromCheckpoint(ctx, volume, checkpointID, clone)
	}
	vol := b.vm.GetVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %q not open", volume)
	}
	return vol.Clone(clone)
}

// DevicePath returns the block device path for a volume.
func (b *Backend) DevicePath(volume string) (string, error) {
	if b.driver == nil {
		return "", fmt.Errorf("backend does not expose device paths")
	}
	dev := b.driver.DevicePath(volume)
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

// Mounts returns a copy of all active mountpoint → volume name mappings.
func (b *Backend) Mounts() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[string]string, len(b.mounts))
	for k, v := range b.mounts {
		cp[k] = v.vol.Name()
	}
	return cp
}

// Close shuts down all mounted volumes concurrently, then closes drivers and
// the volume manager. Each volume independently goes through:
//
//  1. fsfreeze (quiesce filesystem so no new writes land)
//  2. flush (persist dirty data to S3)
//  3. unmount (detach the filesystem)
//  4. flush again (catch any writes that landed between freeze and unmount)
//
// Every step is best-effort: failures are logged but never prevent later steps
// or other volumes from proceeding.
func (b *Backend) Close(ctx context.Context) error {
	mounts := b.Mounts() // copy: mountpoint → volume name

	slog.Info("backend close: unmounting volumes", "count", len(mounts))
	var wg sync.WaitGroup
	for mp, vol := range mounts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("backend close: closing mount", "mountpoint", mp, "volume", vol)
			b.closeMount(ctx, mp, vol)
			slog.Info("backend close: mount closed", "mountpoint", mp, "volume", vol)
		}()
	}
	wg.Wait()
	slog.Info("backend close: all mounts closed")

	if b.driver != nil {
		slog.Info("backend close: closing driver")
		if err := b.driver.Close(ctx); err != nil {
			slog.Warn("backend close: driver close failed", "error", err)
		}
	}
	slog.Info("backend close: closing volume manager")
	if err := b.vm.Close(ctx); err != nil {
		slog.Warn("backend close: volume manager close failed", "error", err)
		return err
	}
	slog.Info("backend close: done")
	return nil
}

// closeMount shuts down a single mount. Every step is best-effort.
func (b *Backend) closeMount(ctx context.Context, mountpoint, volume string) {
	b.mu.Lock()
	entry, ok := b.mounts[mountpoint]
	b.mu.Unlock()
	if !ok {
		return
	}

	// 1. Quiesce FS (manual — shutdown doesn't use before_freeze hooks).
	slog.Info("closeMount: freezing", "mountpoint", mountpoint, "volume", volume)
	if err := entry.driver.Freeze(ctx, entry.handle); err != nil {
		slog.Warn("freeze failed during close", "mountpoint", mountpoint, "error", err)
	}

	// 2. Flush to S3.
	slog.Info("closeMount: flushing", "mountpoint", mountpoint, "volume", volume)
	if err := entry.vol.Flush(); err != nil {
		slog.Warn("flush failed during close", "volume", volume, "error", err)
	}

	// 3. Tear down mount (chroot teardown, FS unmount, map cleanup).
	slog.Info("closeMount: unmounting", "mountpoint", mountpoint, "volume", volume)
	b.doUnmount(ctx, mountpoint, entry)

	// 4. Release ref (may trigger volume destruction which flushes again).
	slog.Info("closeMount: releasing ref", "mountpoint", mountpoint, "volume", volume)
	if err := entry.vol.ReleaseRef(); err != nil {
		slog.Warn("release ref failed during close", "volume", volume, "error", err)
	}
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

// MountOpen mounts an already-open Volume at the given mountpoint.
func (b *Backend) MountOpen(ctx context.Context, vol loophole.Volume, mountpoint string) error {
	return b.mountVolume(ctx, vol, mountpoint)
}

// mountVolume mounts an already-open *Volume. Used by Mount, Clone, and MountOpen.
// Acquires a ref on the volume; Unmount releases it.
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

	if err := vol.AcquireRef(); err != nil {
		return fmt.Errorf("acquire ref for mount: %w", err)
	}

	driver, err := b.driverFor()
	if err != nil {
		util.SafeRun(vol.ReleaseRef, "release ref after driver lookup failure")
		return err
	}
	slog.Debug("backend: registering volume for mount", "volume", vol.Name())
	driver.RegisterVolume(vol.Name(), vol)
	slog.Debug("backend: calling driver.Mount", "volume", vol.Name(), "mountpoint", mountpoint)
	handle, err := driver.Mount(ctx, vol, mountpoint)
	if err != nil {
		slog.Info("backend: driver.Mount failed", "volume", vol.Name(), "error", err)
		driver.UnregisterVolume(vol.Name())
		util.SafeRun(vol.ReleaseRef, "release ref after mount failure")
		return err
	}

	slog.Debug("backend: mountVolume done", "volume", vol.Name(), "mountpoint", mountpoint)
	b.mu.Lock()
	b.mounts[mountpoint] = mountEntry{vol: vol, handle: handle, driver: driver}
	b.mu.Unlock()

	// Register before-freeze hook — flushes FS cache before volume freeze.
	vol.OnBeforeFreeze(func() error {
		return driver.Freeze(ctx, handle)
	})

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
