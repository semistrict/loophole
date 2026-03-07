package fsbackend

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lwext4"
)

// lwext4Mount is the per-mount handle for Lwext4Driver.
type lwext4Mount struct {
	fs *lwext4.FS
}

// Lwext4Driver implements Driver using an in-process lwext4 filesystem.
// Mountpoints are logical keys — no kernel mount occurs.
type Lwext4Driver struct{}

var _ Driver[lwext4Mount] = (*Lwext4Driver)(nil)

// NewLwext4Driver returns a type-erased driver for in-process lwext4.
func NewLwext4Driver() AnyDriver {
	return EraseDriver[lwext4Mount](&Lwext4Driver{})
}

// NewLwext4 creates a Service backed by in-process lwext4.
func NewLwext4(vm loophole.VolumeManager) Service {
	return New(vm, NewLwext4Driver(), loophole.VolumeTypeExt4)
}

func (l *Lwext4Driver) Format(ctx context.Context, vol loophole.Volume) error {
	fs, err := lwext4.Format(vol, int64(vol.Size()), nil)
	if err != nil {
		return fmt.Errorf("lwext4: format: %w", err)
	}
	return fs.Close()
}

func (l *Lwext4Driver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (lwext4Mount, error) {
	var fs *lwext4.FS
	var err error
	if vol.ReadOnly() {
		fs, err = lwext4.MountReadOnly(vol, int64(vol.Size()))
	} else {
		fs, err = lwext4.Mount(vol, int64(vol.Size()))
	}
	if err != nil {
		return lwext4Mount{}, fmt.Errorf("lwext4: mount: %w", err)
	}
	return lwext4Mount{fs: fs}, nil
}

func (l *Lwext4Driver) Unmount(_ context.Context, h lwext4Mount) error {
	return h.fs.Close()
}

func (l *Lwext4Driver) Freeze(_ context.Context, h lwext4Mount) error {
	return h.fs.CacheFlush()
}

func (l *Lwext4Driver) Thaw(_ context.Context, _ lwext4Mount) error {
	return nil
}

func (l *Lwext4Driver) Close(_ context.Context) error {
	return nil
}

// FS returns a filesystem handle for the given mount.
func (l *Lwext4Driver) FS(h lwext4Mount) (FS, error) {
	return newLwext4FS(h.fs), nil
}
