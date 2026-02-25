package fsbackend

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lwext4"
)

const defaultVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB

// lwext4Mount is the per-mount handle for Lwext4Driver.
type lwext4Mount struct {
	fs *lwext4.FS
}

// Lwext4Driver implements Driver using an in-process lwext4 filesystem.
// Mountpoints are logical keys — no kernel mount occurs.
type Lwext4Driver struct {
	volumeSize int64
}

var _ Driver[lwext4Mount] = (*Lwext4Driver)(nil)

// Lwext4Options configures the in-process lwext4 backend.
type Lwext4Options struct {
	VolumeSize int64 // apparent volume size; default 100 GB
}

// NewLwext4 creates a Service backed by in-process lwext4.
func NewLwext4(vm *loophole.VolumeManager, opts *Lwext4Options) Service {
	volumeSize := int64(defaultVolumeSize)
	if opts != nil && opts.VolumeSize > 0 {
		volumeSize = opts.VolumeSize
	}
	return New[lwext4Mount](&Lwext4Driver{
		volumeSize: volumeSize,
	}, vm)
}

func (l *Lwext4Driver) Format(ctx context.Context, vol *loophole.Volume) error {
	vio := vol.IO(ctx)
	fs, err := lwext4.Format(vio, l.volumeSize, nil)
	if err != nil {
		return fmt.Errorf("lwext4: format: %w", err)
	}
	return fs.Close()
}

func (l *Lwext4Driver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) (lwext4Mount, error) {
	vio := vol.IO(ctx)
	fs, err := lwext4.Mount(vio, l.volumeSize)
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
