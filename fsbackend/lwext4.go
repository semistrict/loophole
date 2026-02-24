package fsbackend

import (
	"context"
	"fmt"
	"sync"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/lwext4"
)

const defaultVolumeSize = 100 * 1024 * 1024 * 1024 // 100 GB

// Lwext4Driver implements Driver using an in-process lwext4 filesystem.
// Mountpoints are logical keys — no kernel mount occurs.
type Lwext4Driver struct {
	volumeSize int64

	mu     sync.Mutex
	mounts map[string]*lwext4Mount
}

type lwext4Mount struct {
	fs  *lwext4.FS
	vol *loophole.Volume
}

var _ Driver = (*Lwext4Driver)(nil)

// Lwext4Options configures the in-process lwext4 backend.
type Lwext4Options struct {
	VolumeSize int64 // apparent volume size; default 1 GB
}

// NewLwext4 creates a Backend backed by in-process lwext4.
func NewLwext4(vm *loophole.VolumeManager, opts *Lwext4Options) *Backend {
	volumeSize := int64(defaultVolumeSize)
	if opts != nil && opts.VolumeSize > 0 {
		volumeSize = opts.VolumeSize
	}
	return New(&Lwext4Driver{
		volumeSize: volumeSize,
		mounts:     make(map[string]*lwext4Mount),
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

func (l *Lwext4Driver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	vio := vol.IO(ctx)

	fs, err := lwext4.Mount(vio, l.volumeSize)
	if err != nil {
		return fmt.Errorf("lwext4: mount: %w", err)
	}

	l.mounts[mountpoint] = &lwext4Mount{fs: fs, vol: vol}
	return nil
}

func (l *Lwext4Driver) Unmount(_ context.Context, mountpoint string, _ string) error {
	l.mu.Lock()
	m, ok := l.mounts[mountpoint]
	if ok {
		delete(l.mounts, mountpoint)
	}
	l.mu.Unlock()

	if !ok {
		return fmt.Errorf("lwext4: %q not mounted", mountpoint)
	}
	return m.fs.Close()
}

func (l *Lwext4Driver) Freeze(_ context.Context, mountpoint string) error {
	l.mu.Lock()
	m, ok := l.mounts[mountpoint]
	l.mu.Unlock()

	if !ok {
		return fmt.Errorf("lwext4: %q not mounted", mountpoint)
	}
	return m.fs.CacheFlush()
}

func (l *Lwext4Driver) Thaw(_ context.Context, mountpoint string) error {
	return nil
}

func (l *Lwext4Driver) Close(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var firstErr error
	for mp, m := range l.mounts {
		if err := m.fs.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(l.mounts, mp)
	}
	return firstErr
}

// FS returns a filesystem handle for the given mountpoint.
func (l *Lwext4Driver) FS(mountpoint string) (FS, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m, ok := l.mounts[mountpoint]
	if !ok {
		return nil, fmt.Errorf("lwext4: %q not mounted", mountpoint)
	}
	return newLwext4FS(m.fs), nil
}
