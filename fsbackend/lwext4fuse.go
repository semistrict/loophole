package fsbackend

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fuselwext4"
	"github.com/semistrict/loophole/lwext4"
)

// lwext4FUSEMount is the per-mount handle for Lwext4FUSEDriver.
type lwext4FUSEMount struct {
	mountpoint string
	ext4fs     *lwext4.FS
	server     *fuselwext4.Server
}

// Lwext4FUSEDriver implements Driver using FUSE + in-process lwext4.
// No root required — uses fusermount3 for unprivileged FUSE mounts.
type Lwext4FUSEDriver struct {
	volumeSize int64
}

var _ Driver[lwext4FUSEMount] = (*Lwext4FUSEDriver)(nil)

// NewLwext4FUSE creates a Service backed by FUSE + lwext4.
func NewLwext4FUSE(vm *loophole.VolumeManager, opts *Lwext4Options) Service {
	volumeSize := int64(defaultVolumeSize)
	if opts != nil && opts.VolumeSize > 0 {
		volumeSize = opts.VolumeSize
	}
	return New[lwext4FUSEMount](&Lwext4FUSEDriver{
		volumeSize: volumeSize,
	}, vm)
}

func (d *Lwext4FUSEDriver) Format(ctx context.Context, vol *loophole.Volume) error {
	vio := vol.IO(ctx)
	fs, err := lwext4.Format(vio, d.volumeSize, &lwext4.FormatOptions{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	})
	if err != nil {
		return fmt.Errorf("lwext4: format: %w", err)
	}
	return fs.Close()
}

func (d *Lwext4FUSEDriver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) (lwext4FUSEMount, error) {
	vio := vol.IO(ctx)
	ext4fs, err := lwext4.Mount(vio, d.volumeSize)
	if err != nil {
		return lwext4FUSEMount{}, fmt.Errorf("lwext4: mount: %w", err)
	}
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		// Try to recover from a stale FUSE mount.
		if err := exec.Command("fusermount", "-u", mountpoint).Run(); err != nil {
			slog.Warn("fusermount -u failed", "mountpoint", mountpoint, "error", err)
		}
		if err := os.MkdirAll(mountpoint, 0o755); err != nil {
			if closeErr := ext4fs.Close(); closeErr != nil {
				slog.Warn("ext4fs close error", "error", closeErr)
			}
			return lwext4FUSEMount{}, fmt.Errorf("mkdir %s: %w", mountpoint, err)
		}
	}
	server, err := fuselwext4.Mount(mountpoint, ext4fs)
	if err != nil {
		if closeErr := ext4fs.Close(); closeErr != nil {
			slog.Warn("ext4fs close error", "error", closeErr)
		}
		return lwext4FUSEMount{}, fmt.Errorf("fuse mount %s: %w", mountpoint, err)
	}
	return lwext4FUSEMount{mountpoint: mountpoint, ext4fs: ext4fs, server: server}, nil
}

func (d *Lwext4FUSEDriver) Unmount(_ context.Context, h lwext4FUSEMount) error {
	if err := h.server.Unmount(); err != nil {
		return err
	}
	return h.ext4fs.Close()
}

func (d *Lwext4FUSEDriver) Freeze(_ context.Context, h lwext4FUSEMount) error {
	return h.ext4fs.CacheFlush()
}

func (d *Lwext4FUSEDriver) Thaw(_ context.Context, _ lwext4FUSEMount) error {
	return nil
}

func (d *Lwext4FUSEDriver) Close(_ context.Context) error {
	return nil
}

func (d *Lwext4FUSEDriver) FS(h lwext4FUSEMount) (FS, error) {
	return newOSFS(h.mountpoint), nil
}
