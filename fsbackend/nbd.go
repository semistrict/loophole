//go:build linux

package fsbackend

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/linuxutil"
	"github.com/semistrict/loophole/nbdvm"
)

// nbdMount is the per-mount handle for NBDDriver.
type nbdMount struct {
	mountpoint string
	volumeName string
}

// NBDDriver implements Driver using NBD devices (/dev/nbdN) + kernel FS.
type NBDDriver struct {
	nbd *nbdvm.Server
}

var _ Driver[nbdMount] = (*NBDDriver)(nil)
var _ DevicePather = (*NBDDriver)(nil)
var _ DeviceConnector = (*NBDDriver)(nil)

// NewNBDDriver creates a type-erased NBD driver.
func NewNBDDriver(vm loophole.VolumeManager, opts *nbdvm.Options) (AnyDriver, error) {
	srv, err := nbdvm.NewServer(vm, opts)
	if err != nil {
		return nil, err
	}
	return EraseDriver[nbdMount](&NBDDriver{nbd: srv}), nil
}

// NewNBD creates a Service backed by NBD.
func NewNBD(vm loophole.VolumeManager, opts *nbdvm.Options) (Service, error) {
	d, err := NewNBDDriver(vm, opts)
	if err != nil {
		return nil, err
	}
	return New(vm, d, loophole.VolumeTypeExt4, loophole.VolumeTypeXFS), nil
}

func (n *NBDDriver) Format(ctx context.Context, vol loophole.Volume) error {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return err
	}
	return linuxutil.FormatFSDirect(ctx, dev, vol.VolumeType())
}

func (n *NBDDriver) Mount(ctx context.Context, vol loophole.Volume, mountpoint string) (nbdMount, error) {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return nbdMount{}, err
	}
	if err := linuxutil.MountFSDirect(ctx, dev, mountpoint, vol.VolumeType()); err != nil {
		if disconnErr := n.nbd.Disconnect(ctx, vol.Name()); disconnErr != nil {
			slog.Warn("nbd disconnect error during mount cleanup", "error", disconnErr)
		}
		return nbdMount{}, err
	}
	return nbdMount{mountpoint: mountpoint, volumeName: vol.Name()}, nil
}

func (n *NBDDriver) Unmount(ctx context.Context, h nbdMount) error {
	if err := linuxutil.Unmount(h.mountpoint); err != nil {
		return err
	}
	if h.volumeName != "" {
		if err := n.nbd.Disconnect(ctx, h.volumeName); err != nil {
			return fmt.Errorf("nbd disconnect %q: %w", h.volumeName, err)
		}
	}
	return nil
}

func (n *NBDDriver) Freeze(ctx context.Context, h nbdMount) error {
	return linuxutil.Freeze(h.mountpoint)
}

func (n *NBDDriver) Thaw(ctx context.Context, h nbdMount) error {
	return linuxutil.Thaw(h.mountpoint)
}

func (n *NBDDriver) Close(ctx context.Context) error {
	n.nbd.Close(ctx)
	return nil
}

func (n *NBDDriver) ConnectDevice(ctx context.Context, vol loophole.Volume) (string, error) {
	return n.nbd.Connect(ctx, vol)
}

func (n *NBDDriver) DisconnectDevice(ctx context.Context, volumeName string) error {
	// Unmount any kernel filesystem on this device before disconnecting.
	dev := n.nbd.DevicePath(volumeName)
	if dev != "" {
		if mp := linuxutil.FindMountBySource(dev); mp != "" {
			slog.Info("nbd: unmounting before disconnect", "device", dev, "mountpoint", mp)
			if err := linuxutil.Unmount(mp); err != nil {
				slog.Warn("nbd: unmount before disconnect failed", "mountpoint", mp, "error", err)
			}
		}
	}
	return n.nbd.Disconnect(ctx, volumeName)
}

func (n *NBDDriver) DevicePath(volumeName string) string {
	return n.nbd.DevicePath(volumeName)
}

func (n *NBDDriver) FS(h nbdMount) (FS, error) {
	return NewOSFS(h.mountpoint), nil
}
