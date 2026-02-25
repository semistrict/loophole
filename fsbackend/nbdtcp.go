//go:build linux

package fsbackend

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/ext4"
	"github.com/semistrict/loophole/nbdvm"
)

// nbdTCPMount is the per-mount handle for NBDTCPDriver.
type nbdTCPMount struct {
	mountpoint string
	volumeName string
}

// NBDTCPDriver implements Driver using TCP-based NBD + kernel ext4.
type NBDTCPDriver struct {
	nbd *nbdvm.TCPServer
}

var _ Driver[nbdTCPMount] = (*NBDTCPDriver)(nil)
var _ DevicePather = (*NBDTCPDriver)(nil)
var _ DeviceConnector = (*NBDTCPDriver)(nil)

// NewNBDTCP creates a Service backed by TCP NBD.
func NewNBDTCP(vm *loophole.VolumeManager, opts *nbdvm.Options) (Service, error) {
	srv, err := nbdvm.NewTCPServer(vm, opts)
	if err != nil {
		return nil, err
	}
	return New[nbdTCPMount](&NBDTCPDriver{nbd: srv}, vm), nil
}

func (n *NBDTCPDriver) Format(ctx context.Context, vol *loophole.Volume) error {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return err
	}
	return ext4.FormatDirect(ctx, dev)
}

func (n *NBDTCPDriver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) (nbdTCPMount, error) {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return nbdTCPMount{}, err
	}
	if err := ext4.MountDirect(ctx, dev, mountpoint); err != nil {
		n.nbd.Disconnect(ctx, vol.Name())
		return nbdTCPMount{}, err
	}
	return nbdTCPMount{mountpoint: mountpoint, volumeName: vol.Name()}, nil
}

func (n *NBDTCPDriver) Unmount(ctx context.Context, h nbdTCPMount) error {
	if err := ext4.UnmountDirect(ctx, h.mountpoint); err != nil {
		return err
	}
	if h.volumeName != "" {
		if err := n.nbd.Disconnect(ctx, h.volumeName); err != nil {
			return fmt.Errorf("nbd-tcp disconnect %q: %w", h.volumeName, err)
		}
	}
	return nil
}

func (n *NBDTCPDriver) Freeze(ctx context.Context, h nbdTCPMount) error {
	return ext4.Freeze(ctx, h.mountpoint)
}

func (n *NBDTCPDriver) Thaw(ctx context.Context, h nbdTCPMount) error {
	return ext4.Thaw(ctx, h.mountpoint)
}

func (n *NBDTCPDriver) Close(ctx context.Context) error {
	n.nbd.Close(ctx)
	return nil
}

func (n *NBDTCPDriver) ConnectDevice(ctx context.Context, vol *loophole.Volume) (string, error) {
	return n.nbd.Connect(ctx, vol)
}

func (n *NBDTCPDriver) DisconnectDevice(ctx context.Context, volumeName string) error {
	return n.nbd.Disconnect(ctx, volumeName)
}

func (n *NBDTCPDriver) DevicePath(volumeName string) string {
	return n.nbd.DevicePath(volumeName)
}

func (n *NBDTCPDriver) FS(h nbdTCPMount) (FS, error) {
	return newOSFS(h.mountpoint), nil
}
