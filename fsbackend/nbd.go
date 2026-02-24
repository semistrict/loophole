//go:build linux

package fsbackend

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/ext4"
	"github.com/semistrict/loophole/nbdvm"
)

// NBDDriver implements Driver using NBD devices (/dev/nbdN) + kernel ext4.
type NBDDriver struct {
	nbd *nbdvm.Server
}

var _ Driver = (*NBDDriver)(nil)
var _ DevicePather = (*NBDDriver)(nil)
var _ DeviceConnector = (*NBDDriver)(nil)

// NewNBD creates a Backend backed by NBD.
func NewNBD(vm *loophole.VolumeManager, opts *nbdvm.Options) (*Backend, error) {
	srv, err := nbdvm.NewServer(vm, opts)
	if err != nil {
		return nil, err
	}
	return New(&NBDDriver{nbd: srv}, vm), nil
}

func (n *NBDDriver) Format(ctx context.Context, vol *loophole.Volume) error {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return err
	}
	return ext4.FormatDirect(ctx, dev)
}

func (n *NBDDriver) Mount(ctx context.Context, vol *loophole.Volume, mountpoint string) error {
	dev, err := n.nbd.Connect(ctx, vol)
	if err != nil {
		return err
	}
	if err := ext4.MountDirect(ctx, dev, mountpoint); err != nil {
		n.nbd.Disconnect(ctx, vol.Name())
		return err
	}
	return nil
}

func (n *NBDDriver) Unmount(ctx context.Context, mountpoint string, volumeName string) error {
	if err := ext4.UnmountDirect(ctx, mountpoint); err != nil {
		return err
	}
	if volumeName != "" {
		if err := n.nbd.Disconnect(ctx, volumeName); err != nil {
			return fmt.Errorf("nbd disconnect %q: %w", volumeName, err)
		}
	}
	return nil
}

func (n *NBDDriver) Freeze(ctx context.Context, mountpoint string) error {
	return ext4.Freeze(ctx, mountpoint)
}

func (n *NBDDriver) Thaw(ctx context.Context, mountpoint string) error {
	return ext4.Thaw(ctx, mountpoint)
}

func (n *NBDDriver) Close(ctx context.Context) error {
	n.nbd.Close(ctx)
	return nil
}

func (n *NBDDriver) ConnectDevice(ctx context.Context, vol *loophole.Volume) (string, error) {
	return n.nbd.Connect(ctx, vol)
}

func (n *NBDDriver) DisconnectDevice(ctx context.Context, volumeName string) error {
	return n.nbd.Disconnect(ctx, volumeName)
}

func (n *NBDDriver) DevicePath(volumeName string) string {
	return n.nbd.DevicePath(volumeName)
}

func (n *NBDDriver) FS(mountpoint string) (FS, error) {
	return newOSFS(mountpoint), nil
}
