//go:build linux

package fsserver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/semistrict/loophole/internal/nbdvm"
	"github.com/semistrict/loophole/internal/storage"
)

type nbdMount struct {
	mountpoint string
	volumeName string
}

// NBDDriver implements the Linux filesystem path using in-kernel NBD devices.
type NBDDriver struct {
	server *nbdvm.Server
}

func NewNBDDriver(vm *storage.Manager, opts *nbdvm.Options) (*NBDDriver, error) {
	server, err := nbdvm.NewServer(vm, opts)
	if err != nil {
		return nil, err
	}
	return &NBDDriver{server: server}, nil
}

func (n *NBDDriver) Format(ctx context.Context, vol *storage.Volume) error {
	devPath, err := n.server.Connect(ctx, vol)
	if err != nil {
		return err
	}
	formatErr := formatFSDirect(ctx, devPath)
	if formatErr == nil {
		if flushErr := flushBlockDevice(devPath); flushErr != nil {
			formatErr = flushErr
		}
	}
	disconnectErr := n.server.Disconnect(ctx, vol.Name())
	if formatErr != nil {
		if disconnectErr != nil {
			slog.Warn("nbd disconnect after format failure", "volume", vol.Name(), "error", disconnectErr)
		}
		return formatErr
	}
	if disconnectErr != nil {
		return fmt.Errorf("disconnect NBD after format %q: %w", vol.Name(), disconnectErr)
	}
	return nil
}

func (n *NBDDriver) Mount(ctx context.Context, vol *storage.Volume, mountpoint string) (nbdMount, error) {
	devPath, err := n.server.Connect(ctx, vol)
	if err != nil {
		return nbdMount{}, err
	}
	if err := mountFSDirect(devPath, mountpoint); err != nil {
		if disconnectErr := n.server.Disconnect(ctx, vol.Name()); disconnectErr != nil {
			slog.Warn("nbd disconnect after mount failure", "volume", vol.Name(), "error", disconnectErr)
		}
		return nbdMount{}, err
	}
	return nbdMount{mountpoint: mountpoint, volumeName: vol.Name()}, nil
}

func (n *NBDDriver) Unmount(ctx context.Context, h nbdMount) error {
	if err := unmountFS(h.mountpoint); err != nil {
		return err
	}
	if h.volumeName != "" {
		if devPath := n.server.DevicePath(h.volumeName); devPath != "" {
			if err := flushBlockDevice(devPath); err != nil {
				return err
			}
		}
		if err := n.server.Disconnect(ctx, h.volumeName); err != nil {
			return fmt.Errorf("disconnect NBD volume %q: %w", h.volumeName, err)
		}
	}
	return nil
}

func (n *NBDDriver) Freeze(_ context.Context, h nbdMount) error {
	return freezeFS(h.mountpoint)
}

func (n *NBDDriver) Thaw(_ context.Context, h nbdMount) error {
	return thawFS(h.mountpoint)
}

func (n *NBDDriver) Close(ctx context.Context) error {
	n.server.Close(ctx)
	return nil
}

func (n *NBDDriver) ConnectDevice(ctx context.Context, vol *storage.Volume) (string, error) {
	return n.server.Connect(ctx, vol)
}

func (n *NBDDriver) DisconnectDevice(ctx context.Context, volumeName string) error {
	if devPath := n.server.DevicePath(volumeName); devPath != "" {
		if mountpoint := findMountpointBySource(devPath); mountpoint != "" {
			slog.Info("nbd: unmounting before disconnect", "volume", volumeName, "device", devPath, "mountpoint", mountpoint)
			if err := unmountFS(mountpoint); err != nil {
				return err
			}
		}
	}
	return n.server.Disconnect(ctx, volumeName)
}

func (n *NBDDriver) DevicePath(volumeName string) string {
	return n.server.DevicePath(volumeName)
}
