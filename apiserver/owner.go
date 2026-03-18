package apiserver

import (
	"context"
	"fmt"
	"os"

	"github.com/semistrict/loophole"
)

// CreateAndMount creates a fresh volume and mounts it on this owner process.
func (d *Server) CreateAndMount(ctx context.Context, p loophole.CreateParams, mountpoint string) (string, error) {
	if d.backend == nil {
		return "", fmt.Errorf("storage not available: %s", d.startupErr)
	}
	if p.Volume == "" {
		return "", fmt.Errorf("volume name is required")
	}
	if mountpoint == "" {
		mountpoint = p.Volume
	}
	if err := d.backend.Create(ctx, p); err != nil {
		return "", err
	}
	if err := d.backend.Mount(ctx, p.Volume, mountpoint); err != nil {
		return "", err
	}
	d.managedVolume = p.Volume
	d.mountpoint = mountpoint
	d.writeSymlink(mountpoint)
	return mountpoint, nil
}

// MountVolume mounts an existing volume on this owner process.
func (d *Server) MountVolume(ctx context.Context, volume, mountpoint string) (string, error) {
	if d.backend == nil {
		return "", fmt.Errorf("storage not available: %s", d.startupErr)
	}
	if volume == "" {
		return "", fmt.Errorf("volume name is required")
	}
	if mountpoint == "" {
		mountpoint = volume
	}
	if err := d.backend.Mount(ctx, volume, mountpoint); err != nil {
		return "", err
	}
	d.managedVolume = volume
	d.mountpoint = mountpoint
	d.writeSymlink(mountpoint)
	return mountpoint, nil
}

// AttachVolume attaches an existing volume as a raw block device on this owner process.
func (d *Server) AttachVolume(ctx context.Context, volume string) (string, error) {
	if d.backend == nil {
		return "", fmt.Errorf("storage not available: %s", d.startupErr)
	}
	if volume == "" {
		return "", fmt.Errorf("volume name is required")
	}
	devicePath, err := d.backend.DeviceAttach(ctx, volume)
	if err != nil {
		return "", err
	}
	d.managedVolume = volume
	d.devicePath = devicePath
	d.writeDeviceSymlink(devicePath)
	return devicePath, nil
}

// Cleanup tears down local server resources after a setup failure before Serve runs.
func (d *Server) Cleanup(ctx context.Context) {
	d.removeOwnerLinks()
	if d.backend != nil {
		_ = d.backend.Close(ctx)
	}
	if d.diskCache != nil {
		_ = d.diskCache.Close()
	}
	if d.ln != nil {
		_ = d.ln.Close()
	}
	if d.socket != "" {
		_ = os.Remove(d.socket)
	}
	if d.axiomClose != nil {
		d.axiomClose()
	}
}
