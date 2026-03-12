package loophole

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

// Dir is the loophole home directory (typically ~/.loophole).
// It derives all local filesystem paths for sockets, caches, logs, etc.
type Dir string

// DefaultDir returns the loophole home directory. It checks LOOPHOLE_HOME
// first, falling back to ~/.loophole.
func DefaultDir() Dir {
	if d := os.Getenv("LOOPHOLE_HOME"); d != "" {
		return Dir(d)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}
	return Dir(filepath.Join(home, ".loophole"))
}

// Socket returns the Unix socket path for the given profile.
func (d Dir) Socket(profile string) string {
	return filepath.Join(string(d), profile+".sock")
}

// VolumeSocket returns the Unix socket path for a single-volume owner process.
func (d Dir) VolumeSocket(volume string) string {
	h := sha256.Sum256([]byte(volume))
	return filepath.Join(string(d), "volumes", fmt.Sprintf("%x.sock", h[:6]))
}

// Fuse returns the internal FUSE mount directory for the given profile.
func (d Dir) Fuse(profile string) string {
	return filepath.Join(string(d), "fuse", profile)
}

// Log returns the daemon log file path for the given profile.
func (d Dir) Log(profile string) string {
	return filepath.Join(string(d), profile+".log")
}

// Cache returns the block cache directory for the given profile.
func (d Dir) Cache(profile string) string {
	return filepath.Join(string(d), "cache", profile)
}

// MountSymlink returns the symlink path that maps a user mountpoint back to
// the daemon socket. This allows post-mount commands to find the daemon.
func (d Dir) MountSymlink(mountpoint string) string {
	abs, err := filepath.Abs(mountpoint)
	if err != nil {
		abs = mountpoint
	}
	h := sha256.Sum256([]byte(abs))
	return filepath.Join(string(d), "mounts", fmt.Sprintf("%x.sock", h[:6]))
}

// DeviceSymlink returns the symlink path that maps a block-device path back to
// the owner socket. This allows device-level commands to find the process.
func (d Dir) DeviceSymlink(device string) string {
	abs, err := filepath.Abs(device)
	if err != nil {
		abs = device
	}
	h := sha256.Sum256([]byte(abs))
	return filepath.Join(string(d), "devices", fmt.Sprintf("%x.sock", h[:6]))
}
