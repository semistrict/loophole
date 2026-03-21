package env

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

func shortHash(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return fmt.Sprintf("%x", sum[:6])
}

// VolumeSocket returns the Unix socket path for a single-volume owner process.
func (d Dir) VolumeSocket(volsetID, volume string) string {
	h := shortHash(volsetID, volume)
	return filepath.Join(string(d), "volumes", h+".sock")
}

// Fuse returns the internal FUSE mount directory for the given store.
func (d Dir) Fuse(volsetID string) string {
	return filepath.Join(string(d), "fuse", volsetID)
}

// VolumeLog returns the daemon log file path for a volume, co-located with
// the volume socket under ~/.loophole/volumes/.
func (d Dir) VolumeLog(volsetID, volume string) string {
	h := shortHash(volsetID, volume)
	return filepath.Join(string(d), "volumes", fmt.Sprintf("%s.log", h))
}

// Cache returns the block cache directory for the given store.
func (d Dir) Cache(volsetID string) string {
	return filepath.Join(string(d), "cache", volsetID)
}

// CachedSocket returns the page cache daemon socket path for the given store.
func (d Dir) CachedSocket(volsetID string) string {
	return filepath.Join(string(d), "cache", volsetID, "cached.sock")
}

// CachedLog returns the page cache daemon log file path for the given store.
func (d Dir) CachedLog(volsetID string) string {
	return filepath.Join(string(d), "cache", volsetID, "cached.log")
}

// SandboxdSocket returns the Unix socket path for the sandbox daemon.
func (d Dir) SandboxdSocket() string {
	return filepath.Join(string(d), "sandboxd.sock")
}

// SandboxdLog returns the sandbox daemon log file path.
func (d Dir) SandboxdLog() string {
	return filepath.Join(string(d), "sandboxd.log")
}

// SandboxdState returns the sandbox daemon state directory.
func (d Dir) SandboxdState() string {
	return filepath.Join(string(d), "sandboxd")
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
