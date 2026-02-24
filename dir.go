package loophole

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Dir is the loophole home directory (typically ~/.loophole).
// It derives all local filesystem paths for sockets, caches, logs, etc.
type Dir string

// DefaultDir returns the default loophole home directory (~/.loophole).
func DefaultDir() Dir {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}
	return Dir(filepath.Join(home, ".loophole"))
}

// Socket returns the Unix socket path for the given instance.
func (d Dir) Socket(inst Instance) string {
	return filepath.Join(string(d), inst.Hash()+".sock")
}

// Fuse returns the internal FUSE mount directory for the given instance.
func (d Dir) Fuse(inst Instance) string {
	return filepath.Join(string(d), "fuse", inst.Hash())
}

// NBD returns the directory for per-volume NBD Unix sockets.
func (d Dir) NBD(inst Instance) string {
	return filepath.Join(string(d), "nbd", inst.Hash())
}

// Log returns the daemon log file path for the given instance.
func (d Dir) Log(inst Instance) string {
	return filepath.Join(string(d), inst.Hash()+".log")
}

// Cache returns the block cache directory for the given instance.
func (d Dir) Cache(inst Instance) string {
	return filepath.Join(string(d), "cache", inst.Hash())
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

// FindSocket returns the path of the only live daemon socket in this
// directory. Returns an error if zero or more than one are found.
func (d Dir) FindSocket() (string, error) {
	pattern := filepath.Join(string(d), "*.sock")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}

	var live []string
	for _, path := range matches {
		if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
			conn.Close()
			live = append(live, path)
		}
	}

	switch len(live) {
	case 0:
		return "", fmt.Errorf("no running daemons found")
	case 1:
		return live[0], nil
	default:
		return "", fmt.Errorf("multiple daemons running (%d); specify an S3 URL to select one", len(live))
	}
}
