package loophole

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
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

// Socket returns the Unix socket path for the given profile.
func (d Dir) Socket(profile string) string {
	return filepath.Join(string(d), profile+".sock")
}

// Fuse returns the internal FUSE mount directory for the given profile.
func (d Dir) Fuse(profile string) string {
	return filepath.Join(string(d), "fuse", profile)
}

// NBD returns the directory for per-volume NBD Unix sockets.
func (d Dir) NBD(profile string) string {
	return filepath.Join(string(d), "nbd", profile)
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
			if err := conn.Close(); err != nil {
				slog.Warn("close failed", "error", err)
			}
			live = append(live, path)
		}
	}

	switch len(live) {
	case 0:
		return "", fmt.Errorf("no running daemons found")
	case 1:
		return live[0], nil
	default:
		return "", fmt.Errorf("multiple daemons running (%d); specify a profile with -p", len(live))
	}
}
