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

// Log returns the daemon log file path for the given profile.
func (d Dir) Log(profile string) string {
	return filepath.Join(string(d), profile+".log")
}

// Cache returns the block cache directory for the given profile.
func (d Dir) Cache(profile string) string {
	return filepath.Join(string(d), "cache", profile)
}

// NBD returns the NBD Unix socket path for the given profile.
func (d Dir) NBD(profile string) string {
	return filepath.Join(string(d), profile+".nbd.sock")
}

// GRPC returns the default gRPC (snapshotter) socket path for the given profile.
func (d Dir) GRPC(profile string) string {
	return filepath.Join(string(d), profile+".grpc.sock")
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
