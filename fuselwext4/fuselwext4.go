// Package fuselwext4 implements a FUSE filesystem backed by lwext4.
// It serves a real mountpoint using go-fuse v2's nodefs API, delegating
// all operations to an lwext4.FS inode-level API. No root required.
package fuselwext4

import (
	"fmt"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/lwext4"
)

// Server manages a FUSE mount backed by lwext4.
type Server struct {
	server *fuse.Server
}

// Mount starts a FUSE filesystem at mountpoint backed by ext4fs.
// The lwext4.FS must remain valid until Unmount is called.
// All FUSE operations are serialized through a mutex since lwext4 is not thread-safe.
func Mount(mountpoint string, ext4fs *lwext4.FS) (*Server, error) {
	// Clean up any orphaned inodes left from a previous crash.
	if err := ext4fs.OrphanRecover(); err != nil {
		return nil, fmt.Errorf("fuselwext4: orphan recovery failed: %w", err)
	}

	root := &ext4Node{
		ext4fs:  ext4fs,
		mu:      &sync.Mutex{},
		ino:     lwext4.RootIno,
		orphans: newOrphanTracker(),
	}

	cacheTTL := 5 * time.Second
	negTTL := time.Second
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:            "loophole-lwext4",
			Name:              "loophole",
			DisableXAttrs:     true,
			MaxWrite:          1 << 20, // 1 MiB
			MaxReadAhead:      1 << 20, // 1 MiB
			MaxBackground:     128,
			DirectMount:       true,
			ExtraCapabilities: fuse.CAP_WRITEBACK_CACHE, // kernel batches writes into page cache
		},
		AttrTimeout:     &cacheTTL,
		EntryTimeout:    &cacheTTL,
		NegativeTimeout: &negTTL, // cache ENOENT lookups
	})
	if err != nil {
		return nil, err
	}
	return &Server{server: server}, nil
}

// Unmount unmounts the FUSE filesystem.
func (s *Server) Unmount() error {
	return s.server.Unmount()
}

// Wait blocks until the FUSE server exits.
func (s *Server) Wait() {
	s.server.Wait()
}
