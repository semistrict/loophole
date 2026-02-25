// Package fuselwext4 implements a FUSE filesystem backed by lwext4.
// It serves a real mountpoint using go-fuse v2's nodefs API, delegating
// all operations to an lwext4.FS inode-level API. No root required.
package fuselwext4

import (
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
	root := &ext4Node{
		ext4fs: ext4fs,
		mu:     &sync.Mutex{},
		ino:    lwext4.RootIno,
	}

	sec := time.Second
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "loophole-lwext4",
			Name:   "loophole",
		},
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
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
