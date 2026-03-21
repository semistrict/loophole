//go:build !linux

package nbdvm

import (
	"context"
	"runtime"

	"github.com/semistrict/loophole/storage"
)

// Options configures the NBD server.
type Options struct{}

// Server manages NBD exports. Only available on Linux.
type Server struct{}

// NewServer returns an error on non-Linux platforms.
func NewServer(_ *storage.Manager, _ *Options) (*Server, error) {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}

func (s *Server) DevicePath(_ string) string {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}

func (s *Server) Connect(_ context.Context, _ *storage.Volume) (string, error) {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}

func (s *Server) Disconnect(_ context.Context, _ string) error {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}

func (s *Server) Close(_ context.Context) {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}

func Available() error {
	panic("NBD server requires Linux (got " + runtime.GOOS + ")")
}
