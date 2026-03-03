//go:build !linux

package nbdvm

import (
	"context"
	"fmt"
	"runtime"

	"github.com/semistrict/loophole"
)

// Options configures the NBD server.
type Options struct{}

// Server manages NBD exports. Only available on Linux.
type Server struct{}

// NewServer returns an error on non-Linux platforms.
func NewServer(_ loophole.VolumeManager, _ *Options) (*Server, error) {
	return nil, fmt.Errorf("NBD server requires Linux (got %s)", runtime.GOOS)
}

func (s *Server) DevicePath(_ string) string { return "" }

func (s *Server) Connect(_ context.Context, _ loophole.Volume) (string, error) {
	return "", fmt.Errorf("NBD server requires Linux (got %s)", runtime.GOOS)
}

func (s *Server) Disconnect(_ context.Context, _ string) error {
	return fmt.Errorf("NBD server requires Linux (got %s)", runtime.GOOS)
}

func (s *Server) Close(_ context.Context) {}
