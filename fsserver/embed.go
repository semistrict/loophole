package fsserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/semistrict/loophole/storage"
	"github.com/semistrict/loophole/volserver"
)

// EmbedSocketPath returns the UDS path for an embedded server running in the
// process with the given PID.
func EmbedSocketPath(pid int) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("loophole-%d.sock", pid))
}

// StartEmbedded starts a volserver on a PID-derived UDS socket for the given
// volume. This is designed for the C API / embedded use case where loophole is
// linked into another process (e.g. Firecracker).
//
// The returned cleanup function stops the server and removes the socket.
// The server runs in background goroutines; it does not block.
func StartEmbedded(vol *storage.Volume) (func(), error) {
	pid := os.Getpid()
	sockPath := EmbedSocketPath(pid)

	srv, err := volserver.Start(vol, sockPath)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = srv.Serve(ctx)
	}()

	cleanup := func() {
		cancel()
		srv.Close()
	}

	return cleanup, nil
}
