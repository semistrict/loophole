package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage2"
)

// EmbedSocketPath returns the UDS path for an embedded daemon running in the
// process with the given PID.
func EmbedSocketPath(pid int) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("loophole-%d.sock", pid))
}

// StartEmbedded starts a daemon HTTP server on a PID-derived UDS socket,
// reusing an existing VolumeManager. This is designed for the C API / embedded
// use case where loophole is linked into another process (e.g. Firecracker).
//
// The returned cleanup function stops the server and removes the socket.
// The server runs in background goroutines; it does not block.
func StartEmbedded(vm loophole.VolumeManager, diskCache *storage2.PageCache, inst loophole.Instance) (func(), error) {
	pid := os.Getpid()
	sockPath := EmbedSocketPath(pid)

	// Clean up stale socket from a previous run.
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("embed: remove stale socket", "path", sockPath, "error", err)
	}

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("embed: listen %s: %w", sockPath, err)
	}
	// Make socket world-accessible so the CLI can connect without root.
	if err := os.Chmod(sockPath, 0o666); err != nil {
		util.SafeClose(ln, "embed: close listener after chmod failure")
		return nil, fmt.Errorf("embed: chmod socket: %w", err)
	}

	backend := fsbackend.NewBackend(vm, nil)

	d := &Daemon{
		inst:       inst,
		backend:    backend,
		diskCache:  diskCache,
		ln:         ln,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())

	srv := &http.Server{Handler: d.instrument(d.mux(cancel))}

	// Serve in background.
	go func() {
		slog.Info("embed: daemon ready", "socket", sockPath, "pid", pid)
		if err := srv.Serve(ln); err != http.ErrServerClosed {
			slog.Error("embed: serve error", "error", err)
		}
	}()

	// Shutdown goroutine — mirrors the normal daemon's shutdown path.
	go func() {
		<-ctx.Done()
		close(d.shutdownCh)
		slog.Info("embed: shutdown start")
		close(d.doneCh)
		util.SafeClose(srv, "embed: close http server")
		util.SafeRun(func() error { return os.Remove(sockPath) }, "embed: remove socket")
		slog.Info("embed: shutdown complete")
	}()

	cleanup := func() {
		cancel()
	}

	return cleanup, nil
}
