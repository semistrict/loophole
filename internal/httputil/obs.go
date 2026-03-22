package httputil

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/semistrict/loophole/internal/env"
)

// ObsServer is a minimal HTTP server on a PID-named Unix socket that always
// exposes /metrics and pprof endpoints. Callers can add routes to the mux
// before or after the server starts serving (http.ServeMux is safe for
// concurrent registration).
type ObsServer struct {
	mux      *http.ServeMux
	ln       net.Listener
	srv      *http.Server
	sockPath string
	symlinks []string
	done     chan error
}

// StartObsServer opens a PID-named socket under dir and starts serving
// observability routes (/metrics, pprof) immediately.
func StartObsServer(dir env.Dir) (*ObsServer, error) {
	sockPath := dir.PidSocket()
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return nil, err
	}
	_ = os.Remove(sockPath) // clean stale socket from a previous run with same PID

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	RegisterObservabilityRoutes(mux)

	srv := &http.Server{Handler: mux}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	return &ObsServer{
		mux:      mux,
		ln:       ln,
		srv:      srv,
		sockPath: sockPath,
		done:     done,
	}, nil
}

// Mux returns the server's mux for adding routes.
func (s *ObsServer) Mux() *http.ServeMux { return s.mux }

// Listener returns the underlying listener.
func (s *ObsServer) Listener() net.Listener { return s.ln }

// SocketPath returns the PID-named socket path.
func (s *ObsServer) SocketPath() string { return s.sockPath }

// Symlink creates a symlink from target to the PID socket. The symlink is
// removed on Close. This allows volume-name-based discovery to find the
// PID-named socket.
func (s *ObsServer) Symlink(target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	_ = os.Remove(target)
	if err := os.Symlink(s.sockPath, target); err != nil {
		return err
	}
	s.symlinks = append(s.symlinks, target)
	return nil
}

// Close shuts down the HTTP server, removes the PID socket and all symlinks.
func (s *ObsServer) Close() {
	_ = s.srv.Close()
	<-s.done
	if err := os.Remove(s.sockPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove pid socket", "path", s.sockPath, "error", err)
	}
	for _, link := range s.symlinks {
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove socket symlink", "path", link, "error", err)
		}
	}
}
