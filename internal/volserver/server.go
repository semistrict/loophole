// Package volserver implements a pure volume-level HTTP server over a Unix
// domain socket. It is a direct remoting layer for *storage.Volume with no
// filesystem dependency, making it suitable for both the full fsserver
// (which adds FUSE/mount endpoints on top) and the C API (which needs
// volume management without any filesystem stack).
package volserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/semistrict/loophole/internal/httputil"
	"github.com/semistrict/loophole/internal/metrics"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/util"
)

// Server serves volume-level HTTP endpoints over a Unix socket.
// It is a pure remoting layer for a single *storage.Volume.
type Server struct {
	vol    *storage.Volume
	socket string
	ln     net.Listener

	shutdownCh chan struct{} // closed when shutdown begins
	doneCh     chan struct{} // closed when cleanup is complete
}

// New creates a headless Server (no listener) for embedding into fsserver.
// The returned Server can register its handlers on an external mux.
func New(vol *storage.Volume) *Server {
	return &Server{
		vol:        vol,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Start creates a volserver listening on the given Unix socket path.
// The caller must call Serve to begin handling requests, or Close to clean up.
func Start(vol *storage.Volume, socketPath string) (*Server, error) {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("volserver: remove stale socket", "path", socketPath, "error", err)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("volserver: listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o666); err != nil {
		util.SafeClose(ln, "volserver: close listener after chmod failure")
		return nil, fmt.Errorf("volserver: chmod socket: %w", err)
	}

	return &Server{
		vol:        vol,
		socket:     socketPath,
		ln:         ln,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}, nil
}

// Serve blocks, handling HTTP requests until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	srv := &http.Server{Handler: s.instrument(s.Mux(cancel))}

	go func() {
		<-ctx.Done()
		close(s.shutdownCh)
		slog.Info("volserver: shutdown start")

		if s.vol != nil {
			slog.Info("volserver: flushing volume")
			if err := s.vol.Flush(); err != nil {
				slog.Warn("volserver: flush error", "error", err)
			}
			slog.Info("volserver: releasing ref")
			if err := s.vol.ReleaseRef(); err != nil {
				slog.Warn("volserver: release ref error", "error", err)
			}
		}

		close(s.doneCh)

		util.SafeClose(srv, "volserver: close http server")
		util.SafeRun(func() error { return os.Remove(s.socket) }, "volserver: remove socket")
		slog.Info("volserver: shutdown complete")
	}()

	slog.Info("volserver: ready", "socket", s.socket)
	err := srv.Serve(s.ln)
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}

// Close triggers a graceful shutdown and waits for completion.
func (s *Server) Close() {
	select {
	case <-s.shutdownCh:
	default:
		if s.ln != nil {
			util.SafeClose(s.ln, "volserver: close listener for shutdown")
		}
	}
	<-s.doneCh
}

// Socket returns the Unix socket path.
func (s *Server) Socket() string { return s.socket }

// Volume returns the managed volume (may be nil).
func (s *Server) Volume() *storage.Volume { return s.vol }

// ShuttingDown reports whether the server is shutting down.
func (s *Server) ShuttingDown() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

// DoneCh returns a channel that is closed when shutdown cleanup is complete.
func (s *Server) DoneCh() <-chan struct{} { return s.doneCh }

// Mux returns an http.ServeMux with all volume-level routes registered.
func (s *Server) Mux(stop context.CancelFunc) *http.ServeMux {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, stop)
	return mux
}

// RegisterRoutes registers all volume-level routes on the given mux.
// All volume operations live under /device/.
func (s *Server) RegisterRoutes(mux *http.ServeMux, stop context.CancelFunc) {
	mux.HandleFunc("GET /status", s.HandleStatus)
	mux.HandleFunc("GET /debug/volume", s.HandleDebugVolume)
	mux.HandleFunc("POST /device/flush", s.HandleFlush)
	mux.HandleFunc("POST /device/checkpoint", s.HandleCheckpoint)
	mux.HandleFunc("POST /device/dd/write", s.HandleDeviceDDWrite)
	mux.HandleFunc("GET /device/dd/read", s.HandleDeviceDDRead)
	mux.HandleFunc("GET /device/dd/size", s.HandleDeviceDDSize)
	mux.HandleFunc("POST /device/dd/finalize", s.HandleDeviceDDFinalize)

	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("shutdown requested")
		WriteJSON(w, map[string]string{"status": "shutting_down"})
		stop()
	})
	mux.HandleFunc("GET /shutdown/wait", func(w http.ResponseWriter, r *http.Request) {
		<-s.doneCh
		WriteJSON(w, map[string]string{"status": "done"})
	})

	httputil.RegisterObservabilityRoutes(mux)
}

// --- Exported handlers (used by fsserver to wire individually) ---

func (s *Server) RequireVolume(w http.ResponseWriter) bool {
	if s.vol == nil {
		WriteError(w, 400, fmt.Errorf("no volume managed"))
		return true
	}
	return false
}

func (s *Server) HandleStatus(w http.ResponseWriter, r *http.Request) {
	state := "running"
	if s.ShuttingDown() {
		select {
		case <-s.doneCh:
			state = "stopped"
		default:
			state = "shutting_down"
		}
	}
	status := map[string]any{
		"state":  state,
		"mode":   "volume",
		"socket": s.socket,
	}
	if s.vol != nil {
		status["volume"] = s.vol.Name()
	}
	WriteJSON(w, status)
}

func (s *Server) HandleFlush(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}
	slog.Info("flush", "volume", s.vol.Name())
	if err := s.vol.Flush(); err != nil {
		WriteError(w, 500, fmt.Errorf("flush: %w", err))
		return
	}
	WriteJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) HandleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}
	slog.Info("checkpoint", "volume", s.vol.Name())
	cpID, err := s.vol.Checkpoint()
	if err != nil {
		slog.Error("checkpoint failed", "err", err)
		WriteError(w, 500, err)
		return
	}
	WriteJSON(w, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (s *Server) HandleDebugVolume(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}
	WriteJSON(w, s.vol.DebugInfo())
}

func (s *Server) HandleDeviceDDWrite(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		http.Error(w, "offset missing", 400)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset: "+err.Error(), 400)
		return
	}

	buf := make([]byte, storage.BlockSize)
	n, readErr := io.ReadFull(r.Body, buf)
	if n > 0 {
		if err := s.vol.Write(buf[:n], offset); err != nil {
			slog.Error("device/dd/write: write failed", "volume", s.vol.Name(), "offset", offset, "error", err)
			http.Error(w, "write failed: "+err.Error(), 500)
			return
		}
	}
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		slog.Error("device/dd/write: read body failed", "error", readErr)
		http.Error(w, "read body failed: "+readErr.Error(), 500)
		return
	}

	w.WriteHeader(204)
}

func (s *Server) HandleDeviceDDRead(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	sizeStr := r.URL.Query().Get("size")
	if offsetStr == "" || sizeStr == "" {
		http.Error(w, "offset or size missing", 400)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset: "+err.Error(), 400)
		return
	}
	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid size: "+err.Error(), 400)
		return
	}

	const maxReadSize = storage.BlockSize
	if size > maxReadSize {
		http.Error(w, "size exceeds maximum ("+strconv.FormatUint(maxReadSize, 10)+")", 400)
		return
	}

	buf := make([]byte, size)
	n, err := s.vol.Read(r.Context(), buf, offset)
	if err != nil {
		slog.Error("device/dd/read: read failed", "volume", s.vol.Name(), "offset", offset, "error", err)
		http.Error(w, "read failed: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(200)
	_, _ = w.Write(buf[:n])
}

func (s *Server) HandleDeviceDDSize(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}
	WriteJSON(w, map[string]uint64{"size": s.vol.Size()})
}

func (s *Server) HandleDeviceDDFinalize(w http.ResponseWriter, r *http.Request) {
	if s.RequireVolume(w) {
		return
	}

	if err := s.vol.Flush(); err != nil {
		slog.Error("device/dd/finalize: flush failed", "volume", s.vol.Name(), "error", err)
		http.Error(w, "flush failed: "+err.Error(), 500)
		return
	}

	if err := s.vol.ReleaseRef(); err != nil {
		slog.Warn("device/dd/finalize: release ref", "volume", s.vol.Name(), "error", err)
	}

	slog.Info("device/dd/finalize: complete", "volume", s.vol.Name())
	w.WriteHeader(204)
}

// --- HTTP helpers (exported so fsserver can reuse them) ---

func ReadJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("decode request: %w", err)
	}
	return nil
}

func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("writeJSON encode error", "error", err)
	}
}

func WriteError(w http.ResponseWriter, code int, err error) {
	slog.Warn("returning http error", "code", code, "err", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if encErr := json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); encErr != nil {
		slog.Warn("writeError encode error", "error", encErr)
	}
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || len(r.URL.Path) >= 6 && r.URL.Path[:6] == "/debug" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(rw, r)
		dur := time.Since(start)

		code := strconv.Itoa(rw.code)
		metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, code).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur.Seconds())

		level := slog.LevelInfo
		if rw.code >= 500 {
			level = slog.LevelWarn
		}

		slog.Log(r.Context(), level, "http", "method", r.Method, "path", r.URL.Path, "status", rw.code, "dur", dur)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
