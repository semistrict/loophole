package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/semistrict/loophole/internal/util"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "serve" {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	listenAddr := envOr("CONTROL_LISTEN_ADDR", ":8080")
	sandboxProxyTarget := os.Getenv("CONTROL_SANDBOXD_TARGET")
	sandboxProxy, err := buildProxy(sandboxProxyTarget)
	if err != nil {
		return fmt.Errorf("parse CONTROL_SANDBOXD_TARGET: %w", err)
	}
	sandboxUpstream, err := buildUpstream(sandboxProxyTarget)
	if err != nil {
		return fmt.Errorf("build CONTROL_SANDBOXD_TARGET client: %w", err)
	}
	go reapChildren()

	srv := &controlServer{
		container:       envOrMany([]string{"CONTROL_CONTAINER_ID", "CONTAINER_DO_ID"}, "unknown"),
		sandboxProxy:    sandboxProxy,
		sandboxAPI:      sandboxUpstream,
		cli:             newLoopholeCLI(),
		logger:          log.Default(),
		toolboxSessions: make(map[string]*toolboxSession),
	}

	s := &http.Server{
		Addr:              listenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("container-control listening on %s sandboxProxy=%q container=%s", listenAddr, sandboxProxyTarget, srv.container)
	return s.ListenAndServe()
}

func buildProxy(target string) (*httputil.ReverseProxy, error) {
	if target == "" {
		return nil, nil
	}
	if strings.HasPrefix(target, "unix://") {
		sockPath := strings.TrimPrefix(target, "unix://")
		return &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = "http"
				req.URL.Host = "localhost"
			},
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", sockPath)
				},
			},
		}, nil
	}
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(targetURL), nil
}

type upstream struct {
	baseURL     string
	wsURL       string
	client      *http.Client
	dialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}

func buildUpstream(target string) (*upstream, error) {
	if target == "" {
		return nil, nil
	}
	if strings.HasPrefix(target, "unix://") {
		sockPath := strings.TrimPrefix(target, "unix://")
		dialContext := func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", sockPath)
		}
		return &upstream{
			baseURL: "http://localhost",
			wsURL:   "ws://localhost",
			client: &http.Client{
				Transport: &http.Transport{
					DialContext: dialContext,
				},
			},
			dialContext: dialContext,
		}, nil
	}
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	wsScheme := "ws"
	if targetURL.Scheme == "https" {
		wsScheme = "wss"
	}
	return &upstream{
		baseURL: strings.TrimRight(targetURL.String(), "/"),
		wsURL:   wsScheme + "://" + targetURL.Host,
		client:  &http.Client{Transport: &http.Transport{}},
	}, nil
}

func (u *upstream) dialWebsocket(ctx context.Context, path string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if u == nil {
		return nil, nil, fmt.Errorf("upstream not configured")
	}
	dialer := websocket.Dialer{}
	if u.dialContext != nil {
		dialer.NetDialContext = u.dialContext
	}
	return dialer.DialContext(ctx, u.wsURL+path, headers)
}

type controlServer struct {
	container    string
	proxyMu      sync.RWMutex
	sandboxProxy *httputil.ReverseProxy
	sandboxAPI   *upstream
	cli          *loopholeCLI
	logger       *log.Logger

	zygoteMu        sync.Mutex
	toolboxMu       sync.Mutex
	toolboxSeq      atomic.Int64
	toolboxSessions map[string]*toolboxSession
}

func (s *controlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/control/") {
		s.handleControl(w, r)
		return
	}

	if s.handleRuntime(w, r) {
		return
	}

	s.proxyMu.RLock()
	sandboxProxy := s.sandboxProxy
	s.proxyMu.RUnlock()
	if strings.HasPrefix(r.URL.Path, "/sandboxd/") {
		if sandboxProxy == nil {
			http.Error(w, "sandbox proxy target not configured", http.StatusBadGateway)
			return
		}
		req := r.Clone(r.Context())
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/sandboxd")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		if req.URL.RawPath != "" {
			req.URL.RawPath = strings.TrimPrefix(req.URL.RawPath, "/sandboxd")
			if req.URL.RawPath == "" {
				req.URL.RawPath = "/"
			}
		}
		sandboxProxy.ServeHTTP(w, req)
		return
	}
	http.NotFound(w, r)
}

func (s *controlServer) handleControl(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/control/status":
		s.handleStatus(w, r)
	case "/control/exec":
		s.handleExec(w, r)
	case "/control/upload":
		s.handleUpload(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	hostname, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"container": s.container,
		"hostname":  hostname,
		"pid":       os.Getpid(),
	})
}

func (s *controlServer) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cmdText := r.URL.Query().Get("cmd")
	if cmdText == "" {
		http.Error(w, "missing cmd", http.StatusBadRequest)
		return
	}
	cwd := r.URL.Query().Get("cwd")
	timeout := 5 * time.Minute
	if timeoutText := r.URL.Query().Get("timeout"); timeoutText != "" {
		d, err := time.ParseDuration(timeoutText)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid timeout: %v", err), http.StatusBadRequest)
			return
		}
		timeout = d
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "/bin/sh", "-lc", cmdText)
	if cwd != "" {
		cmd.Dir = cwd
	}
	stdout, stderr, exitCode, err := runCommand(cmd)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"exitCode": exitCode,
		"stdout":   stdout,
		"stderr":   stderr,
		"timedOut": errors.Is(ctx.Err(), context.DeadlineExceeded),
	})
}

func (s *controlServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dstPath := r.URL.Query().Get("path")
	if dstPath == "" {
		http.Error(w, "missing path", http.StatusBadRequest)
		return
	}

	mode := os.FileMode(0o644)
	if modeText := r.URL.Query().Get("mode"); modeText != "" {
		parsed, err := strconv.ParseUint(modeText, 8, 32)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid mode: %v", err), http.StatusBadRequest)
			return
		}
		mode = os.FileMode(parsed)
	}

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		http.Error(w, fmt.Sprintf("mkdir parent: %v", err), http.StatusInternalServerError)
		return
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(dstPath), "."+filepath.Base(dstPath)+".tmp-*")
	if err != nil {
		http.Error(w, fmt.Sprintf("create temp file: %v", err), http.StatusInternalServerError)
		return
	}
	tmpPath := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			util.SafeClose(tmpFile, "close temp upload file")
		}
		if _, err := os.Stat(tmpPath); err == nil {
			_ = os.Remove(tmpPath)
		}
	}()

	written, err := io.Copy(tmpFile, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("write upload: %v", err), http.StatusInternalServerError)
		return
	}
	if err := tmpFile.Chmod(mode); err != nil {
		http.Error(w, fmt.Sprintf("chmod temp upload: %v", err), http.StatusInternalServerError)
		return
	}
	if err := tmpFile.Close(); err != nil {
		http.Error(w, fmt.Sprintf("close temp upload: %v", err), http.StatusInternalServerError)
		return
	}
	tmpFile = nil
	if err := os.Rename(tmpPath, dstPath); err != nil {
		http.Error(w, fmt.Sprintf("rename upload: %v", err), http.StatusInternalServerError)
		return
	}
	tmpPath = ""

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"path":  dstPath,
		"bytes": written,
		"mode":  fmt.Sprintf("%#o", mode),
	})
}

func runCommand(cmd *exec.Cmd) (stdout string, stderr string, exitCode int, err error) {
	var outBuf strings.Builder
	var errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return outBuf.String(), errBuf.String(), exitErr.ExitCode(), nil
		}
		return "", "", 0, err
	}
	return outBuf.String(), errBuf.String(), 0, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrMany(keys []string, fallback string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return fallback
}

func reapChildren() {
	for {
		var status syscall.WaitStatus
		_, err := syscall.Wait4(-1, &status, 0, nil)
		if err != nil {
			if errors.Is(err, syscall.ECHILD) {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}
