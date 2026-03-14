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
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/semistrict/loophole/internal/util"
)

const controlSecretHeader = "X-Control-Secret"

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "bg-exec":
			if err := clientBGExec(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
			return
		case "serve":
			// Explicit serve mode — fall through to run().
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
	}
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// clientBGExec starts a background command on a remote container-control server,
// polls for completion, and prints the output. Exits with the remote command's exit code.
//
// Usage: container-control bg-exec --url URL [--secret SECRET] [--poll INTERVAL] -- cmd arg1 arg2 ...
func clientBGExec(args []string) error {
	var baseURL, secret, pollInterval string
	var cmdArgv []string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--url":
			i++
			if i >= len(args) {
				return fmt.Errorf("--url requires a value")
			}
			baseURL = args[i]
		case "--secret":
			i++
			if i >= len(args) {
				return fmt.Errorf("--secret requires a value")
			}
			secret = args[i]
		case "--poll":
			i++
			if i >= len(args) {
				return fmt.Errorf("--poll requires a value")
			}
			pollInterval = args[i]
		case "--":
			cmdArgv = args[i+1:]
			i = len(args)
		default:
			// First non-flag arg starts the command.
			cmdArgv = args[i:]
			i = len(args)
		}
	}

	if baseURL == "" {
		baseURL = os.Getenv("CONTROL_URL")
	}
	if secret == "" {
		secret = os.Getenv("CONTROL_SECRET")
	}
	if baseURL == "" {
		return fmt.Errorf("--url or CONTROL_URL is required")
	}
	if len(cmdArgv) == 0 {
		return fmt.Errorf("no command specified")
	}

	poll := 2 * time.Second
	if pollInterval != "" {
		d, err := time.ParseDuration(pollInterval)
		if err != nil {
			return fmt.Errorf("invalid --poll value: %w", err)
		}
		poll = d
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// Start the job.
	argvJSON, err := json.Marshal(cmdArgv)
	if err != nil {
		return fmt.Errorf("marshal argv: %w", err)
	}
	// If the base URL already ends with a control path segment (e.g. worker proxy URL
	// like .../debug/control/{id}), don't add another /control/ prefix.
	pathPrefix := "/control/bg-exec"
	if strings.Contains(baseURL, "/control/") {
		pathPrefix = "/bg-exec"
	}
	startURL := fmt.Sprintf("%s%s?argv=%s", baseURL, pathPrefix, url.QueryEscape(string(argvJSON)))
	req, err := http.NewRequest(http.MethodPost, startURL, nil)
	if err != nil {
		return err
	}
	if secret != "" {
		req.Header.Set(controlSecretHeader, secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("start bg-exec: %w", err)
	}
	defer util.SafeClose(resp.Body, "close bg-exec start response body")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("start bg-exec: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var startResp struct {
		ID  string `json:"id"`
		PID int    `json:"pid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&startResp); err != nil {
		return fmt.Errorf("decode start response: %w", err)
	}
	fmt.Fprintf(os.Stderr, "bg-exec: started job %s (pid %d)\n", startResp.ID, startResp.PID)

	// Poll for completion.
	pollPrefix := "/control/bg-exec"
	if strings.Contains(baseURL, "/control/") {
		pollPrefix = "/bg-exec"
	}
	pollURL := fmt.Sprintf("%s%s/%s", baseURL, pollPrefix, startResp.ID)
	for {
		time.Sleep(poll)

		req, err := http.NewRequest(http.MethodGet, pollURL, nil)
		if err != nil {
			return err
		}
		if secret != "" {
			req.Header.Set(controlSecretHeader, secret)
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bg-exec: poll error: %v\n", err)
			continue
		}

		var pollResp struct {
			Done     bool   `json:"done"`
			ExitCode int    `json:"exitCode"`
			Output   string `json:"output"`
			Error    string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&pollResp); err != nil {
			util.SafeClose(resp.Body, "close bg-exec poll response body after decode failure")
			fmt.Fprintf(os.Stderr, "bg-exec: decode poll error: %v\n", err)
			continue
		}
		util.SafeClose(resp.Body, "close bg-exec poll response body")

		if !pollResp.Done {
			continue
		}

		// Done — print output and exit with the remote exit code.
		fmt.Print(pollResp.Output)
		if pollResp.Error != "" {
			fmt.Fprintf(os.Stderr, "bg-exec: error: %s\n", pollResp.Error)
		}
		os.Exit(pollResp.ExitCode)
	}
}

func run() error {
	listenAddr := envOr("CONTROL_LISTEN_ADDR", ":8080")
	proxyTarget := os.Getenv("CONTROL_PROXY_TARGET")
	proxy, err := buildProxy(proxyTarget)
	if err != nil {
		return fmt.Errorf("parse CONTROL_PROXY_TARGET: %w", err)
	}
	daemonUpstream, err := buildUpstream(proxyTarget)
	if err != nil {
		return fmt.Errorf("build CONTROL_PROXY_TARGET client: %w", err)
	}
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
		daemonProxy:     proxy,
		daemonAPI:       daemonUpstream,
		sandboxProxy:    sandboxProxy,
		sandboxAPI:      sandboxUpstream,
		logger:          log.Default(),
		bgJobs:          make(map[string]*bgJob),
		toolboxSessions: make(map[string]*toolboxSession),
	}

	s := &http.Server{
		Addr:              listenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("container-control listening on %s proxy=%q sandboxProxy=%q container=%s", listenAddr, proxyTarget, sandboxProxyTarget, srv.container)
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

type bgJob struct {
	id       string
	cmd      *exec.Cmd
	outFile  *os.File
	doneCh   chan struct{}
	exitCode int
	execErr  string
}

type controlServer struct {
	container    string
	proxyMu      sync.RWMutex
	daemonProxy  *httputil.ReverseProxy
	daemonAPI    *upstream
	sandboxProxy *httputil.ReverseProxy
	sandboxAPI   *upstream
	logger       *log.Logger

	bgMu            sync.Mutex
	bgJobs          map[string]*bgJob
	zygoteMu        sync.Mutex
	bgSeq           int
	toolboxMu       sync.Mutex
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
	daemonProxy := s.daemonProxy
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
	if daemonProxy == nil {
		http.Error(w, "proxy target not configured", http.StatusBadGateway)
		return
	}
	daemonProxy.ServeHTTP(w, r)
}

func (s *controlServer) handleControl(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/control/status":
		s.handleStatus(w, r)
	case "/control/exec":
		s.handleExec(w, r)
	case "/control/upload":
		s.handleUpload(w, r)
	case "/control/bg-exec":
		s.handleBGExec(w, r)
	case "/control/set-proxy":
		s.handleSetProxy(w, r)
	default:
		if strings.HasPrefix(r.URL.Path, "/control/bg-exec/") {
			s.handleBGExecJob(w, r)
			return
		}
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

func (s *controlServer) handleSetProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	target := r.URL.Query().Get("target")
	if target == "" {
		http.Error(w, "missing target query parameter (e.g. unix:///path/to.sock or http://host:port)", http.StatusBadRequest)
		return
	}

	proxy, err := buildProxy(target)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid target URL: %v", err), http.StatusBadRequest)
		return
	}

	s.proxyMu.Lock()
	s.daemonProxy = proxy
	s.proxyMu.Unlock()

	s.logger.Printf("proxy target set to %s", target)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "proxy": target})
}

func (s *controlServer) handleBGExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	argv := r.URL.Query()["argv"]
	if len(argv) == 0 {
		http.Error(w, "missing argv query parameter (JSON array or repeated argv=...)", http.StatusBadRequest)
		return
	}
	// If a single argv param is provided and looks like a JSON array, parse it.
	if len(argv) == 1 && strings.HasPrefix(argv[0], "[") {
		var parsed []string
		if err := json.Unmarshal([]byte(argv[0]), &parsed); err == nil && len(parsed) > 0 {
			argv = parsed
		}
	}
	if len(argv) == 0 || argv[0] == "" {
		http.Error(w, "empty argv", http.StatusBadRequest)
		return
	}

	cwd := r.URL.Query().Get("cwd")

	s.bgMu.Lock()
	s.bgSeq++
	id := fmt.Sprintf("bg-%d-%d", time.Now().Unix(), s.bgSeq)
	s.bgMu.Unlock()

	outFile, err := os.CreateTemp("", "bg-exec-*.log")
	if err != nil {
		http.Error(w, fmt.Sprintf("create output file: %v", err), http.StatusInternalServerError)
		return
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	cmd.Env = os.Environ()
	cmd.Stdout = outFile
	cmd.Stderr = outFile

	// Wire request body as stdin if present.
	if r.Body != nil && r.ContentLength != 0 {
		cmd.Stdin = r.Body
	}

	if err := cmd.Start(); err != nil {
		util.SafeClose(outFile, "close bg-exec output after start failure")
		_ = os.Remove(outFile.Name())
		http.Error(w, fmt.Sprintf("start command: %v", err), http.StatusInternalServerError)
		return
	}

	job := &bgJob{
		id:      id,
		cmd:     cmd,
		outFile: outFile,
		doneCh:  make(chan struct{}),
	}

	s.bgMu.Lock()
	s.bgJobs[id] = job
	s.bgMu.Unlock()

	go func() {
		defer close(job.doneCh)
		waitErr := cmd.Wait()
		util.SafeClose(outFile, "close bg-exec output file")
		exitCode, execErr := processExit(waitErr)
		job.exitCode = exitCode
		if execErr != nil {
			job.execErr = execErr.Error()
		}
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      id,
		"pid":     cmd.Process.Pid,
		"logFile": outFile.Name(),
	})
}

func (s *controlServer) handleBGExecJob(w http.ResponseWriter, r *http.Request) {
	// Parse /control/bg-exec/{id}[/kill]
	rest := strings.TrimPrefix(r.URL.Path, "/control/bg-exec/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	s.bgMu.Lock()
	job, ok := s.bgJobs[id]
	s.bgMu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no such job", "id": id})
		return
	}

	switch action {
	case "kill":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if job.cmd.Process != nil {
			_ = job.cmd.Process.Kill()
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "killed": true})

	case "":
		// GET: check status. If done, return output and reap.
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		select {
		case <-job.doneCh:
			// Done — read output, clean up, return everything.
			output, err := os.ReadFile(job.outFile.Name())
			if err != nil {
				output = []byte(fmt.Sprintf("<read error: %v>", err))
			}
			_ = os.Remove(job.outFile.Name())

			s.bgMu.Lock()
			delete(s.bgJobs, id)
			s.bgMu.Unlock()

			resp := map[string]any{
				"id":       id,
				"done":     true,
				"exitCode": job.exitCode,
				"output":   string(output),
			}
			if job.execErr != "" {
				resp["error"] = job.execErr
			}
			writeJSON(w, http.StatusOK, resp)

		default:
			// Still running.
			writeJSON(w, http.StatusOK, map[string]any{
				"id":   id,
				"done": false,
				"pid":  pidOf(job.cmd),
			})
		}

	default:
		http.NotFound(w, r)
	}
}

func pidOf(cmd *exec.Cmd) int {
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	return cmd.Process.Pid
}

func processExit(err error) (int, error) {
	if err == nil {
		return 0, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	if errors.Is(err, context.Canceled) {
		return 130, nil
	}
	return 0, err
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
