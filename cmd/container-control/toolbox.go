package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/gorilla/websocket"

	"github.com/semistrict/loophole/internal/util"
)

type sandboxCreateRequest struct {
	Name       string            `json:"name,omitempty"`
	Source     map[string]any    `json:"source,omitempty"`
	Snapshot   string            `json:"snapshot,omitempty"`
	Volume     string            `json:"volume,omitempty"`
	Checkpoint string            `json:"checkpoint,omitempty"`
	SandboxID  string            `json:"sandboxId,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Entrypoint []string          `json:"entrypoint,omitempty"`
}

type daytonaProcessRequest struct {
	Command  string            `json:"command,omitempty"`
	Argv     []string          `json:"argv,omitempty"`
	Cwd      string            `json:"cwd,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
	Timeout  int               `json:"timeout,omitempty"`
	RunAsync bool              `json:"runAsync,omitempty"`
}

type toolboxSession struct {
	ID        string
	SandboxID string
	Cwd       string
	Env       map[string]string
	CreatedAt time.Time
	Commands  map[string]*toolboxCommand
	Order     []string
}

type toolboxCommand struct {
	ID        string
	ProcessID string
	Command   string
	Status    string
	ExitCode  *int
	Stdout    strings.Builder
	Stderr    strings.Builder
	Logs      strings.Builder
	CreatedAt time.Time
	StoppedAt *time.Time
	ws        *websocket.Conn
}

func (s *controlServer) handleSandboxAPI(w http.ResponseWriter, r *http.Request) {
	rest := r.URL.Path
	switch {
	case strings.HasPrefix(rest, "/api/sandboxes"):
		rest = strings.TrimPrefix(rest, "/api/sandboxes")
	case strings.HasPrefix(rest, "/api/sandbox"):
		rest = strings.TrimPrefix(rest, "/api/sandbox")
	default:
		rest = strings.TrimPrefix(rest, "/sandbox")
	}
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodGet:
			s.handleDaytonaListSandboxes(w, r)
		case http.MethodPost:
			s.handleDaytonaCreateSandbox(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	sandboxIDOrName, err := url.PathUnescape(parts[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.handleDaytonaGetSandbox(w, r, sandboxIDOrName)
		case http.MethodDelete:
			s.handleDaytonaDeleteSandbox(w, r, sandboxIDOrName)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch parts[1] {
	case "start":
		s.handleDaytonaStartSandbox(w, r, sandboxIDOrName)
	case "stop":
		s.handleDaytonaStopSandbox(w, r, sandboxIDOrName)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleToolboxAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "toolbox" || parts[2] != "toolbox" {
		http.NotFound(w, r)
		return
	}
	sandboxID, err := url.PathUnescape(parts[1])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch parts[3] {
	case "files":
		s.handleToolboxFiles(w, r, sandboxID, parts[4:])
	case "process":
		s.handleToolboxProcess(w, r, sandboxID, parts[4:])
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleDaytonaListSandboxes(w http.ResponseWriter, r *http.Request) {
	sandboxes, err := s.listSandboxes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, sandboxes)
}

func (s *controlServer) handleDaytonaCreateSandbox(w http.ResponseWriter, r *http.Request) {
	var req sandboxCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	source, err := s.daytonaSourceForCreate(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.maybeEnsureDefaultZygote(r.Context(), source); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	body, _ := json.Marshal(map[string]any{
		"name":       req.Name,
		"source":     source,
		"env":        req.Env,
		"cwd":        req.Cwd,
		"labels":     req.Labels,
		"entrypoint": req.Entrypoint,
	})
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes", body, "application/json")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close sandbox create response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleDaytonaGetSandbox(w http.ResponseWriter, r *http.Request, sandboxIDOrName string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxIDOrName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, sb)
}

func (s *controlServer) handleDaytonaDeleteSandbox(w http.ResponseWriter, r *http.Request, sandboxIDOrName string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxIDOrName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := s.deleteSandbox(r.Context(), sb.ID); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": sb.ID})
}

func (s *controlServer) handleDaytonaStartSandbox(w http.ResponseWriter, r *http.Request, sandboxIDOrName string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxIDOrName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	started, err := s.startSandbox(r.Context(), sb.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, started)
}

func (s *controlServer) handleDaytonaStopSandbox(w http.ResponseWriter, r *http.Request, sandboxIDOrName string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxIDOrName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sb.ID)+"/stop", nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close sandbox stop response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxFiles(w http.ResponseWriter, r *http.Request, sandboxID string, rest []string) {
	sandbox, err := s.lookupSandboxByIDOrName(r.Context(), sandboxID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	action := ""
	if len(rest) > 0 {
		action = rest[0]
	}
	switch {
	case action == "" && r.Method == http.MethodGet:
		s.handleToolboxListFiles(w, r, sandbox)
	case action == "" && r.Method == http.MethodDelete:
		s.handleToolboxDeleteFile(w, r, sandbox)
	case action == "info" && r.Method == http.MethodGet:
		s.handleToolboxFileInfo(w, r, sandbox)
	case action == "download" && r.Method == http.MethodGet:
		s.handleToolboxDownloadFile(w, r, sandbox)
	case action == "upload" && r.Method == http.MethodPost:
		s.handleToolboxUploadFile(w, r, sandbox)
	case action == "folder" && r.Method == http.MethodPost:
		s.handleToolboxCreateFolder(w, r, sandbox)
	case action == "move" && r.Method == http.MethodPost:
		s.handleToolboxMoveFile(w, r, sandbox)
	case action == "permissions" && r.Method == http.MethodPost:
		s.handleToolboxPermissions(w, r, sandbox)
	case action == "search" && r.Method == http.MethodGet:
		s.handleToolboxSearchFiles(w, r, sandbox)
	case action == "find" && r.Method == http.MethodGet:
		s.handleToolboxFindInFiles(w, r, sandbox)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleToolboxListFiles(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		reqPath = "/"
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/readdir?path="+url.QueryEscape(reqPath), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox readdir response body")
	if resp.StatusCode >= 400 {
		copyResponse(w, resp)
		return
	}

	var entries []struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Mode  uint32 `json:"mode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})

	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"name":  entry.Name,
			"isDir": entry.IsDir,
			"mode":  entry.Mode,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *controlServer) handleToolboxFileInfo(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/stat?path="+url.QueryEscape(reqPath), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox stat response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxDownloadFile(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/read?path="+url.QueryEscape(reqPath), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox read response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxUploadFile(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/write?path="+url.QueryEscape(reqPath), body, "application/octet-stream")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox write response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxCreateFolder(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/mkdir?path="+url.QueryEscape(reqPath), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox mkdir response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxDeleteFile(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	recursive := r.URL.Query().Get("recursive")
	query := url.Values{"path": []string{reqPath}}
	if recursive != "" {
		query.Set("recursive", recursive)
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandbox.ID)+"/fs/remove?"+query.Encode(), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox remove response body")
	copyResponse(w, resp)
}

func (s *controlServer) handleToolboxMoveFile(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	source := r.URL.Query().Get("source")
	destination := r.URL.Query().Get("destination")
	if source == "" || destination == "" {
		http.Error(w, "source and destination are required", http.StatusBadRequest)
		return
	}
	srcPath, err := securejoin.SecureJoin(sandbox.Mountpoint, source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dstPath, err := securejoin.SecureJoin(sandbox.Mountpoint, destination)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(srcPath, dstPath); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *controlServer) handleToolboxPermissions(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	reqPath := r.URL.Query().Get("path")
	modeText := r.URL.Query().Get("mode")
	if reqPath == "" || modeText == "" {
		http.Error(w, "path and mode are required", http.StatusBadRequest)
		return
	}
	target, err := securejoin.SecureJoin(sandbox.Mountpoint, reqPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	parsed, err := strconv.ParseUint(modeText, 8, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.Chmod(target, fs.FileMode(parsed)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *controlServer) handleToolboxSearchFiles(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	root := r.URL.Query().Get("path")
	if root == "" {
		root = "/"
	}
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		http.Error(w, "pattern is required", http.StatusBadRequest)
		return
	}
	base, err := securejoin.SecureJoin(sandbox.Mountpoint, root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	matches := make([]map[string]string, 0)
	err = filepath.WalkDir(base, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.Contains(d.Name(), pattern) {
			rel, relErr := filepath.Rel(sandbox.Mountpoint, current)
			if relErr != nil {
				return relErr
			}
			matches = append(matches, map[string]string{"path": "/" + filepath.ToSlash(rel)})
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, matches)
}

func (s *controlServer) handleToolboxFindInFiles(w http.ResponseWriter, r *http.Request, sandbox sandboxRecord) {
	root := r.URL.Query().Get("path")
	if root == "" {
		root = "/"
	}
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		http.Error(w, "pattern is required", http.StatusBadRequest)
		return
	}
	base, err := securejoin.SecureJoin(sandbox.Mountpoint, root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	matches := make([]map[string]any, 0)
	err = filepath.WalkDir(base, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		data, readErr := os.ReadFile(current)
		if readErr != nil || !strings.Contains(string(data), pattern) {
			return nil
		}
		rel, relErr := filepath.Rel(sandbox.Mountpoint, current)
		if relErr != nil {
			return relErr
		}
		matches = append(matches, map[string]any{
			"path":    "/" + filepath.ToSlash(rel),
			"pattern": pattern,
		})
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, matches)
}

func (s *controlServer) handleToolboxProcess(w http.ResponseWriter, r *http.Request, sandboxID string, rest []string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	sandboxID = sb.ID
	if len(rest) == 0 {
		http.NotFound(w, r)
		return
	}
	switch rest[0] {
	case "execute":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleToolboxExecute(w, r, sandboxID)
	case "terminal":
		s.handleToolboxTerminal(w, r, sandboxID)
	case "session":
		s.handleToolboxSession(w, r, sandboxID, rest[1:])
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleToolboxTerminal(w http.ResponseWriter, r *http.Request, sandboxID string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer util.SafeClose(clientConn, "close toolbox terminal client websocket")

	argv := r.URL.Query()["arg"]
	if len(argv) == 0 {
		argv = defaultTerminalArgv(sb)
	}
	rows, _ := strconv.Atoi(r.URL.Query().Get("rows"))
	cols, _ := strconv.Atoi(r.URL.Query().Get("cols"))
	body, _ := json.Marshal(map[string]any{
		"argv":       argv,
		"cwd":        r.URL.Query().Get("cwd"),
		"background": true,
		"tty":        true,
		"rows":       rows,
		"cols":       cols,
	})
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sb.ID)+"/processes", body, "application/json")
	if err != nil {
		_ = clientConn.WriteJSON(map[string]any{"type": "error", "error": err.Error()})
		return
	}
	defer util.SafeClose(resp.Body, "close toolbox terminal create response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		_ = clientConn.WriteJSON(map[string]any{"type": "error", "error": strings.TrimSpace(string(data))})
		return
	}
	var process struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&process); err != nil {
		_ = clientConn.WriteJSON(map[string]any{"type": "error", "error": err.Error()})
		return
	}
	defer func() {
		_ = s.signalProcess(context.Background(), sb.ID, process.ID, "TERM")
	}()

	upstreamConn, wsResp, err := s.sandboxAPI.dialWebsocket(r.Context(), "/v1/sandboxes/"+url.PathEscape(sb.ID)+"/processes/"+url.PathEscape(process.ID)+"/stream", nil)
	if wsResp != nil {
		util.SafeClose(wsResp.Body, "close toolbox terminal upstream response body")
	}
	if err != nil {
		_ = clientConn.WriteJSON(map[string]any{"type": "error", "error": err.Error()})
		return
	}
	defer util.SafeClose(upstreamConn, "close toolbox terminal upstream websocket")

	errCh := make(chan error, 2)

	go func() {
		for {
			msgType, data, err := clientConn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			switch msgType {
			case websocket.BinaryMessage:
				if len(data) == 0 {
					continue
				}
				if err := upstreamConn.WriteJSON(map[string]any{"stdin": string(data)}); err != nil {
					errCh <- err
					return
				}
			case websocket.TextMessage:
				var msg struct {
					Stdin  string `json:"stdin,omitempty"`
					Signal string `json:"signal,omitempty"`
					Rows   int    `json:"rows,omitempty"`
					Cols   int    `json:"cols,omitempty"`
				}
				if err := json.Unmarshal(data, &msg); err != nil {
					errCh <- err
					return
				}
				switch {
				case msg.Signal != "":
					if err := s.signalProcess(context.Background(), sb.ID, process.ID, msg.Signal); err != nil {
						errCh <- err
						return
					}
				case msg.Rows > 0 && msg.Cols > 0:
					if err := upstreamConn.WriteJSON(map[string]any{"rows": msg.Rows, "cols": msg.Cols}); err != nil {
						errCh <- err
						return
					}
				case msg.Stdin != "":
					if err := upstreamConn.WriteJSON(map[string]any{"stdin": msg.Stdin}); err != nil {
						errCh <- err
						return
					}
				}
			}
		}
	}()

	go func() {
		for {
			var event struct {
				Stream string `json:"stream"`
				Data   string `json:"data,omitempty"`
				Error  string `json:"error,omitempty"`
			}
			if err := upstreamConn.ReadJSON(&event); err != nil {
				errCh <- err
				return
			}
			switch event.Stream {
			case "stdout", "stderr":
				if err := clientConn.WriteMessage(websocket.BinaryMessage, []byte(event.Data)); err != nil {
					errCh <- err
					return
				}
			case "error":
				_ = clientConn.WriteJSON(map[string]any{"type": "error", "error": event.Error})
				errCh <- nil
				return
			case "exit":
				exitCode, _ := strconv.Atoi(strings.TrimSpace(event.Data))
				_ = clientConn.WriteJSON(map[string]any{"type": "closed", "exitCode": exitCode})
				errCh <- nil
				return
			}
		}
	}()

	if err := <-errCh; err != nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return
	}
}

func defaultTerminalArgv(sb sandboxRecord) []string {
	for _, candidate := range []string{"/usr/bin/bash", "/bin/bash", "/usr/bin/dash", "/bin/dash", "/bin/sh"} {
		hostPath := filepath.Join(sb.Mountpoint, strings.TrimPrefix(candidate, "/"))
		if _, err := os.Stat(hostPath); err == nil {
			return []string{candidate, "-i"}
		}
	}
	return []string{"/bin/sh", "-i"}
}

func (s *controlServer) handleToolboxExecute(w http.ResponseWriter, r *http.Request, sandboxID string) {
	sb, err := s.lookupSandboxByIDOrName(r.Context(), sandboxID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var req daytonaProcessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	response, err := s.executeProcess(r.Context(), sb.ID, req, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *controlServer) handleToolboxSession(w http.ResponseWriter, r *http.Request, sandboxID string, rest []string) {
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodGet:
			s.handleListSessions(w, r, sandboxID)
		case http.MethodPost:
			s.handleCreateSession(w, r, sandboxID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	sessionID, err := url.PathUnescape(rest[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(rest) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.handleGetSession(w, r, sandboxID, sessionID)
		case http.MethodDelete:
			s.handleDeleteSession(w, r, sandboxID, sessionID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	switch {
	case rest[1] == "exec" && r.Method == http.MethodPost:
		s.handleExecSessionCommand(w, r, sandboxID, sessionID)
	case len(rest) >= 4 && rest[1] == "command" && rest[3] == "input" && r.Method == http.MethodPost:
		commandID, decodeErr := url.PathUnescape(rest[2])
		if decodeErr != nil {
			http.Error(w, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		s.handleSessionCommandInput(w, r, sandboxID, sessionID, commandID)
	case len(rest) >= 4 && rest[1] == "command" && rest[3] == "logs" && r.Method == http.MethodGet:
		commandID, decodeErr := url.PathUnescape(rest[2])
		if decodeErr != nil {
			http.Error(w, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		s.handleSessionCommandLogs(w, r, sandboxID, sessionID, commandID)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleListSessions(w http.ResponseWriter, _ *http.Request, sandboxID string) {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	out := make([]map[string]any, 0)
	for _, session := range s.toolboxSessions {
		if session.SandboxID == sandboxID {
			out = append(out, s.sessionResponse(session))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *controlServer) handleCreateSession(w http.ResponseWriter, r *http.Request, sandboxID string) {
	if _, err := s.lookupSandboxByIDOrName(r.Context(), sandboxID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var req struct {
		SessionID string            `json:"sessionId,omitempty"`
		Cwd       string            `json:"cwd,omitempty"`
		Env       map[string]string `json:"env,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := &toolboxSession{
		ID:        firstNonEmpty(req.SessionID, s.newToolboxID("sess")),
		SandboxID: sandboxID,
		Cwd:       req.Cwd,
		Env:       req.Env,
		CreatedAt: time.Now().UTC(),
		Commands:  make(map[string]*toolboxCommand),
	}
	s.toolboxMu.Lock()
	s.toolboxSessions[session.ID] = session
	resp := s.sessionResponse(session)
	s.toolboxMu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

func (s *controlServer) handleGetSession(w http.ResponseWriter, _ *http.Request, sandboxID, sessionID string) {
	session, err := s.getSession(sandboxID, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	s.toolboxMu.Lock()
	resp := s.sessionResponse(session)
	s.toolboxMu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

func (s *controlServer) handleDeleteSession(w http.ResponseWriter, r *http.Request, sandboxID, sessionID string) {
	session, err := s.getSession(sandboxID, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	for _, commandID := range session.Order {
		command := session.Commands[commandID]
		if command == nil || command.Status != "running" {
			continue
		}
		_ = s.signalProcess(r.Context(), sandboxID, command.ProcessID, "KILL")
		if command.ws != nil {
			util.SafeClose(command.ws, "close toolbox command websocket on session delete")
		}
	}
	s.toolboxMu.Lock()
	delete(s.toolboxSessions, sessionID)
	s.toolboxMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "sessionId": sessionID})
}

func (s *controlServer) handleExecSessionCommand(w http.ResponseWriter, r *http.Request, sandboxID, sessionID string) {
	session, err := s.getSession(sandboxID, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var req daytonaProcessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Cwd == "" {
		req.Cwd = session.Cwd
	}
	if req.Env == nil {
		req.Env = session.Env
	}

	commandID := s.newToolboxID("cmd")
	command := &toolboxCommand{
		ID:        commandID,
		Command:   firstNonEmpty(req.Command, strings.Join(req.Argv, " ")),
		Status:    "running",
		CreatedAt: time.Now().UTC(),
	}

	s.toolboxMu.Lock()
	session.Commands[commandID] = command
	session.Order = append(session.Order, commandID)
	s.toolboxMu.Unlock()

	if req.RunAsync {
		processID, wsConn, err := s.startAsyncProcess(r.Context(), sandboxID, req)
		if err != nil {
			s.finishCommandWithError(session, commandID, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		s.toolboxMu.Lock()
		command.ProcessID = processID
		command.ws = wsConn
		s.toolboxMu.Unlock()
		go s.streamAsyncCommand(sessionID, commandID, wsConn)
		writeJSON(w, http.StatusAccepted, s.commandResponse(command))
		return
	}

	response, err := s.executeProcess(r.Context(), sandboxID, req, false)
	if err != nil {
		s.finishCommandWithError(session, commandID, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.finishCommandWithResult(session, commandID, response)
	writeJSON(w, http.StatusOK, response)
}

func (s *controlServer) handleSessionCommandInput(w http.ResponseWriter, r *http.Request, sandboxID, sessionID, commandID string) {
	command, err := s.getCommand(sandboxID, sessionID, commandID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if command.ws == nil {
		http.Error(w, "command does not accept input", http.StatusConflict)
		return
	}
	var req struct {
		Input string `json:"input,omitempty"`
		Stdin string `json:"stdin,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data := firstNonEmpty(req.Input, req.Stdin)
	if data == "" {
		http.Error(w, "input is required", http.StatusBadRequest)
		return
	}
	if err := command.ws.WriteJSON(map[string]any{"stdin": data}); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *controlServer) handleSessionCommandLogs(w http.ResponseWriter, _ *http.Request, sandboxID, sessionID, commandID string) {
	command, err := s.getCommand(sandboxID, sessionID, commandID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, command.Logs.String())
}

func (s *controlServer) executeProcess(ctx context.Context, sandboxID string, req daytonaProcessRequest, background bool) (map[string]any, error) {
	payload, err := processPayload(req, background)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(payload)
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandboxID)+"/processes", body, "application/json")
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(resp.Body, "close toolbox execute response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("execute process: status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *controlServer) startAsyncProcess(ctx context.Context, sandboxID string, req daytonaProcessRequest) (string, *websocket.Conn, error) {
	payload, err := processPayload(req, true)
	if err != nil {
		return "", nil, err
	}
	body, _ := json.Marshal(payload)
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandboxID)+"/processes", body, "application/json")
	if err != nil {
		return "", nil, err
	}
	defer util.SafeClose(resp.Body, "close async process create response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("create async process: status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var record struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		return "", nil, err
	}
	headers := http.Header{}
	conn, wsResp, err := s.sandboxAPI.dialWebsocket(ctx, "/v1/sandboxes/"+url.PathEscape(sandboxID)+"/processes/"+url.PathEscape(record.ID)+"/stream", headers)
	if wsResp != nil {
		util.SafeClose(wsResp.Body, "close async process websocket response body")
	}
	if err != nil {
		return "", nil, err
	}
	return record.ID, conn, nil
}

func (s *controlServer) streamAsyncCommand(sessionID, commandID string, conn *websocket.Conn) {
	defer util.SafeClose(conn, "close async toolbox websocket")
	for {
		var event struct {
			Stream string `json:"stream"`
			Data   string `json:"data,omitempty"`
			Error  string `json:"error,omitempty"`
		}
		if err := conn.ReadJSON(&event); err != nil {
			s.markCommandFailed(sessionID, commandID, err.Error())
			return
		}
		s.toolboxMu.Lock()
		session := s.toolboxSessions[sessionID]
		command := (*toolboxCommand)(nil)
		if session != nil {
			command = session.Commands[commandID]
		}
		if command == nil {
			s.toolboxMu.Unlock()
			return
		}
		switch event.Stream {
		case "stdout":
			command.Stdout.WriteString(event.Data)
			command.Logs.WriteString(event.Data)
		case "stderr":
			command.Stderr.WriteString(event.Data)
			command.Logs.WriteString(event.Data)
		case "error":
			command.Status = "error"
			command.Logs.WriteString(event.Error)
			now := time.Now().UTC()
			command.StoppedAt = &now
			command.ws = nil
			s.toolboxMu.Unlock()
			return
		case "exit":
			code, _ := strconv.Atoi(strings.TrimSpace(event.Data))
			command.ExitCode = &code
			command.Status = "done"
			now := time.Now().UTC()
			command.StoppedAt = &now
			command.ws = nil
			s.toolboxMu.Unlock()
			return
		}
		s.toolboxMu.Unlock()
	}
}

func (s *controlServer) finishCommandWithError(session *toolboxSession, commandID string, err error) {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	command := session.Commands[commandID]
	if command == nil {
		return
	}
	command.Status = "error"
	command.Logs.WriteString(err.Error())
	now := time.Now().UTC()
	command.StoppedAt = &now
}

func (s *controlServer) finishCommandWithResult(session *toolboxSession, commandID string, response map[string]any) {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	command := session.Commands[commandID]
	if command == nil {
		return
	}
	command.Status = "done"
	if stdout, ok := response["stdout"].(string); ok {
		command.Stdout.WriteString(stdout)
		command.Logs.WriteString(stdout)
	}
	if stderr, ok := response["stderr"].(string); ok {
		command.Stderr.WriteString(stderr)
		command.Logs.WriteString(stderr)
	}
	if exitCode, ok := response["exit_code"].(float64); ok {
		exit := int(exitCode)
		command.ExitCode = &exit
	}
	now := time.Now().UTC()
	command.StoppedAt = &now
}

func (s *controlServer) markCommandFailed(sessionID, commandID, message string) {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	session := s.toolboxSessions[sessionID]
	if session == nil {
		return
	}
	command := session.Commands[commandID]
	if command == nil {
		return
	}
	command.Status = "error"
	command.Logs.WriteString(message)
	now := time.Now().UTC()
	command.StoppedAt = &now
	command.ws = nil
}

func (s *controlServer) getSession(sandboxID, sessionID string) (*toolboxSession, error) {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	session := s.toolboxSessions[sessionID]
	if session == nil || session.SandboxID != sandboxID {
		return nil, fmt.Errorf("unknown session %q", sessionID)
	}
	return session, nil
}

func (s *controlServer) getCommand(sandboxID, sessionID, commandID string) (*toolboxCommand, error) {
	session, err := s.getSession(sandboxID, sessionID)
	if err != nil {
		return nil, err
	}
	command := session.Commands[commandID]
	if command == nil {
		return nil, fmt.Errorf("unknown command %q", commandID)
	}
	return command, nil
}

// sessionResponse returns a snapshot of the session state.
// Caller must hold s.toolboxMu.
func (s *controlServer) sessionResponse(session *toolboxSession) map[string]any {
	commands := make([]map[string]any, 0, len(session.Order))
	for _, id := range session.Order {
		command := session.Commands[id]
		if command != nil {
			commands = append(commands, commandResponseLocked(command))
		}
	}
	return map[string]any{
		"sessionId": session.ID,
		"sandboxId": session.SandboxID,
		"cwd":       session.Cwd,
		"env":       session.Env,
		"createdAt": session.CreatedAt,
		"commands":  commands,
	}
}

// commandResponse returns a snapshot of the command state.
// Caller must NOT hold s.toolboxMu — this method acquires it.
func (s *controlServer) commandResponse(command *toolboxCommand) map[string]any {
	s.toolboxMu.Lock()
	defer s.toolboxMu.Unlock()
	return commandResponseLocked(command)
}

// commandResponseLocked returns a snapshot of the command state.
// Caller must hold s.toolboxMu.
func commandResponseLocked(command *toolboxCommand) map[string]any {
	return map[string]any{
		"cmdId":     command.ID,
		"processId": command.ProcessID,
		"command":   command.Command,
		"status":    command.Status,
		"exitCode":  command.ExitCode,
		"stdout":    command.Stdout.String(),
		"stderr":    command.Stderr.String(),
		"createdAt": command.CreatedAt,
		"stoppedAt": command.StoppedAt,
	}
}

func (s *controlServer) daytonaSourceForCreate(ctx context.Context, req sandboxCreateRequest) (map[string]any, error) {
	if req.Source != nil {
		return req.Source, nil
	}
	defaultZygote := strings.TrimSpace(os.Getenv("LOOPHOLE_DEFAULT_ZYGOTE"))
	switch {
	case req.Volume != "":
		return map[string]any{"kind": "volume", "volume": req.Volume, "mode": "attach"}, nil
	case req.Checkpoint != "":
		return map[string]any{"kind": "checkpoint", "checkpoint": req.Checkpoint}, nil
	case req.SandboxID != "":
		return map[string]any{"kind": "sandbox", "sandbox_id": req.SandboxID}, nil
	case req.Snapshot != "":
		return map[string]any{"kind": "zygote", "zygote": req.Snapshot}, nil
	case req.Name != "":
		if defaultZygote != "" {
			return map[string]any{"kind": "zygote", "zygote": defaultZygote}, nil
		}
		if err := s.ensureVolumeExists(ctx, req.Name); err != nil {
			return nil, err
		}
		return map[string]any{"kind": "volume", "volume": req.Name, "mode": "attach"}, nil
	default:
		return nil, fmt.Errorf("source, snapshot, volume, checkpoint, sandboxId, or name is required")
	}
}

func (s *controlServer) ensureVolumeExists(ctx context.Context, volume string) error {
	err := s.cli.createVolume(ctx, map[string]any{"volume": volume})
	if err == nil {
		return nil
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "exist") || strings.Contains(message, "already") {
		return nil
	}
	return fmt.Errorf("ensure volume %q: %w", volume, err)
}

func (s *controlServer) lookupSandboxByIDOrName(ctx context.Context, sandboxIDOrName string) (sandboxRecord, error) {
	resp, err := s.sandboxAPI.do(ctx, http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sandboxIDOrName), nil, "")
	if err == nil {
		defer util.SafeClose(resp.Body, "close sandbox lookup response body")
		if resp.StatusCode < 400 {
			var sandbox sandboxRecord
			if decodeErr := json.NewDecoder(resp.Body).Decode(&sandbox); decodeErr != nil {
				return sandboxRecord{}, decodeErr
			}
			return sandbox, nil
		}
	}
	sandboxes, err := s.listSandboxes(ctx)
	if err != nil {
		return sandboxRecord{}, err
	}
	for _, sandbox := range sandboxes {
		if sandbox.ID == sandboxIDOrName || sandbox.Name == sandboxIDOrName {
			return sandbox, nil
		}
	}
	return sandboxRecord{}, fmt.Errorf("unknown sandbox %q", sandboxIDOrName)
}

func (s *controlServer) signalProcess(ctx context.Context, sandboxID, processID, signal string) error {
	body, _ := json.Marshal(map[string]string{"signal": signal})
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(sandboxID)+"/processes/"+url.PathEscape(processID)+"/signal", body, "application/json")
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close process signal response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("signal process: status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

func (s *controlServer) newToolboxID(prefix string) string {
	seq := s.toolboxSeq.Add(1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().Unix(), seq)
}

func processPayload(req daytonaProcessRequest, background bool) (map[string]any, error) {
	argv := req.Argv
	if len(argv) == 0 {
		if req.Command == "" {
			return nil, fmt.Errorf("command is required")
		}
		argv = []string{"/bin/sh", "-lc", req.Command}
	}
	return map[string]any{
		"argv":       argv,
		"cwd":        req.Cwd,
		"env":        req.Env,
		"background": background,
		"timeout":    req.Timeout,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
