package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/internal/util"
)

// ownerClient returns a client connected to the per-volume owner process.
func (s *controlServer) ownerClient(volume string) *client.Client {
	dir := loophole.DefaultDir()
	return client.NewFromSocket(dir.VolumeSocket(volume))
}

type sandboxSource struct {
	Kind       string `json:"kind"`
	Zygote     string `json:"zygote,omitempty"`
	Volume     string `json:"volume,omitempty"`
	Mode       string `json:"mode,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"`
	SandboxID  string `json:"sandbox_id,omitempty"`
}

type sandboxRecord struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	State        string        `json:"state"`
	Source       sandboxSource `json:"source"`
	RootfsVolume string        `json:"rootfs_volume"`
	Mountpoint   string        `json:"mountpoint"`
}

func (u *upstream) do(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	if u == nil {
		return nil, fmt.Errorf("upstream not configured")
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return u.client.Do(req)
}

func (s *controlServer) handleRuntime(w http.ResponseWriter, r *http.Request) bool {
	switch {
	case r.URL.Path == "/health":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return true
	case r.URL.Path == "/sandbox" || strings.HasPrefix(r.URL.Path, "/sandbox/"):
		s.handleSandboxAPI(w, r)
		return true
	case strings.HasPrefix(r.URL.Path, "/toolbox/"):
		s.handleToolboxAPI(w, r)
		return true
	case r.URL.Path == "/api/volumes" && r.Method == http.MethodGet:
		s.handleListVolumes(w, r)
		return true
	case r.URL.Path == "/api/volumes" && r.Method == http.MethodPost:
		s.handleCreateVolume(w, r)
		return true
	case strings.HasPrefix(r.URL.Path, "/api/volumes/"):
		s.handleVolumeAPI(w, r)
		return true
	case r.URL.Path == "/api/sandbox" && r.Method == http.MethodGet:
		s.handleDaytonaListSandboxes(w, r)
		return true
	case r.URL.Path == "/api/sandbox" && r.Method == http.MethodPost:
		s.handleDaytonaCreateSandbox(w, r)
		return true
	case strings.HasPrefix(r.URL.Path, "/api/sandbox/"):
		s.handleSandboxAPI(w, r)
		return true
	case strings.HasPrefix(r.URL.Path, "/v/"):
		s.handleVolumeRuntime(w, r)
		return true
	default:
		return false
	}
}

func (s *controlServer) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	volumes, err := s.cli.listVolumes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"volumes": volumes})
}

func (s *controlServer) handleCreateVolume(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.cli.createVolume(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	volume, _ := req["volume"].(string)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "volume": volume})
}

func (s *controlServer) handleVolumeAPI(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/volumes/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	volume, err := url.PathUnescape(parts[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch {
	case len(parts) == 1 && r.Method == http.MethodDelete:
		s.handleDeleteVolume(w, r, volume)
	case len(parts) == 2 && parts[1] == "checkpoint" && r.Method == http.MethodPost:
		s.handleCheckpointVolume(w, r, volume)
	case len(parts) == 2 && parts[1] == "clone" && r.Method == http.MethodPost:
		s.handleCloneVolume(w, r, volume)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleDeleteVolume(w http.ResponseWriter, r *http.Request, volume string) {
	sb, err := s.findVolumeSandbox(r.Context(), volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if sb != nil {
		if err := s.deleteSandbox(r.Context(), sb.ID); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	if err := s.cli.deleteVolume(r.Context(), volume); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "volume": volume})
}

func (s *controlServer) handleCheckpointVolume(w http.ResponseWriter, r *http.Request, volume string) {
	sb, err := s.ensureVolumeSandbox(r.Context(), volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	c := s.ownerClient(sb.RootfsVolume)
	cpID, err := c.Checkpoint(r.Context(), "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "checkpoint": cpID})
}

func (s *controlServer) handleCloneVolume(w http.ResponseWriter, r *http.Request, volume string) {
	sb, err := s.ensureVolumeSandbox(r.Context(), volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	var req struct {
		Clone string `json:"clone"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Clone == "" {
		http.Error(w, "clone is required", http.StatusBadRequest)
		return
	}

	c := s.ownerClient(sb.RootfsVolume)
	if err := c.Clone(r.Context(), client.CloneParams{Clone: req.Clone}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "volume": req.Clone})
}

func (s *controlServer) handleVolumeRuntime(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}

	volume, err := url.PathUnescape(parts[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch parts[1] {
	case "readdir":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleReadDir(w, r, volume)
	case "stat":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleStat(w, r, volume)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) handleReadDir(w http.ResponseWriter, r *http.Request, volume string) {
	sb, err := s.ensureVolumeSandbox(r.Context(), volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		path = "/"
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sb.ID)+"/fs/readdir?path="+url.QueryEscape(path), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close sandbox readdir response body")
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

	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"name":    entry.Name,
			"size":    0,
			"isDir":   entry.IsDir,
			"mode":    entry.Mode,
			"modTime": time.Time{},
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *controlServer) handleStat(w http.ResponseWriter, r *http.Request, volume string) {
	sb, err := s.ensureVolumeSandbox(r.Context(), volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	resp, err := s.sandboxAPI.do(r.Context(), http.MethodGet, "/v1/sandboxes/"+url.PathEscape(sb.ID)+"/fs/stat?path="+url.QueryEscape(path), nil, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer util.SafeClose(resp.Body, "close sandbox stat response body")
	if resp.StatusCode >= 400 {
		copyResponse(w, resp)
		return
	}

	var info struct {
		Name    string    `json:"name"`
		Size    int64     `json:"size"`
		Mode    uint32    `json:"mode"`
		IsDir   bool      `json:"is_dir"`
		ModTime time.Time `json:"mod_time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":    info.Name,
		"size":    info.Size,
		"isDir":   info.IsDir,
		"mode":    info.Mode,
		"modTime": info.ModTime,
	})
}

func (s *controlServer) listSandboxes(ctx context.Context) ([]sandboxRecord, error) {
	resp, err := s.sandboxAPI.do(ctx, http.MethodGet, "/v1/sandboxes", nil, "")
	if err != nil {
		return nil, err
	}
	defer util.SafeClose(resp.Body, "close sandbox list response body")
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list sandboxes: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var sandboxes []sandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&sandboxes); err != nil {
		return nil, err
	}
	return sandboxes, nil
}

func (s *controlServer) findVolumeSandbox(ctx context.Context, volume string) (*sandboxRecord, error) {
	sandboxes, err := s.listSandboxes(ctx)
	if err != nil {
		return nil, err
	}
	for _, sb := range sandboxes {
		if sb.Name == volume && sb.Source.Kind == "volume" && sb.RootfsVolume == volume {
			copy := sb
			return &copy, nil
		}
	}
	return nil, nil
}

func (s *controlServer) ensureVolumeSandbox(ctx context.Context, volume string) (sandboxRecord, error) {
	sb, err := s.findVolumeSandbox(ctx, volume)
	if err != nil {
		return sandboxRecord{}, err
	}
	if sb != nil {
		switch sb.State {
		case "running":
			return *sb, nil
		case "stopped":
			return s.startSandbox(ctx, sb.ID)
		default:
			if err := s.deleteSandbox(ctx, sb.ID); err != nil {
				return sandboxRecord{}, err
			}
		}
	}

	body, _ := json.Marshal(map[string]any{
		"name": volume,
		"source": map[string]any{
			"kind":   "volume",
			"volume": volume,
			"mode":   "attach",
		},
	})
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/sandboxes", body, "application/json")
	if err != nil {
		return sandboxRecord{}, err
	}
	defer util.SafeClose(resp.Body, "close sandbox create response body")
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return sandboxRecord{}, fmt.Errorf("create sandbox: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created sandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return sandboxRecord{}, err
	}
	return created, nil
}

func (s *controlServer) startSandbox(ctx context.Context, id string) (sandboxRecord, error) {
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(id)+"/start", nil, "")
	if err != nil {
		return sandboxRecord{}, err
	}
	defer util.SafeClose(resp.Body, "close sandbox start response body")
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return sandboxRecord{}, fmt.Errorf("start sandbox: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var started sandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&started); err != nil {
		return sandboxRecord{}, err
	}
	return started, nil
}

func (s *controlServer) deleteSandbox(ctx context.Context, id string) error {
	resp, err := s.sandboxAPI.do(ctx, http.MethodDelete, "/v1/sandboxes/"+url.PathEscape(id), nil, "")
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close sandbox delete response body")
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete sandbox: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
