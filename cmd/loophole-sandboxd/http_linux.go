//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/gorilla/websocket"

	"github.com/semistrict/loophole/internal/util"
)

func (d *daemon) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", d.handleStatus)
	mux.HandleFunc("GET /v1/zygotes", d.handleListZygotes)
	mux.HandleFunc("POST /v1/zygotes", d.handleRegisterZygote)
	mux.HandleFunc("/v1/zygotes/", d.handleZygote)
	mux.HandleFunc("GET /v1/sandboxes", d.handleListSandboxes)
	mux.HandleFunc("POST /v1/sandboxes", d.handleCreateSandbox)
	mux.HandleFunc("/v1/sandboxes/", d.handleSandbox)
	return mux
}

func (d *daemon) handleStatus(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	platform := d.runscPlatform
	platformSource := d.runscPlatformSource
	if platform == "" {
		platform = defaultRunscPlatform
	}
	if platformSource == "" {
		platformSource = "bundled runsc default"
	}
	status := statusResponse{
		OK:                  true,
		RunscBin:            d.runscBin,
		RunscPlatform:       platform,
		RunscPlatformSource: platformSource,
		RunscDebug:          d.runscDebug,
		RunscRootless:       d.runscRootless,
		RunscRoot:           d.runscRoot,
		SandboxCount:        len(d.sandboxes),
		ZygoteCount:         len(d.zygotes),
	}
	d.mu.Unlock()
	writeJSON(w, status)
}

func (d *daemon) handleListZygotes(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	writeJSON(w, sortedZygotes(d.zygotes))
}

func (d *daemon) handleRegisterZygote(w http.ResponseWriter, r *http.Request) {
	var req registerZygoteRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := d.validateZygoteSource(r.Context(), req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	record := zygoteRecord{
		Name:       req.Name,
		Volume:     req.Volume,
		Checkpoint: req.Checkpoint,
		CreatedAt:  time.Now().UTC(),
	}
	d.mu.Lock()
	d.zygotes[record.Name] = record
	err := d.saveLocked()
	d.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, record)
}

func (d *daemon) handleZygote(w http.ResponseWriter, r *http.Request) {
	name := path.Base(r.URL.Path)
	d.mu.Lock()
	defer d.mu.Unlock()
	record, ok := d.zygotes[name]
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown zygote %q", name))
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, record)
	case http.MethodDelete:
		delete(d.zygotes, name)
		if err := d.saveLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"deleted": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (d *daemon) handleListSandboxes(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	writeJSON(w, sortedSandboxes(d.sandboxes))
}

func (d *daemon) handleCreateSandbox(w http.ResponseWriter, r *http.Request) {
	var req createSandboxRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sb, err := d.createSandbox(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, sb)
}

func (d *daemon) handleSandbox(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("sandbox id required"))
		return
	}
	id := parts[0]
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			sb, err := d.lookupSandbox(id)
			if err != nil {
				writeError(w, http.StatusNotFound, err)
				return
			}
			writeJSON(w, sb)
		case http.MethodDelete:
			if err := d.deleteSandbox(r.Context(), id); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, map[string]any{"deleted": true})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	switch parts[1] {
	case "start":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		sb, err := d.startSandbox(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, sb)
	case "stop":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		sb, err := d.stopSandbox(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, sb)
	case "processes":
		d.handleProcesses(w, r, id, parts[2:])
	case "fs":
		d.handleFS(w, r, id, parts[2:])
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown sandbox route"))
	}
}

func (d *daemon) handleProcesses(w http.ResponseWriter, r *http.Request, sandboxID string, rest []string) {
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodGet:
			d.mu.Lock()
			var out []processRecord
			for _, s := range d.sessions {
				if s.sandbox == sandboxID {
					out = append(out, s.record)
				}
			}
			d.mu.Unlock()
			writeJSON(w, out)
		case http.MethodPost:
			var req processCreateRequest
			if err := readJSON(r, &req); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			if req.Background || req.TTY {
				record, err := d.createSession(r.Context(), sandboxID, req)
				if err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				writeJSON(w, record)
				return
			}
			ctx := r.Context()
			if req.Timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
				defer cancel()
			}
			record, result, err := d.execProcess(ctx, sandboxID, req)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, map[string]any{
				"process":   record,
				"exit_code": result.ExitCode,
				"stdout":    result.Stdout,
				"stderr":    result.Stderr,
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	processID := rest[0]
	if len(rest) == 1 {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		s, ok := d.sessions[processID]
		if !ok || s.sandbox != sandboxID {
			writeError(w, http.StatusNotFound, fmt.Errorf("unknown process %q", processID))
			return
		}
		writeJSON(w, s.record)
		return
	}

	switch rest[1] {
	case "signal":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Signal string `json:"signal"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if req.Signal == "" {
			req.Signal = "TERM"
		}
		if err := d.signalSession(processID, req.Signal); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	case "stream":
		d.handleProcessStream(w, r, sandboxID, processID)
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown process route"))
	}
}

func (d *daemon) handleProcessStream(w http.ResponseWriter, r *http.Request, sandboxID, processID string) {
	d.mu.Lock()
	s, ok := d.sessions[processID]
	if ok && s.sandbox == sandboxID {
		if s.streamAttached {
			d.mu.Unlock()
			writeError(w, http.StatusConflict, fmt.Errorf("process %q already has an attached stream", processID))
			return
		}
		s.streamAttached = true
		d.sessions[processID] = s
	}
	d.mu.Unlock()
	if !ok || s.sandbox != sandboxID {
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown process %q", processID))
		return
	}
	defer func() {
		d.mu.Lock()
		current := d.sessions[processID]
		if current != nil {
			current.streamAttached = false
			d.sessions[processID] = current
		}
		d.mu.Unlock()
	}()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer util.SafeClose(conn, "close sandboxd websocket")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range s.cmd.events {
			payload := map[string]any{"stream": event.Stream}
			if len(event.Data) > 0 {
				payload["data"] = string(event.Data)
			}
			if event.Error != "" {
				payload["error"] = event.Error
			}
			if err := conn.WriteJSON(payload); err != nil {
				return
			}
			if event.Stream == "exit" {
				return
			}
		}
	}()

	for {
		var msg struct {
			Stdin string `json:"stdin"`
			Rows  int    `json:"rows,omitempty"`
			Cols  int    `json:"cols,omitempty"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			<-done
			return
		}
		if msg.Rows > 0 && msg.Cols > 0 {
			if err := d.resizeSession(processID, msg.Rows, msg.Cols); err != nil {
				return
			}
		}
		if s.cmd != nil && s.cmd.stdin != nil && msg.Stdin != "" {
			if _, err := io.WriteString(s.cmd.stdin, msg.Stdin); err != nil {
				return
			}
		}
	}
}

func (d *daemon) handleFS(w http.ResponseWriter, r *http.Request, sandboxID string, _ []string) {
	sb, err := d.lookupSandbox(sandboxID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		reqPath = "/"
	}
	target, err := securejoin.SecureJoin(sb.Mountpoint, reqPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	switch {
	case strings.HasSuffix(r.URL.Path, "/stat"):
		fi, err := os.Stat(target)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, map[string]any{
			"name":     fi.Name(),
			"size":     fi.Size(),
			"mode":     fi.Mode(),
			"is_dir":   fi.IsDir(),
			"mod_time": fi.ModTime(),
		})
	case strings.HasSuffix(r.URL.Path, "/readdir"):
		entries, err := os.ReadDir(target)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		type entry struct {
			Name  string      `json:"name"`
			IsDir bool        `json:"is_dir"`
			Mode  fs.FileMode `json:"mode"`
		}
		out := make([]entry, 0, len(entries))
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				continue
			}
			out = append(out, entry{Name: e.Name(), IsDir: e.IsDir(), Mode: info.Mode()})
		}
		writeJSON(w, out)
	case strings.HasSuffix(r.URL.Path, "/read"):
		data, err := os.ReadFile(target)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	case strings.HasSuffix(r.URL.Path, "/write"):
		data, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	case strings.HasSuffix(r.URL.Path, "/mkdir"):
		if err := os.MkdirAll(target, 0o755); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	case strings.HasSuffix(r.URL.Path, "/remove"):
		recursive := r.URL.Query().Get("recursive") == "1" || r.URL.Query().Get("recursive") == "true"
		if recursive {
			err = os.RemoveAll(target)
		} else {
			err = os.Remove(target)
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown fs route"))
	}
}

func (d *daemon) createSandbox(ctx context.Context, req createSandboxRequest) (sandboxRecord, error) {
	if req.Source.Kind == "" {
		return sandboxRecord{}, fmt.Errorf("source.kind is required")
	}
	id := newID("sbx")
	name := req.Name
	if name == "" {
		name = id
	}
	rootfsVolume := id
	derived := true

	switch req.Source.Kind {
	case sourceKindVolume:
		if req.Source.Volume == "" {
			return sandboxRecord{}, fmt.Errorf("source.volume is required")
		}
		if req.Source.Mode == "attach" {
			d.mu.Lock()
			for _, existing := range d.sandboxes {
				if existing.RootfsVolume == req.Source.Volume {
					d.mu.Unlock()
					return sandboxRecord{}, fmt.Errorf("volume %q is already attached to sandbox %q", req.Source.Volume, existing.ID)
				}
			}
			d.mu.Unlock()
			rootfsVolume = req.Source.Volume
			derived = false
		} else {
			if err := d.cloneVolume(ctx, req.Source, rootfsVolume); err != nil {
				return sandboxRecord{}, err
			}
		}
	case sourceKindCheckpoint, sourceKindZygote, sourceKindSandbox:
		if err := d.cloneVolume(ctx, req.Source, rootfsVolume); err != nil {
			return sandboxRecord{}, err
		}
	default:
		return sandboxRecord{}, fmt.Errorf("unsupported source kind %q", req.Source.Kind)
	}

	mountpoint := d.mountDir(id)
	adopt := req.Source.Kind == sourceKindVolume && req.Source.Mode == "attach"
	owner, err := d.ensureMountedRootfs(ctx, rootfsVolume, mountpoint, adopt)
	if err != nil {
		return sandboxRecord{}, err
	}

	sb := sandboxRecord{
		ID:           id,
		Name:         name,
		State:        stateStopped,
		Source:       req.Source,
		RootfsVolume: rootfsVolume,
		Mountpoint:   owner.Mountpoint,
		OwnerSocket:  owner.Socket,
		RunscID:      id,
		OwnerMode:    map[bool]string{true: "spawned", false: "adopted"}[owner.Spawned],
		OwnerPID:     owner.PID,
		Network:      firstNonEmpty(req.Network, networkModeHost),
		Env:          req.Env,
		Cwd:          req.Cwd,
		Labels:       req.Labels,
		Entrypoint:   req.Entrypoint,
		Derived:      derived,
		CreatedAt:    time.Now().UTC(),
	}
	d.mu.Lock()
	d.sandboxes[id] = sb
	if err := d.saveLocked(); err != nil {
		d.mu.Unlock()
		return sandboxRecord{}, err
	}
	d.mu.Unlock()
	return d.startSandbox(ctx, id)
}

func (d *daemon) startSandbox(ctx context.Context, id string) (sandboxRecord, error) {
	sb, err := d.lookupSandbox(id)
	if err != nil {
		return sandboxRecord{}, err
	}
	if sb.State == stateRunning {
		return sb, nil
	}
	owner, err := d.ensureMountedRootfs(ctx, sb.RootfsVolume, d.mountDir(id), sb.Source.Kind == sourceKindVolume && sb.Source.Mode == volumeModeAttach)
	if err != nil {
		return sandboxRecord{}, err
	}
	ownerMode := map[bool]string{true: "spawned", false: "adopted"}[owner.Spawned]
	ownerPID := owner.PID
	if sb.OwnerMode == "spawned" && owner.Socket == sb.OwnerSocket && owner.Mountpoint == sb.Mountpoint {
		ownerMode = "spawned"
		ownerPID = sb.OwnerPID
	}
	sb.Mountpoint = owner.Mountpoint
	sb.OwnerSocket = owner.Socket
	sb.OwnerMode = ownerMode
	sb.OwnerPID = ownerPID
	if err := d.startRunsc(ctx, sb); err != nil {
		now := time.Now().UTC()
		sb.State = stateBroken
		sb.StartedAt = nil
		sb.StoppedAt = &now
		d.mu.Lock()
		d.sandboxes[id] = sb
		saveErr := d.saveLocked()
		d.mu.Unlock()
		if saveErr != nil {
			return sandboxRecord{}, fmt.Errorf("%w (save broken state: %v)", d.annotateSandboxError(id, err), saveErr)
		}
		return sandboxRecord{}, d.annotateSandboxError(id, err)
	}
	now := time.Now().UTC()
	sb.State = stateRunning
	sb.StartedAt = &now
	sb.StoppedAt = nil
	d.mu.Lock()
	d.sandboxes[id] = sb
	if err := d.saveLocked(); err != nil {
		d.mu.Unlock()
		return sandboxRecord{}, err
	}
	d.mu.Unlock()
	if len(sb.Entrypoint) > 0 {
		_, err := d.createSession(ctx, id, processCreateRequest{
			Command:    sb.Entrypoint,
			Cwd:        sb.Cwd,
			Env:        sb.Env,
			Background: true,
		})
		if err != nil {
			return sandboxRecord{}, err
		}
	}
	return sb, nil
}

func (d *daemon) stopSandbox(ctx context.Context, id string) (sandboxRecord, error) {
	sb, err := d.lookupSandbox(id)
	if err != nil {
		return sandboxRecord{}, err
	}
	if err := d.stopRunsc(ctx, sb); err != nil {
		return sandboxRecord{}, err
	}
	now := time.Now().UTC()
	sb.State = stateStopped
	sb.StoppedAt = &now
	d.mu.Lock()
	d.sandboxes[id] = sb
	err = d.saveLocked()
	d.mu.Unlock()
	return sb, err
}

func (d *daemon) deleteSandbox(ctx context.Context, id string) error {
	sb, err := d.lookupSandbox(id)
	if err != nil {
		return err
	}
	if sb.State == stateRunning {
		if _, err := d.stopSandbox(ctx, id); err != nil {
			return err
		}
	}
	if sb.OwnerMode == "spawned" {
		if sb.Derived && sb.OwnerPID > 0 {
			if err := d.forceStopOwner(ctx, sb.OwnerPID, sb.OwnerSocket, sb.Mountpoint); err != nil {
				return err
			}
			if err := d.breakLeaseForce(ctx, sb.RootfsVolume); err != nil {
				return err
			}
		} else {
			if err := d.stopOwner(ctx, sb.OwnerSocket); err != nil {
				return err
			}
		}
	}
	if sb.Derived {
		if err := d.deleteVolume(ctx, sb.RootfsVolume); err != nil {
			return err
		}
	}
	if err := removeAllRetry(ctx, d.sandboxDir(id)); err != nil {
		return err
	}
	d.mu.Lock()
	delete(d.sandboxes, id)
	err = d.saveLocked()
	d.mu.Unlock()
	return err
}

func removeAllRetry(ctx context.Context, target string) error {
	const (
		maxAttempts = 20
		backoff     = 100 * time.Millisecond
	)
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := os.RemoveAll(target); err == nil {
			return nil
		} else if !errors.Is(err, syscall.EBUSY) {
			return err
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return lastErr
}

func (d *daemon) execProcess(ctx context.Context, sandboxID string, req processCreateRequest) (processRecord, execResult, error) {
	sb, err := d.lookupSandbox(sandboxID)
	if err != nil {
		return processRecord{}, execResult{}, err
	}
	if sb.State != stateRunning {
		return processRecord{}, execResult{}, fmt.Errorf("sandbox %q is not running", sandboxID)
	}
	now := time.Now().UTC()
	command := normalizedCommand(req)
	record := processRecord{
		ID:         newID("proc"),
		SandboxID:  sandboxID,
		Command:    command,
		Cwd:        req.Cwd,
		Env:        req.Env,
		State:      stateRunning,
		TTY:        req.TTY,
		Background: req.Background,
		CreatedAt:  now,
		StartedAt:  &now,
	}
	result, err := d.execOneShot(ctx, sb, req)
	if err != nil {
		return processRecord{}, execResult{}, err
	}
	exit := result.ExitCode
	record.ExitCode = &exit
	record.State = stateStopped
	stopped := time.Now().UTC()
	record.StoppedAt = &stopped
	return record, result, nil
}

func (d *daemon) createSession(ctx context.Context, sandboxID string, req processCreateRequest) (processRecord, error) {
	sb, err := d.lookupSandbox(sandboxID)
	if err != nil {
		return processRecord{}, err
	}
	if sb.State != stateRunning {
		return processRecord{}, fmt.Errorf("sandbox %q is not running", sandboxID)
	}
	now := time.Now().UTC()
	command := normalizedCommand(req)
	record := processRecord{
		ID:         newID("proc"),
		SandboxID:  sandboxID,
		Command:    command,
		Cwd:        req.Cwd,
		Env:        req.Env,
		TTY:        req.TTY,
		Background: true,
		State:      stateRunning,
		CreatedAt:  now,
		StartedAt:  &now,
	}
	d.mu.Lock()
	d.sessions[record.ID] = &session{record: record, sandbox: sandboxID}
	d.mu.Unlock()
	cmd, err := d.startSession(ctx, record.ID, sb, req)
	if err != nil {
		d.mu.Lock()
		delete(d.sessions, record.ID)
		d.mu.Unlock()
		return processRecord{}, err
	}
	d.mu.Lock()
	s := d.sessions[record.ID]
	if s == nil {
		s = &session{record: record, sandbox: sandboxID}
	}
	s.cmd = cmd
	d.sessions[record.ID] = s
	d.mu.Unlock()
	return record, nil
}

func (d *daemon) signalSession(id string, signalName string) error {
	d.mu.Lock()
	s := d.sessions[id]
	d.mu.Unlock()
	if s == nil || s.cmd == nil || s.cmd.proc == nil {
		return fmt.Errorf("unknown session %q", id)
	}
	sig, ok := signalByName(signalName)
	if !ok {
		return fmt.Errorf("unknown signal %q", signalName)
	}
	return s.cmd.proc.Signal(sig)
}

func signalByName(name string) (os.Signal, bool) {
	switch strings.ToUpper(name) {
	case "TERM", "SIGTERM":
		return syscall.SIGTERM, true
	case "INT", "SIGINT":
		return syscall.SIGINT, true
	case "KILL", "SIGKILL":
		return syscall.SIGKILL, true
	default:
		return nil, false
	}
}

func readJSON(r *http.Request, target any) error {
	defer util.SafeClose(r.Body, "close sandboxd request body")
	if err := json.NewDecoder(r.Body).Decode(target); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	payload := errorResponse{Error: err.Error()}
	var debugErr *sandboxDebugError
	if errors.As(err, &debugErr) {
		debug := debugErr.debug
		payload.Debug = &debug
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
