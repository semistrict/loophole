package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/creack/pty/v2"
	"github.com/gorilla/websocket"
	"github.com/muesli/termenv"
	"github.com/vito/midterm"

	"github.com/semistrict/loophole/fsbackend"
)

var nextPTYID atomic.Int64

func ptyStart(cmd *exec.Cmd, cols, rows uint16) (*os.File, error) {
	return pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
}

func ptySetsize(f *os.File, cols, rows uint16) error {
	return pty.Setsize(f, &pty.Winsize{Cols: cols, Rows: rows})
}

// ── WS protocol types ──────────────────────────────────────

type screenMsg struct {
	Type   string    `json:"type"` // "screen"
	Rows   []string  `json:"rows"`
	Cursor cursorMsg `json:"cursor"`
	Width  int       `json:"width"`
	Height int       `json:"height"`
	Title  string    `json:"title"`
}

type updateMsg struct {
	Type   string            `json:"type"` // "update"
	Rows   map[string]string `json:"rows"`
	Cursor cursorMsg         `json:"cursor"`
	Title  string            `json:"title,omitempty"`
}

type cursorMsg struct {
	X       int  `json:"x"`
	Y       int  `json:"y"`
	Visible bool `json:"visible"`
}

type closedMsg struct {
	Type string `json:"type"` // "closed"
}

// ── ptySession ─────────────────────────────────────────────

type ptySession struct {
	id         string
	volume     string
	mountpoint string
	ptmx       *os.File
	cmd        *exec.Cmd

	vt *midterm.Terminal

	mu          sync.Mutex
	subs        map[*websocket.Conn]struct{}
	lastChanges []uint64 // snapshot of vt.Changes after last broadcast

	done chan struct{}
}

func (s *ptySession) subscribe(conn *websocket.Conn) {
	s.mu.Lock()
	s.subs[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *ptySession) unsubscribe(conn *websocket.Conn) {
	s.mu.Lock()
	delete(s.subs, conn)
	s.mu.Unlock()
}

// formatCSS converts a midterm.Format to inline CSS style string.
func formatCSS(f midterm.Format) string {
	var parts []string
	fg, bg := f.Fg, f.Bg
	if f.IsReverse() {
		bg, fg = fg, bg
	}
	if fg != nil {
		parts = append(parts, "color:"+termenv.ConvertToRGB(fg).Hex())
	}
	if bg != nil {
		parts = append(parts, "background-color:"+termenv.ConvertToRGB(bg).Hex())
	}
	if f.IsBold() {
		parts = append(parts, "font-weight:bold")
	}
	if f.IsFaint() {
		parts = append(parts, "opacity:0.33")
	}
	if f.IsUnderline() {
		parts = append(parts, "text-decoration:underline")
	}
	if f.IsConceal() {
		parts = append(parts, "display:none")
	}
	return strings.Join(parts, ";")
}

// renderRowHTML renders a single row as an HTML string of styled <span>s.
// Must be called with s.mu held.
func (s *ptySession) renderRowHTML(y int) string {
	var buf bytes.Buffer
	var x int
	for region := range s.vt.Format.Regions(y) {
		css := formatCSS(region.F)
		buf.WriteString(`<span style="`)
		buf.WriteString(css)
		buf.WriteString(`">`)
		// Replace null runes with spaces — null bytes in HTML are replaced
		// by browsers with U+FFFD replacement characters.
		slice := s.vt.Content[y][x : x+region.Size]
		runes := make([]rune, len(slice))
		for i, r := range slice {
			if r == 0 {
				runes[i] = ' '
			} else {
				runes[i] = r
			}
		}
		buf.WriteString(html.EscapeString(string(runes)))
		buf.WriteString("</span>")
		x += region.Size
	}
	return buf.String()
}

// fullScreenMsg builds a screenMsg with all rows. Must be called with s.mu held.
func (s *ptySession) fullScreenMsg() screenMsg {
	rows := make([]string, s.vt.Height)
	for y := range rows {
		rows[y] = s.renderRowHTML(y)
	}
	return screenMsg{
		Type:   "screen",
		Rows:   rows,
		Width:  s.vt.Width,
		Height: s.vt.Height,
		Title:  s.vt.Title,
		Cursor: cursorMsg{
			X:       s.vt.Cursor.X,
			Y:       s.vt.Cursor.Y,
			Visible: s.vt.CursorVisible,
		},
	}
}

// changedRowsMsg builds an updateMsg with only the rows that changed
// since lastChanges. Must be called with s.mu held.
func (s *ptySession) changedRowsMsg() updateMsg {
	rows := make(map[string]string)
	for y := 0; y < s.vt.Height && y < len(s.vt.Changes); y++ {
		if y >= len(s.lastChanges) || s.vt.Changes[y] != s.lastChanges[y] {
			rows[strconv.Itoa(y)] = s.renderRowHTML(y)
		}
	}
	s.lastChanges = append(s.lastChanges[:0], s.vt.Changes...)
	return updateMsg{
		Type:  "update",
		Rows:  rows,
		Title: s.vt.Title,
		Cursor: cursorMsg{
			X:       s.vt.Cursor.X,
			Y:       s.vt.Cursor.Y,
			Visible: s.vt.CursorVisible,
		},
	}
}

// screenText returns the plain text content of the terminal screen.
func (s *ptySession) screenText() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	height := s.vt.Height
	width := s.vt.Width
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			cell := s.vt.Content[y][x]
			if cell == 0 {
				buf.WriteRune(' ')
			} else {
				buf.WriteRune(cell)
			}
		}
		line := bytes.TrimRight(buf.Bytes()[buf.Len()-width:], " ")
		buf.Truncate(buf.Len() - width)
		buf.Write(line)
		if y < height-1 {
			buf.WriteByte('\n')
		}
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n "))
}

// readLoop reads PTY output, feeds the VT emulator, and broadcasts
// changed rows as JSON to all subscribed WebSockets.
func (s *ptySession) readLoop() {
	defer close(s.done)
	buf := make([]byte, 32*1024)
	for {
		n, err := s.ptmx.Read(buf)
		if n > 0 {
			s.mu.Lock()
			_, _ = s.vt.Write(buf[:n])
			msg := s.changedRowsMsg()
			conns := make([]*websocket.Conn, 0, len(s.subs))
			for c := range s.subs {
				conns = append(conns, c)
			}
			s.mu.Unlock()

			if len(msg.Rows) > 0 {
				data, _ := json.Marshal(msg)
				for _, c := range conns {
					if wErr := c.WriteMessage(websocket.TextMessage, data); wErr != nil {
						slog.Debug("pty.broadcast.write-error", "id", s.id, "error", wErr)
					}
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				slog.Debug("pty.readloop.error", "id", s.id, "error", err)
			}
			closedData, _ := json.Marshal(closedMsg{Type: "closed"})
			s.mu.Lock()
			for c := range s.subs {
				_ = c.WriteMessage(websocket.TextMessage, closedData)
				_ = c.Close()
			}
			s.mu.Unlock()
			return
		}
	}
}

// ── ptyManager ─────────────────────────────────────────────

type ptyManager struct {
	mu       sync.Mutex
	sessions map[string]*ptySession
}

func newPtyManager() *ptyManager {
	return &ptyManager{sessions: make(map[string]*ptySession)}
}

func (pm *ptyManager) start(id, volume, mountpoint string, cols, rows uint16, backend fsbackend.Service) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.sessions[id]; ok {
		return fmt.Errorf("session %s already exists", id)
	}

	shell := "/bin/bash"
	if _, err := os.Stat(mountpoint + shell); err != nil {
		shell = "/bin/sh"
	}
	cmd := chrootCmd(mountpoint, backend, shell, "-l")

	ptmx, err := ptyStart(cmd, cols, rows)
	if err != nil {
		return fmt.Errorf("pty start: %w", err)
	}

	vt := midterm.NewTerminal(int(rows), int(cols))
	vt.CursorVisible = true

	sess := &ptySession{
		id:          id,
		volume:      volume,
		mountpoint:  mountpoint,
		ptmx:        ptmx,
		cmd:         cmd,
		vt:          vt,
		subs:        make(map[*websocket.Conn]struct{}),
		lastChanges: make([]uint64, rows),
		done:        make(chan struct{}),
	}
	pm.sessions[id] = sess

	go sess.readLoop()

	go func() {
		_ = cmd.Wait()
		pm.mu.Lock()
		if s, ok := pm.sessions[id]; ok && s.ptmx == ptmx {
			_ = ptmx.Close()
			delete(pm.sessions, id)
		}
		pm.mu.Unlock()
		slog.Info("pty.exited", "id", id, "volume", volume)
	}()

	slog.Info("pty.started", "id", id, "volume", volume, "cols", cols, "rows", rows)
	return nil
}

func (pm *ptyManager) resize(id string, cols, rows uint16) error {
	pm.mu.Lock()
	s, ok := pm.sessions[id]
	pm.mu.Unlock()
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	if err := ptySetsize(s.ptmx, cols, rows); err != nil {
		return err
	}
	s.mu.Lock()
	s.vt.Resize(int(rows), int(cols))
	s.lastChanges = make([]uint64, rows)
	s.mu.Unlock()
	return nil
}

func (pm *ptyManager) kill(id string) error {
	pm.mu.Lock()
	s, ok := pm.sessions[id]
	if ok {
		delete(pm.sessions, id)
	}
	pm.mu.Unlock()
	if !ok {
		return fmt.Errorf("session %s not found", id)
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	_ = s.ptmx.Close()
	slog.Info("pty.killed", "id", id)
	return nil
}

// killAll kills all PTY sessions (used during shutdown).
func (pm *ptyManager) killAll() {
	pm.mu.Lock()
	sessions := make([]*ptySession, 0, len(pm.sessions))
	for _, s := range pm.sessions {
		sessions = append(sessions, s)
	}
	pm.sessions = make(map[string]*ptySession)
	pm.mu.Unlock()

	for _, s := range sessions {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		_ = s.ptmx.Close()
	}
}

type ptySessionInfo struct {
	ID     string `json:"id"`
	Volume string `json:"volume"`
	Title  string `json:"title"`
}

func (pm *ptyManager) list() []ptySessionInfo {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	infos := make([]ptySessionInfo, 0, len(pm.sessions))
	for _, s := range pm.sessions {
		s.mu.Lock()
		title := s.vt.Title
		s.mu.Unlock()
		infos = append(infos, ptySessionInfo{ID: s.id, Volume: s.volume, Title: title})
	}
	return infos
}

func (pm *ptyManager) get(id string) *ptySession {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.sessions[id]
}

// ── HTTP handlers ──────────────────────────────────────────

func (d *Daemon) handlePTYCreate(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	var body struct {
		ID     string `json:"id"`
		Volume string `json:"volume"`
		Cols   uint16 `json:"cols"`
		Rows   uint16 `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	if body.ID == "" || body.Volume == "" {
		writeError(w, 400, fmt.Errorf("missing id or volume"))
		return
	}
	if body.Cols == 0 {
		body.Cols = 120
	}
	if body.Rows == 0 {
		body.Rows = 30
	}

	// Auto-mount the volume.
	if _, err := d.backend.FSForVolume(r.Context(), body.Volume); err != nil {
		writeError(w, 500, fmt.Errorf("mount volume %s: %w", body.Volume, err))
		return
	}

	mountpoint := ""
	for mp, vol := range d.backend.Mounts() {
		if vol == body.Volume {
			mountpoint = mp
			break
		}
	}
	if mountpoint == "" {
		writeError(w, 500, fmt.Errorf("volume %s not mounted", body.Volume))
		return
	}

	if err := d.ptyMgr.start(body.ID, body.Volume, mountpoint, body.Cols, body.Rows, d.backend); err != nil {
		writeError(w, 400, err)
		return
	}
	writeJSON(w, map[string]string{"id": body.ID})
}

func (d *Daemon) handlePTYList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"sessions": d.ptyMgr.list()})
}

func (d *Daemon) handlePTYWebSocket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s := d.ptyMgr.get(id)
	if s == nil {
		writeError(w, 404, fmt.Errorf("PTY session %s not found", id))
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		d.log.Error("pty ws upgrade failed", "id", id, "err", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Send full screen state to the new client.
	s.mu.Lock()
	screenData, _ := json.Marshal(s.fullScreenMsg())
	s.mu.Unlock()

	if err := conn.WriteMessage(websocket.TextMessage, screenData); err != nil {
		d.log.Debug("pty ws screen write error", "id", id, "err", err)
		return
	}

	s.subscribe(conn)
	defer s.unsubscribe(conn)

	// Client → PTY: read JSON messages.
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg struct {
			Type string `json:"type"`
			Data string `json:"data"`
			Cols uint16 `json:"cols"`
			Rows uint16 `json:"rows"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}

		switch msg.Type {
		case "input":
			if _, err := s.ptmx.Write([]byte(msg.Data)); err != nil {
				return
			}
		case "resize":
			_ = d.ptyMgr.resize(id, msg.Cols, msg.Rows)
		}
	}
}

func (d *Daemon) handlePTYScreen(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s := d.ptyMgr.get(id)
	if s == nil {
		writeError(w, 404, fmt.Errorf("PTY session %s not found", id))
		return
	}
	writeJSON(w, map[string]string{"content": s.screenText()})
}

func (d *Daemon) handlePTYKill(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := d.ptyMgr.kill(id); err != nil {
		writeError(w, 404, err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (d *Daemon) handlePTYResize(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Cols uint16 `json:"cols"`
		Rows uint16 `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	if err := d.ptyMgr.resize(id, body.Cols, body.Rows); err != nil {
		writeError(w, 404, err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

// handleShellCompat is the old GET /sandbox/shell endpoint that now creates
// a PTY session and speaks the midterm protocol over the same WebSocket.
func (d *Daemon) handleShellCompat(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	volume := r.URL.Query().Get("volume")
	if volume == "" {
		writeError(w, 400, fmt.Errorf("missing volume parameter"))
		return
	}

	if _, err := d.backend.FSForVolume(r.Context(), volume); err != nil {
		writeError(w, 500, fmt.Errorf("mount volume %s: %w", volume, err))
		return
	}

	mountpoint := ""
	for mp, vol := range d.backend.Mounts() {
		if vol == volume {
			mountpoint = mp
			break
		}
	}
	if mountpoint == "" {
		writeError(w, 500, fmt.Errorf("volume %s not mounted", volume))
		return
	}

	// Generate a unique session ID.
	id := fmt.Sprintf("%s-%d", volume, nextPTYID.Add(1))
	if err := d.ptyMgr.start(id, volume, mountpoint, 120, 30, d.backend); err != nil {
		writeError(w, 500, err)
		return
	}

	// Upgrade to WebSocket and behave like the PTY WS handler.
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		d.log.Error("shell compat ws upgrade failed", "err", err)
		_ = d.ptyMgr.kill(id)
		return
	}
	defer func() { _ = conn.Close() }()

	s := d.ptyMgr.get(id)
	if s == nil {
		return
	}

	s.mu.Lock()
	screenData, _ := json.Marshal(s.fullScreenMsg())
	s.mu.Unlock()

	if err := conn.WriteMessage(websocket.TextMessage, screenData); err != nil {
		_ = d.ptyMgr.kill(id)
		return
	}

	s.subscribe(conn)
	defer func() {
		s.unsubscribe(conn)
		_ = d.ptyMgr.kill(id)
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Type string `json:"type"`
			Data string `json:"data"`
			Cols uint16 `json:"cols"`
			Rows uint16 `json:"rows"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "input":
			if _, err := s.ptmx.Write([]byte(msg.Data)); err != nil {
				return
			}
		case "resize":
			_ = d.ptyMgr.resize(id, msg.Cols, msg.Rows)
		}
	}
}
