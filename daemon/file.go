package daemon

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/semistrict/loophole/filecmd"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/internal/streammux"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (d *Daemon) handleFile(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		d.log.Error("websocket upgrade failed", "err", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// First text message: argv as JSON string array.
	_, msg, err := conn.ReadMessage()
	if err != nil {
		d.log.Error("read argv", "err", err)
		return
	}

	var argv []string
	if err := json.Unmarshal(msg, &argv); err != nil {
		d.log.Error("decode argv", "err", err)
		return
	}
	if len(argv) == 0 {
		d.log.Error("empty argv")
		return
	}

	cmdName := argv[0]
	args := argv[1:]

	cmd := filecmd.Lookup(cmdName)
	if cmd == nil {
		d.log.Error("unknown file command", "cmd", cmdName)
		wsOut := &wsWriter{conn: conn}
		mux := streammux.NewWriter(wsOut)
		_, _ = fmt.Fprintf(mux.Stderr(), "unknown file command: %s", cmdName)
		_ = mux.Exit(1)
		return
	}

	// Set up streammux output over websocket.
	wsOut := &wsWriter{conn: conn}
	mux := streammux.NewWriter(wsOut)

	sess := &wsSession{
		ctx:     r.Context(),
		backend: d.backend,
		conn:    conn,
		mux:     mux,
		pipes:   make(map[uint32]*io.PipeWriter),
	}

	// Start background reader that routes incoming binary messages to pipes.
	go sess.readLoop()

	if err := cmd.Run(r.Context(), sess, args); err != nil {
		_, _ = fmt.Fprint(mux.Stderr(), err.Error())
		_ = mux.Exit(1)
		return
	}
	_ = mux.Exit(0)
}

// wsSession implements filecmd.Session over a websocket connection.
type wsSession struct {
	ctx     context.Context
	backend fsbackend.Service
	conn    *websocket.Conn
	mux     *streammux.Writer

	// Write lock for sending text messages (read requests) on the websocket.
	// Binary messages are sent via mux which has its own lock.
	writeMu sync.Mutex

	// Pipe routing for file reads.
	mu     sync.Mutex
	pipes  map[uint32]*io.PipeWriter
	nextID atomic.Uint32

	// Set when readLoop finishes (e.g. client disconnects).
	readErr atomic.Value // stores error
}

func (s *wsSession) FS(volume string) (fsbackend.FS, error) {
	wasMounted := s.backend.IsVolumeMounted(volume)
	fs, err := s.backend.FSForVolume(s.ctx, volume)
	if err != nil {
		return nil, err
	}
	if !wasMounted {
		_, _ = fmt.Fprintf(s.mux.Stderr(), "auto-mounted volume %q\n", volume)
	}
	return fs, nil
}

func (s *wsSession) Read(path string) (io.ReadCloser, error) {
	id := s.nextID.Add(1)
	pr, pw := io.Pipe()

	s.mu.Lock()
	s.pipes[id] = pw
	s.mu.Unlock()

	// Send read request to client.
	req, _ := json.Marshal(struct {
		Read string `json:"read"`
		ID   uint32 `json:"id"`
	}{Read: path, ID: id})

	s.writeMu.Lock()
	err := s.conn.WriteMessage(websocket.TextMessage, req)
	s.writeMu.Unlock()
	if err != nil {
		_ = pr.Close()
		s.mu.Lock()
		delete(s.pipes, id)
		s.mu.Unlock()
		return nil, fmt.Errorf("send read request: %w", err)
	}

	return pr, nil
}

func (s *wsSession) Stdout() io.Writer {
	return s.mux.Stdout()
}

func (s *wsSession) Stderr() io.Writer {
	return s.mux.Stderr()
}

// readLoop reads binary messages from the client and routes them to the
// correct pipe based on the 4-byte uint32 id prefix.
func (s *wsSession) readLoop() {
	for {
		msgType, msg, err := s.conn.ReadMessage()
		if err != nil {
			s.readErr.Store(err)
			// Close all pipes.
			s.mu.Lock()
			for id, pw := range s.pipes {
				pw.CloseWithError(io.EOF)
				delete(s.pipes, id)
			}
			s.mu.Unlock()
			return
		}

		// Only process binary messages (file data). Text messages are unexpected
		// from the client at this point.
		if msgType != websocket.BinaryMessage {
			continue
		}

		if len(msg) < 4 {
			continue
		}

		id := binary.BigEndian.Uint32(msg[:4])
		data := msg[4:]

		s.mu.Lock()
		pw, ok := s.pipes[id]
		s.mu.Unlock()

		if !ok {
			continue
		}

		if len(data) == 0 {
			// EOF for this file.
			_ = pw.Close()
			s.mu.Lock()
			delete(s.pipes, id)
			s.mu.Unlock()
		} else {
			if _, err := pw.Write(data); err != nil {
				s.mu.Lock()
				delete(s.pipes, id)
				s.mu.Unlock()
			}
		}
	}
}

// wsWriter sends each Write as a binary websocket message.
// It holds its own lock because streammux.Writer calls Write from
// multiple goroutines (stdout, stderr, exit).
type wsWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *wsWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
