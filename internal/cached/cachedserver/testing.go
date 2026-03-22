package cachedserver

import (
	"net"

	"github.com/semistrict/loophole/internal/cached"
	"github.com/semistrict/loophole/internal/safepoint"
)

// PipeListener is a net.Listener backed by a channel. Use Dial to create
// a connected client/server pair without real sockets.
type PipeListener struct {
	ch     chan net.Conn
	closed chan struct{}
}

// NewPipeListener creates a new in-memory listener.
func NewPipeListener() *PipeListener {
	return &PipeListener{
		ch:     make(chan net.Conn, 8),
		closed: make(chan struct{}),
	}
}

func (l *PipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *PipeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *PipeListener) Addr() net.Addr { return pipeAddr{} }

// Dial creates a new in-process PageCache client connected to a server
// using this listener. The arena is shared directly (no mmap).
func (l *PipeListener) Dial(sp *safepoint.Safepoint) *cached.PageCache {
	clientConn, serverConn := net.Pipe()
	l.ch <- serverConn
	return cached.TestOnlyNewInProcess(clientConn, Arena(), sp)
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
