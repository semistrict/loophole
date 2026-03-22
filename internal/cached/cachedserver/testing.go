package cachedserver

import (
	"bufio"
	"fmt"
	"net"
	"strings"

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
// using this listener. Performs the HTTP 101 upgrade handshake over a pipe,
// then returns a PageCache using the upgraded connection.
func (l *PipeListener) Dial(sp *safepoint.Safepoint) *cached.PageCache {
	clientConn, serverConn := net.Pipe()
	l.ch <- serverConn

	// Perform HTTP upgrade handshake on the client side.
	upgradeReq := "GET /connect HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: loophole-cached\r\n\r\n"
	if _, err := clientConn.Write([]byte(upgradeReq)); err != nil {
		panic(fmt.Sprintf("test dial: write upgrade: %v", err))
	}

	br := bufio.NewReader(clientConn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		panic(fmt.Sprintf("test dial: read status: %v", err))
	}
	if !strings.HasPrefix(statusLine, "HTTP/1.1 101") {
		panic(fmt.Sprintf("test dial: unexpected status: %s", strings.TrimSpace(statusLine)))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			panic(fmt.Sprintf("test dial: read headers: %v", err))
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}

	return cached.TestOnlyNewInProcessBufio(clientConn, br, Arena(), sp)
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
