package cachedserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/semistrict/loophole/internal/cached"
	"github.com/semistrict/loophole/internal/httputil"
	"github.com/semistrict/loophole/internal/metrics"
	"github.com/semistrict/loophole/internal/util"
)

// --- Drain coordination ---
//
// The budget loop sets drainActive=true and waits on drainWg for every
// connected client. Each connection handler checks drainActive after
// processing a request. If set, it sends OpDrain to the client and
// enters the "draining" state. In that state it still processes normal
// requests (in-flight on the wire) but when it sees OpDrained it
// signals drainWg and blocks on drainDone. The budget loop evicts,
// then closes drainDone so all handlers resume.

var (
	drainMu     sync.Mutex
	drainActive bool
	drainWg     sync.WaitGroup
	drainDone   chan struct{} // closed by budget loop after eviction
)

// conn represents a connected client.
type conn struct {
	netConn  net.Conn
	writeMu  sync.Mutex
	decoder  *json.Decoder
	done     chan struct{} // closed when handleConn exits
	draining bool          // per-conn: drain sent, waiting for OpDrained
}

func (c *conn) send(msg any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return json.NewEncoder(c.netConn).Encode(msg)
}

func handleConn(c *conn) {
	defer wg.Done()
	defer close(c.done)
	defer func() {
		util.SafeClose(c.netConn, "close client conn")
		mu.Lock()
		delete(conns, c)
		metrics.CacheDaemonClients.Set(float64(len(conns)))
		mu.Unlock()
	}()

	for {
		// If a drain is needed and we haven't sent OpDrain yet, do so now.
		if !c.draining && shouldDrain() {
			c.draining = true
			_ = c.send(cached.DaemonMsg{Op: cached.OpDrain})
		}

		var msg cached.ClientMsg
		if err := c.decoder.Decode(&msg); err != nil {
			if c.draining {
				drainWg.Done()
			}
			return
		}

		switch msg.Op {
		case cached.OpDrained:
			// Client confirms drain. Wait for eviction, then resume.
			drainWg.Done()
			<-drainDone
			c.draining = false
			_ = c.send(cached.DaemonMsg{Op: cached.OpResume})

		case cached.OpLookup:
			metrics.CacheDaemonLookups.Inc()
			mu.Lock()
			slot, ok := lookupLocked(cacheKey{msg.LayerID, msg.PageIdx})
			mu.Unlock()
			if ok {
				metrics.CacheDaemonHits.Inc()
			}
			_ = c.send(cached.DaemonMsg{Hit: ok, Slot: slot})

		case cached.OpPopulate:
			metrics.CacheDaemonPopulates.Inc()
			mu.Lock()
			slot, err := populateLocked(cacheKey{msg.LayerID, msg.PageIdx}, msg.Data)
			mu.Unlock()
			if err != nil {
				metrics.CacheDaemonPopulateErrors.Inc()
				_ = c.send(cached.DaemonMsg{Error: err.Error()})
			} else {
				_ = c.send(cached.DaemonMsg{Slot: slot})
			}

		case cached.OpDelete:
			metrics.CacheDaemonDeletes.Inc()
			mu.Lock()
			deletePageLocked(cacheKey{msg.LayerID, msg.PageIdx})
			mu.Unlock()
			_ = c.send(cached.DaemonMsg{})

		case cached.OpCount:
			mu.Lock()
			count := len(index)
			mu.Unlock()
			_ = c.send(cached.DaemonMsg{Count: count})

		default:
			_ = c.send(cached.DaemonMsg{Error: fmt.Sprintf("unknown op: %d", msg.Op)})
		}
	}
}

func shouldDrain() bool {
	drainMu.Lock()
	defer drainMu.Unlock()
	return drainActive
}

func startDrain() int {
	drainMu.Lock()
	defer drainMu.Unlock()
	drainActive = true
	drainDone = make(chan struct{})
	mu.Lock()
	n := len(conns)
	drainWg.Add(n)
	mu.Unlock()
	return n
}

func finishDrain() {
	close(drainDone)
	drainMu.Lock()
	drainActive = false
	drainMu.Unlock()
}

// handleConnect upgrades an HTTP connection to the streaming JSON protocol
// using HTTP 101 Switching Protocols.
func handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Connection") != "Upgrade" || r.Header.Get("Upgrade") != "loophole-cached" {
		http.Error(w, "expected Connection: Upgrade + Upgrade: loophole-cached", http.StatusBadRequest)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		return
	}

	nc, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the 101 response manually since we've hijacked.
	if _, err := bufrw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: loophole-cached\r\n\r\n"); err != nil {
		util.SafeClose(nc, "close hijacked conn after write error")
		return
	}
	if err := bufrw.Flush(); err != nil {
		util.SafeClose(nc, "close hijacked conn after flush error")
		return
	}

	c := newConnFromHijack(nc, bufrw.Reader)

	mu.Lock()
	conns[c] = struct{}{}
	metrics.CacheDaemonClients.Set(float64(len(conns)))
	mu.Unlock()

	wg.Add(1)
	go handleConn(c)
}

// newConnFromHijack creates a conn from a hijacked connection.
// Uses the buffered reader from hijack (may have buffered data).
func newConnFromHijack(nc net.Conn, br *bufio.Reader) *conn {
	return &conn{
		netConn: nc,
		decoder: json.NewDecoder(br),
		done:    make(chan struct{}),
	}
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	pages := len(index)
	used := usedBytes
	b := budget
	clients := len(conns)
	mu.Unlock()

	httputil.WriteJSON(w, map[string]any{
		"pages":      pages,
		"used_bytes": used,
		"budget":     b,
		"clients":    clients,
	})
}

// StartServer initializes the daemon state from dir and starts accepting
// connections on a Unix socket at dir/cached.sock.
func StartServer(d string) error {
	return StartServerWithListener(d, nil)
}

// StartServerWithListener initializes the daemon and starts accepting on ln.
// If ln is nil, a Unix socket listener is created at dir/cached.sock.
// If InitInMemory was called first, initDaemon is skipped.
func StartServerWithListener(d string, ln net.Listener) error {
	if arena == nil {
		if err := initDaemon(d); err != nil {
			return err
		}
	}

	if ln == nil {
		sockPath := cached.SocketPath(d)
		if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
			_ = Shutdown()
			return err
		}
		var err error
		ln, err = net.Listen("unix", sockPath)
		if err != nil {
			_ = Shutdown()
			return err
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /connect", handleConnect)
	mux.HandleFunc("GET /status", handleStatus)
	httputil.RegisterObservabilityRoutes(mux)

	srv := &http.Server{Handler: mux}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.Serve(ln) //nolint:errcheck // returns on Close
	}()

	go func() {
		<-stopCh
		util.SafeClose(srv, "close cached HTTP server")
	}()

	return nil
}
