package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
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

func newConn(nc net.Conn) *conn {
	return &conn{
		netConn: nc,
		decoder: json.NewDecoder(nc),
		done:    make(chan struct{}),
	}
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

func startServer(d string) error {
	if err := initDaemon(d); err != nil {
		return err
	}

	sockPath := cached.SocketPath(d)
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		_ = shutdown()
		return err
	}

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		_ = shutdown()
		return err
	}

	wg.Add(1)
	go acceptLoop(ln)
	return nil
}

func acceptLoop(ln net.Listener) {
	defer wg.Done()
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-stopCh:
				return
			default:
				slog.Error("cached: accept error", "error", err)
				continue
			}
		}

		c := newConn(nc)
		mu.Lock()
		conns[c] = struct{}{}
		metrics.CacheDaemonClients.Set(float64(len(conns)))
		mu.Unlock()

		wg.Add(1)
		go handleConn(c)
	}
}
