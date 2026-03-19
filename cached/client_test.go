package cached

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// --- Mock server ---

// mockServer simulates the daemon side of the protocol over a net.Pipe.
// It owns a shared arena []byte (same slice the client reads from).
type mockServer struct {
	conn    net.Conn
	enc     *json.Encoder
	dec     *json.Decoder
	arena   []byte
	index   map[string]int // "layerID:pageIdx" → slot
	nextSl  int
	stopped chan struct{}
}

func newMockServer(conn net.Conn, arena []byte) *mockServer {
	return &mockServer{
		conn:    conn,
		enc:     json.NewEncoder(conn),
		dec:     json.NewDecoder(conn),
		arena:   arena,
		index:   make(map[string]int),
		stopped: make(chan struct{}),
	}
}

func (s *mockServer) key(layerID string, pageIdx uint64) string {
	return fmt.Sprintf("%s:%d", layerID, pageIdx)
}

// serve reads and responds to messages until the connection closes.
func (s *mockServer) serve() {
	defer close(s.stopped)
	for {
		var msg ClientMsg
		if err := s.dec.Decode(&msg); err != nil {
			return
		}
		s.handle(msg)
	}
}

func (s *mockServer) handle(msg ClientMsg) {
	switch msg.Op {
	case OpLookup:
		k := s.key(msg.LayerID, msg.PageIdx)
		slot, ok := s.index[k]
		s.enc.Encode(DaemonMsg{Hit: ok, Slot: slot})
	case OpPopulate:
		k := s.key(msg.LayerID, msg.PageIdx)
		slot, exists := s.index[k]
		if !exists {
			slot = s.nextSl
			s.nextSl++
			s.index[k] = slot
		}
		off := slot * slotSize
		copy(s.arena[off:off+slotSize], msg.Data)
		s.enc.Encode(DaemonMsg{Slot: slot})
	case OpDelete:
		k := s.key(msg.LayerID, msg.PageIdx)
		delete(s.index, k)
		s.enc.Encode(DaemonMsg{})
	case OpCount:
		s.enc.Encode(DaemonMsg{Count: len(s.index)})
	case OpDrained:
		// handled by drainServer
	}
}

// drainServer wraps a mockServer and supports injecting drain events.
type drainServer struct {
	*mockServer
	drainedCh chan struct{} // test reads this to know client sent OpDrained
}

func newDrainServer(conn net.Conn, arena []byte) *drainServer {
	return &drainServer{
		mockServer: newMockServer(conn, arena),
		drainedCh:  make(chan struct{}, 1),
	}
}

// serve reads messages, processing normally but signaling drainedCh on OpDrained.
func (s *drainServer) serve() {
	defer close(s.stopped)
	for {
		var msg ClientMsg
		if err := s.dec.Decode(&msg); err != nil {
			return
		}
		if msg.Op == OpDrained {
			s.drainedCh <- struct{}{}
			continue
		}
		s.handle(msg)
	}
}

// sendDrain sends an OpDrain message to the client.
func (s *drainServer) sendDrain() {
	s.enc.Encode(DaemonMsg{Op: OpDrain})
}

// sendResume sends an OpResume message to the client.
func (s *drainServer) sendResume() {
	s.enc.Encode(DaemonMsg{Op: OpResume})
}

// --- Helpers ---

func makeArena(slots int) []byte { return make([]byte, slots*slotSize) }

func makePage(b byte) []byte { return bytes.Repeat([]byte{b}, slotSize) }

func setup(t *testing.T, arenaSlots int) (*Client, *mockServer) {
	t.Helper()
	arena := makeArena(arenaSlots)
	clientConn, serverConn := net.Pipe()
	client := newClientForTest(clientConn, arena)
	server := newMockServer(serverConn, arena)
	go server.serve()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})
	return client, server
}

func setupDrain(t *testing.T, arenaSlots int) (*Client, *drainServer) {
	t.Helper()
	arena := makeArena(arenaSlots)
	clientConn, serverConn := net.Pipe()
	client := newClientForTest(clientConn, arena)
	server := newDrainServer(serverConn, arena)
	go server.serve()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})
	return client, server
}

// --- Tests ---

func TestClient_PutGetDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := setup(t, 64)

		page := makePage(0xAB)
		client.PutPage("layer1", 1, page)

		got := client.GetPage("layer1", 1)
		if !bytes.Equal(got, page) {
			t.Fatalf("got %x, want %x", got[:4], page[:4])
		}

		client.DeletePage("layer1", 1)
		if got := client.GetPage("layer1", 1); got != nil {
			t.Fatal("expected nil after delete")
		}
	})
}

func TestClient_GetPageRef(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := setup(t, 64)

		page := makePage(0xCD)
		client.PutPage("ref", 1, page)

		data, unpin := client.GetPageRef("ref", 1)
		if data == nil {
			t.Fatal("expected hit")
		}
		if !bytes.Equal(data, page) {
			t.Fatal("data mismatch")
		}
		unpin()

		// Miss returns nil, nil.
		data, unpin = client.GetPageRef("missing", 0)
		if data != nil || unpin != nil {
			t.Fatal("expected nil for miss")
		}
	})
}

func TestClient_LocalCacheHit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, srv := setup(t, 64)

		page := makePage(0x11)
		client.PutPage("lc", 1, page)

		// First read goes through IPC.
		got := client.GetPage("lc", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("first read mismatch")
		}

		// Mutate the server index to prove the second read uses
		// the client's local cache (no IPC).
		delete(srv.index, "lc:1")

		got = client.GetPage("lc", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("second read should hit local cache")
		}
	})
}

func TestClient_Count(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := setup(t, 64)

		for i := range 5 {
			client.PutPage("t", uint64(i), makePage(byte(i)))
		}
		n, err := client.Count()
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("count = %d, want 5", n)
		}
	})
}

func TestClient_ConcurrentAccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := setup(t, 256)

		var wg sync.WaitGroup
		for i := range 16 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				page := makePage(byte(i))
				client.PutPage("c", uint64(i), page)
				got := client.GetPage("c", uint64(i))
				if !bytes.Equal(got, page) {
					t.Errorf("goroutine %d: mismatch", i)
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestClient_DrainClearsLocalCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, srv := setupDrain(t, 64)

		page := makePage(0xAA)
		client.PutPage("d", 1, page)

		// Populate local cache.
		got := client.GetPage("d", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("pre-drain read mismatch")
		}

		// Trigger drain.
		srv.sendDrain()
		<-srv.drainedCh

		// During drain, lookups return nil.
		if got := client.GetPage("d", 1); got != nil {
			t.Fatal("expected nil during drain")
		}

		srv.sendResume()
		time.Sleep(time.Millisecond) // let client process resume

		// After resume, local cache was cleared — IPC goes to server.
		// Server still has the page, so this should hit.
		got = client.GetPage("d", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("post-resume read mismatch")
		}
	})
}

func TestClient_DrainWaitsForPins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, srv := setupDrain(t, 64)

		page := makePage(0xBB)
		client.PutPage("pin", 1, page)

		// Hold a pin via GetPageRef.
		data, unpin := client.GetPageRef("pin", 1)
		if data == nil {
			t.Fatal("expected ref hit")
		}

		// Trigger drain — should block until pin is released.
		srv.sendDrain()

		drained := false
		go func() {
			<-srv.drainedCh
			drained = true
		}()

		time.Sleep(time.Millisecond)
		if drained {
			t.Fatal("drain should not complete while pin is held")
		}

		// Release the pin.
		unpin()
		time.Sleep(time.Millisecond)
		if !drained {
			t.Fatal("drain should complete after pin release")
		}

		srv.sendResume()
	})
}

func TestClient_MultipleClientsDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		const nClients = 4

		// Create a server conn per client. Each client talks to the
		// same mock server goroutine via its own pipe.
		type pair struct {
			client *Client
			srv    *drainServer
		}
		pairs := make([]pair, nClients)
		for i := range nClients {
			cc, sc := net.Pipe()
			cl := newClientForTest(cc, arena)
			sv := newDrainServer(sc, arena)
			go sv.serve()
			t.Cleanup(func() { cl.Close(); sc.Close() })
			pairs[i] = pair{cl, sv}
		}

		page := makePage(0xCC)
		// Populate through each client's own server.
		for _, p := range pairs {
			p.client.PutPage("m", 1, page)
		}

		// All clients read to populate local caches.
		for _, p := range pairs {
			got := p.client.GetPage("m", 1)
			if !bytes.Equal(got, page) {
				t.Fatal("pre-drain read mismatch")
			}
		}

		// Drain all clients concurrently.
		for _, p := range pairs {
			p.srv.sendDrain()
		}

		// Wait for all to drain.
		for _, p := range pairs {
			<-p.srv.drainedCh
		}

		// All clients should return nil during drain.
		for i, p := range pairs {
			if got := p.client.GetPage("m", 1); got != nil {
				t.Fatalf("client %d: expected nil during drain", i)
			}
		}

		// Resume all.
		for _, p := range pairs {
			p.srv.sendResume()
		}
		time.Sleep(time.Millisecond)

		// All clients should work again after resume.
		for i, p := range pairs {
			got := p.client.GetPage("m", 1)
			if !bytes.Equal(got, page) {
				t.Fatalf("client %d: post-resume mismatch", i)
			}
		}
	})
}

func TestClient_ClosedClientReturnsNil(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		srv := newMockServer(sc, arena)
		go srv.serve()

		page := makePage(0xDD)
		client.PutPage("cl", 1, page)

		client.Close()
		sc.Close()

		if got := client.GetPage("cl", 1); got != nil {
			t.Fatal("expected nil from closed client")
		}
		data, unpin := client.GetPageRef("cl", 1)
		if data != nil || unpin != nil {
			t.Fatal("expected nil ref from closed client")
		}
	})
}

// --- Crash / unexpected death tests ---

func TestClient_DaemonDiesWhileIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		srv := newMockServer(sc, arena)
		go srv.serve()

		page := makePage(0x11)
		client.PutPage("d", 1, page)

		got := client.GetPage("d", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("pre-crash read mismatch")
		}

		// Daemon crashes — close the server side of the pipe.
		sc.Close()
		time.Sleep(time.Millisecond)

		if !client.Dead() {
			t.Fatal("client should report dead after daemon crash")
		}

		// Local cache hit still works (arena bytes are valid).
		// But new IPC requests must not hang.
		client.PutPage("d", 2, makePage(0x22))
		// Should not hang or panic.
	})
}

func TestClient_DaemonDiesDuringRequest(t *testing.T) {
	// The server reads the request but crashes before responding.
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		defer client.Close()

		// Server: read one populate, respond. Then read next request and crash.
		go func() {
			dec := json.NewDecoder(sc)
			enc := json.NewEncoder(sc)

			// First request: populate — respond normally.
			var msg ClientMsg
			if err := dec.Decode(&msg); err != nil {
				return
			}
			copy(arena[0:slotSize], msg.Data)
			enc.Encode(DaemonMsg{Slot: 0})

			// Second request: read it but crash (close conn) before responding.
			dec.Decode(&msg)
			sc.Close()
		}()

		page := makePage(0xAA)
		client.PutPage("crash", 1, page)

		// Request a different key so it must go through IPC (no local cache).
		// The server reads this request but crashes before responding.
		// doRequest sees closed respCh, returns error → GetPage returns nil.
		got := client.GetPage("crash", 2)
		if got != nil {
			t.Fatal("expected nil when daemon died during request")
		}
	})
}

func TestClient_DaemonDiesDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		srv := newDrainServer(sc, arena)
		go srv.serve()

		page := makePage(0xBB)
		client.PutPage("dd", 1, page)
		client.GetPage("dd", 1) // populate local cache

		// Send drain.
		srv.sendDrain()
		<-srv.drainedCh

		// Daemon crashes during drain (after receiving drained, before resume).
		sc.Close()
		time.Sleep(time.Millisecond)

		// readLoop exits → draining is cleared, respCh is closed.
		// Local cache was cleared by drain, conn is dead → GetPage returns nil.
		got := client.GetPage("dd", 1)
		if got != nil {
			t.Fatal("expected nil after daemon death during drain")
		}

		// draining must be cleared so ops don't silently return nil forever.
		if client.draining.Load() {
			t.Fatal("draining should be cleared after daemon death")
		}
	})
}

func TestClient_ClientDiesDuringDrain(t *testing.T) {
	// Simulate a client dying mid-drain: the server sends drain,
	// then the client conn closes before sending drained.
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)

		// Manual server: populate, then drain, then expect client death.
		enc := json.NewEncoder(sc)
		dec := json.NewDecoder(sc)

		// Handle populate.
		go func() {
			var msg ClientMsg
			dec.Decode(&msg)
			copy(arena[0:slotSize], msg.Data)
			enc.Encode(DaemonMsg{Slot: 0})

			// Handle lookup (populates local cache).
			dec.Decode(&msg)
			enc.Encode(DaemonMsg{Hit: true, Slot: 0})

			// Send drain.
			enc.Encode(DaemonMsg{Op: OpDrain})

			// Client will process drain inline (set draining, clear cache, send drained).
			// But we close the client before it can finish.
		}()

		page := makePage(0xCC)
		client.PutPage("cd", 1, page)
		client.GetPage("cd", 1)
		time.Sleep(time.Millisecond) // let drain message arrive

		// Client dies.
		client.Close()

		// Server should see the conn close (read error).
		var msg ClientMsg
		err := dec.Decode(&msg)
		// Either we get OpDrained (client finished drain before close)
		// or an error (client died mid-drain). Both are acceptable.
		if err != nil {
			return // client died before sending drained — expected
		}
		if msg.Op != OpDrained {
			t.Fatalf("expected OpDrained or error, got op=%d", msg.Op)
		}
	})
}

func TestClient_DaemonDiesThenReconnect(t *testing.T) {
	// After daemon dies, a new daemon starts. The client should be
	// able to create a new Client and resume operations.
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)

		// First daemon.
		cc1, sc1 := net.Pipe()
		client1 := newClientForTest(cc1, arena)
		srv1 := newMockServer(sc1, arena)
		go srv1.serve()

		page := makePage(0xEE)
		client1.PutPage("r", 1, page)
		got := client1.GetPage("r", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("first daemon read mismatch")
		}

		// Daemon dies.
		sc1.Close()
		client1.Close()

		// Second daemon starts (same arena — simulates SQLite reload).
		cc2, sc2 := net.Pipe()
		client2 := newClientForTest(cc2, arena)
		srv2 := newMockServer(sc2, arena)
		// Manually restore index (simulates loading from SQLite).
		srv2.index["r:1"] = 0
		go srv2.serve()

		got = client2.GetPage("r", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("second daemon read mismatch")
		}

		client2.Close()
		sc2.Close()
	})
}

func TestClient_MultipleClientsSomeDisconnect(t *testing.T) {
	// Some clients disconnect while others continue operating.
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		const nClients = 4

		type pair struct {
			client *Client
			cc     net.Conn
			sc     net.Conn
			srv    *mockServer
		}
		pairs := make([]pair, nClients)
		for i := range nClients {
			cc, sc := net.Pipe()
			cl := newClientForTest(cc, arena)
			sv := newMockServer(sc, arena)
			go sv.serve()
			pairs[i] = pair{cl, cc, sc, sv}
		}

		page := makePage(0xFF)
		// All clients populate.
		for _, p := range pairs {
			p.client.PutPage("mc", 1, page)
		}

		// Kill clients 0 and 2.
		pairs[0].client.Close()
		pairs[0].sc.Close()
		pairs[2].client.Close()
		pairs[2].sc.Close()

		// Surviving clients should still work.
		for _, i := range []int{1, 3} {
			got := pairs[i].client.GetPage("mc", 1)
			if !bytes.Equal(got, page) {
				t.Fatalf("client %d: read after others died should work", i)
			}
		}

		// Cleanup survivors.
		for _, i := range []int{1, 3} {
			pairs[i].client.Close()
			pairs[i].sc.Close()
		}
	})
}

func TestClient_Dead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		srv := newMockServer(sc, arena)
		go srv.serve()

		if client.Dead() {
			t.Fatal("client should not be dead initially")
		}

		sc.Close()
		time.Sleep(time.Millisecond)

		if !client.Dead() {
			t.Fatal("client should be dead after server close")
		}

		client.Close()
	})
}

func TestClient_DaemonDiesDuringDrainPinHeld(t *testing.T) {
	// Client holds a pin, daemon sends drain, then daemon dies
	// before the client can complete the drain protocol.
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newClientForTest(cc, arena)
		defer client.Close()
		srv := newDrainServer(sc, arena)
		go srv.serve()

		page := makePage(0x77)
		client.PutPage("dp", 1, page)

		// Hold a pin.
		data, unpin := client.GetPageRef("dp", 1)
		if data == nil {
			t.Fatal("expected ref hit")
		}

		// Daemon sends drain — client will try to wait for pin release.
		srv.sendDrain()
		time.Sleep(time.Microsecond) // let drain message be read

		// Daemon crashes while client is waiting for pin release.
		sc.Close()

		// Release the pin — handleDrain is trying to spin-wait on this,
		// but the conn is dead so sendMsg(drained) will fail.
		unpin()
		time.Sleep(time.Millisecond)

		// readLoop exited → draining cleared, conn dead.
		if client.draining.Load() {
			t.Fatal("draining should be cleared after daemon death")
		}
		if !client.Dead() {
			t.Fatal("client should report dead")
		}
	})
}
