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

	"github.com/semistrict/loophole/internal/safepoint"
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
		off := slot * SlotSize
		copy(s.arena[off:off+SlotSize], msg.Data)
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

func makeArena(slots int) []byte { return make([]byte, slots*SlotSize) }

func makePage(b byte) []byte { return bytes.Repeat([]byte{b}, SlotSize) }

func setup(t *testing.T, arenaSlots int) (*PageCache, *mockServer, *safepoint.Safepoint) {
	t.Helper()
	sp := safepoint.New()
	arena := makeArena(arenaSlots)
	clientConn, serverConn := net.Pipe()
	client := newPageCacheForTest(newConnForTest(clientConn, arena, sp))
	server := newMockServer(serverConn, arena)
	go server.serve()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})
	return client, server, sp
}

func setupDrain(t *testing.T, arenaSlots int) (*PageCache, *drainServer, *safepoint.Safepoint) {
	t.Helper()
	sp := safepoint.New()
	arena := makeArena(arenaSlots)
	clientConn, serverConn := net.Pipe()
	client := newPageCacheForTest(newConnForTest(clientConn, arena, sp))
	server := newDrainServer(serverConn, arena)
	go server.serve()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})
	return client, server, sp
}

func setupDrainNoSynctest(t *testing.T, arenaSlots int) (*PageCache, *drainServer, *safepoint.Safepoint) {
	t.Helper()
	sp := safepoint.New()
	arena := makeArena(arenaSlots)
	clientConn, serverConn := net.Pipe()
	client := newPageCacheForTest(newConnForTest(clientConn, arena, sp))
	server := newDrainServer(serverConn, arena)
	go server.serve()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})
	return client, server, sp
}

// --- Tests ---

func TestClient_PutGetDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _, _ := setup(t, 64)

		page := makePage(0xAB)
		client.PutPage("layer1", 1, page)

		got := client.GetPage("layer1", 1)
		if !bytes.Equal(got, page) {
			t.Fatalf("got %x, want %x", got[:4], page[:4])
		}

		client.InvalidatePage("layer1", 1)
		if got := client.GetPage("layer1", 1); got != nil {
			t.Fatal("expected nil after delete")
		}
	})
}

func TestClient_GetPageRef(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _, sp := setup(t, 64)

		page := makePage(0xCD)
		client.PutPage("ref", 1, page)

		g := sp.Enter()
		data := client.GetPageRef(g, "ref", 1)
		if data == nil {
			g.Exit()
			t.Fatal("expected hit")
		}
		if !bytes.Equal(data, page) {
			g.Exit()
			t.Fatal("data mismatch")
		}
		g.Exit()

		// Miss returns nil.
		g = sp.Enter()
		data = client.GetPageRef(g, "missing", 0)
		g.Exit()
		if data != nil {
			t.Fatal("expected nil for miss")
		}
	})
}

func TestClient_LocalCacheHit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, srv, _ := setup(t, 64)

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
		client, _, _ := setup(t, 64)

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
		client, _, _ := setup(t, 256)

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
		client, srv, _ := setupDrain(t, 64)

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

func TestClient_DrainWaitsForReaders(t *testing.T) {
	// No synctest — mutex ops are not durably blocking so synctest
	// can't advance time past a Lock blocked on an RLock.
	client, srv, sp := setupDrainNoSynctest(t, 64)

	page := makePage(0xBB)
	client.PutPage("pin", 1, page)

	// Hold the safepoint.
	g := sp.Enter()

	// Trigger drain — should block until reader releases.
	srv.sendDrain()

	drained := make(chan struct{})
	go func() {
		<-srv.drainedCh
		close(drained)
	}()

	select {
	case <-drained:
		t.Fatal("drain should not complete while read is held")
	case <-time.After(10 * time.Millisecond):
	}

	// Release the guard.
	g.Exit()

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain should complete after read release")
	}

	srv.sendResume()
}

func TestClient_MultipleClientsDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		const nClients = 4

		type pair struct {
			client *PageCache
			srv    *drainServer
		}
		pairs := make([]pair, nClients)
		for i := range nClients {
			cc, sc := net.Pipe()
			cl := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
			sv := newDrainServer(sc, arena)
			go sv.serve()
			t.Cleanup(func() { cl.Close(); sc.Close() })
			pairs[i] = pair{cl, sv}
		}

		page := makePage(0xCC)
		for _, p := range pairs {
			p.client.PutPage("m", 1, page)
		}

		for _, p := range pairs {
			got := p.client.GetPage("m", 1)
			if !bytes.Equal(got, page) {
				t.Fatal("pre-drain read mismatch")
			}
		}

		// Drain and resume each client sequentially — with a global
		// safepoint, EnterExclusive serializes across clients.
		for _, p := range pairs {
			p.srv.sendDrain()
			<-p.srv.drainedCh
			p.srv.sendResume()
			time.Sleep(time.Millisecond)
		}

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
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
		srv := newMockServer(sc, arena)
		go srv.serve()

		page := makePage(0xDD)
		client.PutPage("cl", 1, page)

		client.Close()
		sc.Close()

		if got := client.GetPage("cl", 1); got != nil {
			t.Fatal("expected nil from closed client")
		}
		g := safepoint.New().Enter()
		data := client.GetPageRef(g, "cl", 1)
		g.Exit()
		if data != nil {
			t.Fatal("expected nil ref from closed client")
		}
	})
}

// --- Crash / unexpected death tests ---

func TestClient_DaemonDiesWhileIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
		srv := newMockServer(sc, arena)
		go srv.serve()

		page := makePage(0x11)
		client.PutPage("d", 1, page)

		got := client.GetPage("d", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("pre-crash read mismatch")
		}

		sc.Close()
		time.Sleep(time.Millisecond)

		if !client.Dead() {
			t.Fatal("client should report dead after daemon crash")
		}

		client.PutPage("d", 2, makePage(0x22))
	})
}

func TestClient_DaemonDiesDuringRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
		defer client.Close()

		go func() {
			dec := json.NewDecoder(sc)
			enc := json.NewEncoder(sc)

			var msg ClientMsg
			if err := dec.Decode(&msg); err != nil {
				return
			}
			copy(arena[0:SlotSize], msg.Data)
			enc.Encode(DaemonMsg{Slot: 0})

			dec.Decode(&msg)
			sc.Close()
		}()

		page := makePage(0xAA)
		client.PutPage("crash", 1, page)

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
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
		srv := newDrainServer(sc, arena)
		go srv.serve()

		page := makePage(0xBB)
		client.PutPage("dd", 1, page)
		client.GetPage("dd", 1)

		srv.sendDrain()
		<-srv.drainedCh

		sc.Close()
		time.Sleep(time.Millisecond)

		got := client.GetPage("dd", 1)
		if got != nil {
			t.Fatal("expected nil after daemon death during drain")
		}

		if !client.Dead() {
			t.Fatal("client should report dead after daemon death")
		}
	})
}

func TestClient_ClientDiesDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		cc, sc := net.Pipe()
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))

		enc := json.NewEncoder(sc)
		dec := json.NewDecoder(sc)

		go func() {
			var msg ClientMsg
			dec.Decode(&msg)
			copy(arena[0:SlotSize], msg.Data)
			enc.Encode(DaemonMsg{Slot: 0})

			dec.Decode(&msg)
			enc.Encode(DaemonMsg{Hit: true, Slot: 0})

			enc.Encode(DaemonMsg{Op: OpDrain})
		}()

		page := makePage(0xCC)
		client.PutPage("cd", 1, page)
		client.GetPage("cd", 1)
		time.Sleep(time.Millisecond)

		client.Close()

		var msg ClientMsg
		err := dec.Decode(&msg)
		if err != nil {
			return
		}
		if msg.Op != OpDrained {
			t.Fatalf("expected OpDrained or error, got op=%d", msg.Op)
		}
	})
}

func TestClient_DaemonDiesThenReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)

		cc1, sc1 := net.Pipe()
		client1 := newPageCacheForTest(newConnForTest(cc1, arena, safepoint.New()))
		srv1 := newMockServer(sc1, arena)
		go srv1.serve()

		page := makePage(0xEE)
		client1.PutPage("r", 1, page)
		got := client1.GetPage("r", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("first daemon read mismatch")
		}

		sc1.Close()
		client1.Close()

		cc2, sc2 := net.Pipe()
		client2 := newPageCacheForTest(newConnForTest(cc2, arena, safepoint.New()))
		srv2 := newMockServer(sc2, arena)
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
	synctest.Test(t, func(t *testing.T) {
		arena := makeArena(64)
		const nClients = 4

		type pair struct {
			client *PageCache
			cc     net.Conn
			sc     net.Conn
			srv    *mockServer
		}
		pairs := make([]pair, nClients)
		for i := range nClients {
			cc, sc := net.Pipe()
			cl := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
			sv := newMockServer(sc, arena)
			go sv.serve()
			pairs[i] = pair{cl, cc, sc, sv}
		}

		page := makePage(0xFF)
		for _, p := range pairs {
			p.client.PutPage("mc", 1, page)
		}

		pairs[0].client.Close()
		pairs[0].sc.Close()
		pairs[2].client.Close()
		pairs[2].sc.Close()

		for _, i := range []int{1, 3} {
			got := pairs[i].client.GetPage("mc", 1)
			if !bytes.Equal(got, page) {
				t.Fatalf("client %d: read after others died should work", i)
			}
		}

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
		client := newPageCacheForTest(newConnForTest(cc, arena, safepoint.New()))
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

func TestClient_DaemonDiesDuringDrainReadHeld(t *testing.T) {
	client, srv, sp := setupDrainNoSynctest(t, 64)

	page := makePage(0x77)
	client.PutPage("dp", 1, page)

	// Hold the safepoint.
	g := sp.Enter()

	srv.sendDrain()
	time.Sleep(10 * time.Millisecond)

	// Daemon crashes while waiting for safepoint.
	srv.conn.Close()

	g.Exit()
	time.Sleep(10 * time.Millisecond)

	if !client.Dead() {
		t.Fatal("client should report dead")
	}
}
