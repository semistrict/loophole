package cached

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
	"golang.org/x/sys/unix"
)

// slotSize is the arena slot size — must match the daemon's page size.
// Defined here (not imported from storage) to avoid a circular dependency.
const slotSize = 4096

// Client connects to a page cache daemon over UDS and provides
// zero-copy reads via a shared read-only mmap of the arena.
type Client struct {
	conn    net.Conn
	writeMu sync.Mutex // protects writes to conn
	reqMu   sync.Mutex // serializes request-response pairs
	encoder *json.Encoder
	cancel  context.CancelFunc

	arenaFile  *os.File
	arena      []byte // read-only mmap
	arenaSlots int

	localCache map[localKey]int
	localMu    sync.Mutex

	draining atomic.Bool
	pins     atomic.Int32

	respCh   chan DaemonMsg
	readDone chan struct{}

	closed atomic.Bool
}

type localKey struct {
	layerID string
	pageIdx uint64
}

// Connect dials the cache daemon at dir/cached.sock and mmaps
// the arena file read-only.
func Connect(dir string) (*Client, error) {
	sockPath := SocketPath(dir)
	nc, err := net.Dial("unix", sockPath)
	if err != nil {
		return nil, err
	}

	arenaPath := filepath.Join(dir, "arena")
	arenaFile, err := os.Open(arenaPath)
	if err != nil {
		util_close(nc)
		return nil, fmt.Errorf("open arena: %w", err)
	}

	fi, err := arenaFile.Stat()
	if err != nil {
		util_close(nc)
		util_close(arenaFile)
		return nil, fmt.Errorf("stat arena: %w", err)
	}

	arenaSize := fi.Size()
	if arenaSize == 0 {
		util_close(nc)
		util_close(arenaFile)
		return nil, fmt.Errorf("arena is empty")
	}

	arena, err := unix.Mmap(int(arenaFile.Fd()), 0, int(arenaSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		util_close(nc)
		util_close(arenaFile)
		return nil, fmt.Errorf("mmap arena: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:       nc,
		encoder:    json.NewEncoder(nc),
		cancel:     cancel,
		arenaFile:  arenaFile,
		arena:      arena,
		arenaSlots: int(arenaSize / slotSize),
		localCache: make(map[localKey]int),
		respCh:     make(chan DaemonMsg, 1),
		readDone:   make(chan struct{}),
	}

	go c.readLoop(ctx)
	return c, nil
}

func (c *Client) readLoop(ctx context.Context) {
	defer close(c.readDone)
	defer close(c.respCh)         // unblock doRequest on conn death
	defer c.draining.Store(false) // unstick drain state on conn death
	decoder := json.NewDecoder(c.conn)
	for {
		var resp DaemonMsg
		if err := decoder.Decode(&resp); err != nil {
			return
		}
		switch resp.Op {
		case OpDrain:
			c.handleDrain()
		case OpResume:
			c.handleResume()
		default:
			select {
			case c.respCh <- resp:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Client) handleDrain() {
	c.draining.Store(true)
	// Wait for all pins to be released (~1µs per pin).
	for c.pins.Load() > 0 {
		time.Sleep(time.Microsecond)
	}
	// Clear local cache — all prior slot numbers are invalid after eviction.
	c.localMu.Lock()
	clear(c.localCache)
	c.localMu.Unlock()
	// Notify daemon.
	util.SafeRun(func() error { return c.sendMsg(ClientMsg{Op: OpDrained}) }, "send drained")
}

func (c *Client) handleResume() {
	c.draining.Store(false)
}

func (c *Client) sendMsg(msg any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.encoder.Encode(msg)
}

func (c *Client) doRequest(req ClientMsg) (DaemonMsg, error) {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()

	if c.closed.Load() {
		return DaemonMsg{}, fmt.Errorf("client closed")
	}

	if err := c.sendMsg(req); err != nil {
		return DaemonMsg{}, err
	}

	resp, ok := <-c.respCh
	if !ok {
		return DaemonMsg{}, fmt.Errorf("connection closed")
	}
	return resp, nil
}

// --- Public API ---

// GetPage returns a copy of the cached page, or nil on miss.
func (c *Client) GetPage(layerID string, pageIdx uint64) []byte {
	if c.closed.Load() {
		return nil
	}
	key := localKey{layerID, pageIdx}

	// Fast path: local cache.
	c.localMu.Lock()
	slot, ok := c.localCache[key]
	c.localMu.Unlock()

	if ok {
		c.pins.Add(1)
		if c.draining.Load() {
			c.pins.Add(-1)
			metrics.CacheDrainSkips.Inc()
			return nil
		}
		metrics.CacheLocalHits.Inc()
		buf := make([]byte, slotSize)
		off := slot * slotSize
		copy(buf, c.arena[off:off+slotSize])
		c.pins.Add(-1)
		return buf
	}

	if c.draining.Load() {
		metrics.CacheDrainSkips.Inc()
		return nil
	}

	// Slow path: IPC to daemon.
	metrics.CacheIPCLookups.Inc()
	resp, err := c.doRequest(ClientMsg{Op: OpLookup, LayerID: layerID, PageIdx: pageIdx})
	if err != nil || !resp.Hit {
		return nil
	}

	c.pins.Add(1)
	if c.draining.Load() {
		c.pins.Add(-1)
		metrics.CacheDrainSkips.Inc()
		return nil
	}
	buf := make([]byte, slotSize)
	off := resp.Slot * slotSize
	copy(buf, c.arena[off:off+slotSize])
	c.pins.Add(-1)

	c.localMu.Lock()
	c.localCache[key] = resp.Slot
	c.localMu.Unlock()

	return buf
}

// GetPageRef returns a zero-copy reference into the mmap'd arena.
// The caller MUST call the returned unpin function when done.
// Returns (nil, nil) on miss.
func (c *Client) GetPageRef(layerID string, pageIdx uint64) ([]byte, func()) {
	if c.closed.Load() {
		return nil, nil
	}
	key := localKey{layerID, pageIdx}

	c.localMu.Lock()
	slot, ok := c.localCache[key]
	c.localMu.Unlock()

	if ok {
		c.pins.Add(1)
		if c.draining.Load() {
			c.pins.Add(-1)
			metrics.CacheDrainSkips.Inc()
			return nil, nil
		}
		metrics.CacheLocalHits.Inc()
		off := slot * slotSize
		return c.arena[off : off+slotSize], func() { c.pins.Add(-1) }
	}

	if c.draining.Load() {
		metrics.CacheDrainSkips.Inc()
		return nil, nil
	}

	metrics.CacheIPCLookups.Inc()
	resp, err := c.doRequest(ClientMsg{Op: OpLookup, LayerID: layerID, PageIdx: pageIdx})
	if err != nil || !resp.Hit {
		return nil, nil
	}
	slot = resp.Slot
	c.localMu.Lock()
	c.localCache[key] = slot
	c.localMu.Unlock()

	c.pins.Add(1)
	if c.draining.Load() {
		c.pins.Add(-1)
		metrics.CacheDrainSkips.Inc()
		return nil, nil
	}
	off := slot * slotSize
	return c.arena[off : off+slotSize], func() { c.pins.Add(-1) }
}

// PutPage inserts a page into the cache.
func (c *Client) PutPage(layerID string, pageIdx uint64, data []byte) {
	if c.closed.Load() || c.draining.Load() {
		return
	}
	resp, err := c.doRequest(ClientMsg{Op: OpPopulate, LayerID: layerID, PageIdx: pageIdx, Data: data})
	if err != nil || resp.Error != "" {
		return
	}
	c.localMu.Lock()
	c.localCache[localKey{layerID, pageIdx}] = resp.Slot
	c.localMu.Unlock()
}

// DeletePage removes a page from the cache.
func (c *Client) DeletePage(layerID string, pageIdx uint64) {
	if c.closed.Load() {
		return
	}
	_, _ = c.doRequest(ClientMsg{Op: OpDelete, LayerID: layerID, PageIdx: pageIdx})
	c.localMu.Lock()
	delete(c.localCache, localKey{layerID, pageIdx})
	c.localMu.Unlock()
}

// Count returns the number of pages in the cache.
func (c *Client) Count() (int, error) {
	resp, err := c.doRequest(ClientMsg{Op: OpCount})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// Close closes the client connection and unmaps the arena.
func (c *Client) Close() error {
	c.closed.Store(true)
	c.cancel()
	_ = c.conn.Close() // unblocks readLoop
	<-c.readDone       // wait for readLoop to finish

	var errs []error
	if c.arena != nil {
		errs = append(errs, unix.Munmap(c.arena))
		c.arena = nil
	}
	if c.arenaFile != nil {
		errs = append(errs, c.arenaFile.Close())
	}
	return errors.Join(errs...)
}

// Dead reports whether the connection to the daemon is dead.
// Non-blocking: returns true once readLoop has exited.
func (c *Client) Dead() bool {
	select {
	case <-c.readDone:
		return true
	default:
		return false
	}
}

// newClientForTest creates a client from an already-connected net.Conn and
// a shared arena byte slice. No files, mmap, or flock — safe for synctest.
func newClientForTest(conn net.Conn, arena []byte) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:       conn,
		encoder:    json.NewEncoder(conn),
		cancel:     cancel,
		arena:      arena,
		arenaSlots: len(arena) / slotSize,
		localCache: make(map[localKey]int),
		respCh:     make(chan DaemonMsg, 1),
		readDone:   make(chan struct{}),
	}
	go c.readLoop(ctx)
	return c
}

func util_close(c interface{ Close() error }) {
	_ = c.Close()
}
