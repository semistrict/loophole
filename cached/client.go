// Package cached implements the client side of the page cache daemon
// (loophole-cached). The daemon server code lives in cmd/loophole-cached.
package cached

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// conn is a single connection to the page cache daemon. It provides
// zero-copy reads via a shared read-only mmap of the arena.
type conn struct {
	nc      net.Conn
	writeMu sync.Mutex // protects writes to nc
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

// dial connects to the cache daemon at dir/cached.sock and mmaps
// the arena file read-only.
func dial(dir string) (*conn, error) {
	sockPath := SocketPath(dir)
	nc, err := net.Dial("unix", sockPath)
	if err != nil {
		return nil, err
	}

	arenaPath := filepath.Join(dir, "arena")
	arenaFile, err := os.Open(arenaPath)
	if err != nil {
		safeClose(nc)
		return nil, fmt.Errorf("open arena: %w", err)
	}

	fi, err := arenaFile.Stat()
	if err != nil {
		safeClose(nc)
		safeClose(arenaFile)
		return nil, fmt.Errorf("stat arena: %w", err)
	}

	arenaSize := fi.Size()
	if arenaSize == 0 {
		safeClose(nc)
		safeClose(arenaFile)
		return nil, fmt.Errorf("arena is empty")
	}

	arena, err := unix.Mmap(int(arenaFile.Fd()), 0, int(arenaSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		safeClose(nc)
		safeClose(arenaFile)
		return nil, fmt.Errorf("mmap arena: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		nc:         nc,
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

func (c *conn) readLoop(ctx context.Context) {
	defer close(c.readDone)
	defer close(c.respCh)         // unblock doRequest on conn death
	defer c.draining.Store(false) // unstick drain state on conn death
	decoder := json.NewDecoder(c.nc)
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

func (c *conn) handleDrain() {
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

func (c *conn) handleResume() {
	c.draining.Store(false)
}

func (c *conn) sendMsg(msg any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.encoder.Encode(msg)
}

func (c *conn) doRequest(req ClientMsg) (DaemonMsg, error) {
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

func (c *conn) getPage(layerID string, pageIdx uint64) []byte {
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

func (c *conn) getPageRef(layerID string, pageIdx uint64) ([]byte, func()) {
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

func (c *conn) putPage(layerID string, pageIdx uint64, data []byte) {
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

func (c *conn) invalidatePage(layerID string, pageIdx uint64) {
	if c.closed.Load() {
		return
	}
	_, _ = c.doRequest(ClientMsg{Op: OpDelete, LayerID: layerID, PageIdx: pageIdx})
	c.localMu.Lock()
	delete(c.localCache, localKey{layerID, pageIdx})
	c.localMu.Unlock()
}

func (c *conn) count() (int, error) {
	resp, err := c.doRequest(ClientMsg{Op: OpCount})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// dead reports whether the connection to the daemon is dead.
func (c *conn) dead() bool {
	select {
	case <-c.readDone:
		return true
	default:
		return false
	}
}

func (c *conn) close() error {
	c.closed.Store(true)
	c.cancel()
	_ = c.nc.Close() // unblocks readLoop
	<-c.readDone     // wait for readLoop to finish

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

// --- PageCache (public API with auto-reconnect) ---

// PageCache connects to the page cache daemon (loophole-cached) and
// provides zero-copy page reads via a shared mmap arena.
//
// If the daemon dies, the PageCache detects the dead connection and
// automatically respawns the daemon and reconnects.
type PageCache struct {
	dir    string
	mu     sync.Mutex
	client *conn
	closed bool
}

// NewPageCache connects to the page cache daemon for dir.
// If no daemon is running, one is spawned as a detached subprocess.
func NewPageCache(dir string) (*PageCache, error) {
	if err := EnsureDaemon(dir); err != nil {
		return nil, err
	}
	client, err := dial(dir)
	if err != nil {
		return nil, err
	}
	return &PageCache{dir: dir, client: client}, nil
}

// getConn returns a live connection, reconnecting if the previous one died.
// Returns nil if reconnection fails (operations degrade gracefully).
func (c *PageCache) getConn() *conn {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.client != nil && !c.client.dead() {
		return c.client
	}

	// Clean up dead connection.
	if c.client != nil {
		util.SafeRun(func() error { return c.client.close() }, "close dead pagecache conn")
		c.client = nil
	}

	// Spawn daemon if needed, reconnect.
	if err := EnsureDaemon(c.dir); err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	client, err := dial(c.dir)
	if err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	c.client = client
	return c.client
}

// GetPage returns a copy of the cached page, or nil on miss.
func (c *PageCache) GetPage(layerID string, pageIdx uint64) []byte {
	cn := c.getConn()
	if cn == nil {
		return nil
	}
	return cn.getPage(layerID, pageIdx)
}

// GetPageRef returns a zero-copy reference into the mmap'd arena.
// The caller MUST call the returned unpin function when done.
// Returns (nil, nil) on miss.
func (c *PageCache) GetPageRef(layerID string, pageIdx uint64) ([]byte, func()) {
	cn := c.getConn()
	if cn == nil {
		return nil, nil
	}
	return cn.getPageRef(layerID, pageIdx)
}

// PutPage inserts a page into the cache.
func (c *PageCache) PutPage(layerID string, pageIdx uint64, data []byte) {
	cn := c.getConn()
	if cn == nil {
		return
	}
	cn.putPage(layerID, pageIdx, data)
}

// InvalidatePage invalidates a page in the cache.
func (c *PageCache) InvalidatePage(layerID string, pageIdx uint64) {
	cn := c.getConn()
	if cn == nil {
		return
	}
	cn.invalidatePage(layerID, pageIdx)
}

// Count returns the number of pages in the cache.
func (c *PageCache) Count() (int, error) {
	cn := c.getConn()
	if cn == nil {
		return 0, nil
	}
	return cn.count()
}

// Dead reports whether the connection to the daemon is dead.
func (c *PageCache) Dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client == nil || c.client.dead()
}

// Close closes the client connection. Safe to call multiple times.
func (c *PageCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.client != nil {
		return c.client.close()
	}
	return nil
}

// newConnForTest creates a conn from an already-connected net.Conn and
// a shared arena byte slice. No files, mmap, or flock — safe for synctest.
func newConnForTest(nc net.Conn, arena []byte) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		nc:         nc,
		encoder:    json.NewEncoder(nc),
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

// newPageCacheForTest wraps a test conn in a PageCache.
func newPageCacheForTest(c *conn) *PageCache {
	return &PageCache{client: c}
}

func safeClose(c interface{ Close() error }) {
	_ = c.Close()
}
