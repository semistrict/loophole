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

	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/metrics"
	"golang.org/x/sys/unix"
)

// SlotSize is the arena slot size — must match the daemon's page size.
// Defined here (not imported from storage) to avoid a circular dependency.
const SlotSize = 4096

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

	safepoint *safepoint.Safepoint

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
func dial(dir string, sp *safepoint.Safepoint) (*conn, error) {
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
		arenaSlots: int(arenaSize / SlotSize),
		localCache: make(map[localKey]int),
		safepoint:  sp,
		respCh:     make(chan DaemonMsg, 1),
		readDone:   make(chan struct{}),
	}

	go c.readLoop(ctx)
	return c, nil
}

func (c *conn) readLoop(ctx context.Context) {
	defer close(c.readDone)
	defer close(c.respCh) // unblock doRequest on conn death
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
	c.safepoint.Lock() // blocks until all readers exit
	c.localMu.Lock()
	clear(c.localCache)
	c.localMu.Unlock()
	util.SafeRun(func() error { return c.sendMsg(ClientMsg{Op: OpDrained}) }, "send drained")
	// safepoint stays locked until handleResume
}

func (c *conn) handleResume() {
	c.safepoint.Unlock()
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

	c.localMu.Lock()
	slot, ok := c.localCache[key]
	c.localMu.Unlock()

	if ok {
		g := c.safepoint.Enter()
		metrics.CacheLocalHits.Inc()
		buf := make([]byte, SlotSize)
		off := slot * SlotSize
		copy(buf, c.arena[off:off+SlotSize])
		g.Exit()
		return buf
	}

	metrics.CacheIPCLookups.Inc()
	resp, err := c.doRequest(ClientMsg{Op: OpLookup, LayerID: layerID, PageIdx: pageIdx})
	if err != nil || !resp.Hit {
		return nil
	}

	g := c.safepoint.Enter()
	buf := make([]byte, SlotSize)
	off := resp.Slot * SlotSize
	copy(buf, c.arena[off:off+SlotSize])
	g.Exit()

	c.localMu.Lock()
	c.localCache[key] = resp.Slot
	c.localMu.Unlock()

	return buf
}

// getPageRef returns a zero-copy slice into the arena. The caller must
// already hold the safepoint via the provided Guard.
func (c *conn) getPageRef(g safepoint.Guard, layerID string, pageIdx uint64) []byte {
	if c.closed.Load() {
		return nil
	}
	key := localKey{layerID, pageIdx}

	c.localMu.Lock()
	slot, ok := c.localCache[key]
	c.localMu.Unlock()

	if ok {
		metrics.CacheLocalHits.Inc()
		off := slot * SlotSize
		ref := c.arena[off : off+SlotSize]
		g.Register(ref)
		return ref
	}

	metrics.CacheIPCLookups.Inc()
	resp, err := c.doRequest(ClientMsg{Op: OpLookup, LayerID: layerID, PageIdx: pageIdx})
	if err != nil || !resp.Hit {
		return nil
	}

	c.localMu.Lock()
	c.localCache[localKey{layerID, pageIdx}] = resp.Slot
	c.localMu.Unlock()

	off := resp.Slot * SlotSize
	ref := c.arena[off : off+SlotSize]
	g.Register(ref)
	return ref
}

func (c *conn) putPage(layerID string, pageIdx uint64, data []byte) {
	if c.closed.Load() {
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
	_ = c.nc.Close()
	<-c.readDone

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
type PageCache struct {
	dir       string
	safepoint *safepoint.Safepoint
	mu        sync.Mutex
	client    *conn
	closed    bool
}

// NewPageCache connects to the page cache daemon for dir.
func NewPageCache(dir string, sp *safepoint.Safepoint) (*PageCache, error) {
	if err := EnsureDaemon(dir); err != nil {
		return nil, err
	}
	client, err := dial(dir, sp)
	if err != nil {
		return nil, err
	}
	return &PageCache{dir: dir, safepoint: sp, client: client}, nil
}

func (c *PageCache) getConn() *conn {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.client != nil && !c.client.dead() {
		return c.client
	}

	if c.client != nil {
		util.SafeRun(func() error { return c.client.close() }, "close dead pagecache conn")
		c.client = nil
	}

	if err := EnsureDaemon(c.dir); err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	client, err := dial(c.dir, c.safepoint)
	if err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	c.client = client
	return c.client
}

func (c *PageCache) GetPage(layerID string, pageIdx uint64) []byte {
	cn := c.getConn()
	if cn == nil {
		return nil
	}
	return cn.getPage(layerID, pageIdx)
}

// GetPageRef returns a zero-copy reference into the mmap'd arena.
// The caller must hold a Guard from the safepoint.
func (c *PageCache) GetPageRef(g safepoint.Guard, layerID string, pageIdx uint64) []byte {
	cn := c.getConn()
	if cn == nil {
		return nil
	}
	return cn.getPageRef(g, layerID, pageIdx)
}

func (c *PageCache) PutPage(layerID string, pageIdx uint64, data []byte) {
	cn := c.getConn()
	if cn == nil {
		return
	}
	cn.putPage(layerID, pageIdx, data)
}

func (c *PageCache) InvalidatePage(layerID string, pageIdx uint64) {
	cn := c.getConn()
	if cn == nil {
		return
	}
	cn.invalidatePage(layerID, pageIdx)
}

func (c *PageCache) Count() (int, error) {
	cn := c.getConn()
	if cn == nil {
		return 0, nil
	}
	return cn.count()
}

func (c *PageCache) Dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client == nil || c.client.dead()
}

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

func newConnForTest(nc net.Conn, arena []byte, sp *safepoint.Safepoint) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		nc:         nc,
		encoder:    json.NewEncoder(nc),
		cancel:     cancel,
		arena:      arena,
		arenaSlots: len(arena) / SlotSize,
		localCache: make(map[localKey]int),
		safepoint:  sp,
		respCh:     make(chan DaemonMsg, 1),
		readDone:   make(chan struct{}),
	}
	go c.readLoop(ctx)
	return c
}

// NewInProcess creates a PageCache connected to an in-process server via
// the given net.Conn and shared arena. No daemon binary, no mmap, no files.
// Use with cachedserver.StartServerWithListener + net.Pipe for testing.
func TestOnlyNewInProcess(nc net.Conn, arena []byte, sp *safepoint.Safepoint) *PageCache {
	c := newConnForTest(nc, arena, sp)
	return &PageCache{safepoint: sp, client: c}
}

func newPageCacheForTest(c *conn) *PageCache {
	return &PageCache{client: c, safepoint: c.safepoint}
}

func safeClose(c interface{ Close() error }) {
	_ = c.Close()
}
