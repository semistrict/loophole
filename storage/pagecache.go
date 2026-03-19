package storage

import (
	"log/slog"
	"sync"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/internal/util"
)

// cacheKey identifies a cached logical page for an immutable source layer.
// Writable layers do not use the persistent page cache.
type cacheKey struct {
	LayerID string
	PageIdx PageIdx
}

// PageCache is a client to the page cache daemon (loophole-cached).
// The daemon owns a shared mmap arena; clients read pages via a
// read-only mmap of the same file (zero-copy data plane).
//
// If the daemon dies, getClient() detects the dead connection and
// automatically respawns the daemon and reconnects.
type PageCache struct {
	dir    string
	mu     sync.Mutex
	client *cached.Client
	closed bool
}

// NewPageCache connects to the page cache daemon for dir.
// If no daemon is running, one is spawned as a detached subprocess.
func NewPageCache(dir string) (*PageCache, error) {
	if err := cached.EnsureDaemon(dir); err != nil {
		return nil, err
	}
	client, err := cached.Connect(dir)
	if err != nil {
		return nil, err
	}
	return &PageCache{dir: dir, client: client}, nil
}

// getClient returns a live client, reconnecting if the previous one died.
// Returns nil if reconnection fails (operations degrade gracefully).
func (c *PageCache) getClient() *cached.Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.client != nil && !c.client.Dead() {
		return c.client
	}

	// Clean up dead client.
	if c.client != nil {
		util.SafeClose(c.client, "close dead pagecache client")
		c.client = nil
	}

	// Spawn daemon if needed, reconnect.
	if err := cached.EnsureDaemon(c.dir); err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	client, err := cached.Connect(c.dir)
	if err != nil {
		slog.Warn("pagecache: reconnect failed", "error", err)
		return nil
	}
	c.client = client
	return c.client
}

// GetPage returns a copy of the cached page, or nil on miss.
func (c *PageCache) GetPage(key cacheKey) []byte {
	cl := c.getClient()
	if cl == nil {
		return nil
	}
	return cl.GetPage(key.LayerID, uint64(key.PageIdx))
}

// GetPageRef returns a slice pointing directly into the mmap'd arena without
// copying. The caller MUST call the returned unpin function when done.
// Returns (nil, nil) on cache miss.
func (c *PageCache) GetPageRef(key cacheKey) ([]byte, func()) {
	cl := c.getClient()
	if cl == nil {
		return nil, nil
	}
	return cl.GetPageRef(key.LayerID, uint64(key.PageIdx))
}

// PutPage inserts a page into the cache.
func (c *PageCache) PutPage(key cacheKey, data []byte) {
	cl := c.getClient()
	if cl == nil {
		return
	}
	cl.PutPage(key.LayerID, uint64(key.PageIdx), data)
}

// DeletePage removes a page from the cache.
func (c *PageCache) DeletePage(key cacheKey) {
	cl := c.getClient()
	if cl == nil {
		return
	}
	cl.DeletePage(key.LayerID, uint64(key.PageIdx))
}

// CountPages returns the number of pages in the cache.
func (c *PageCache) CountPages() (int, error) {
	cl := c.getClient()
	if cl == nil {
		return 0, nil
	}
	return cl.Count()
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
		return c.client.Close()
	}
	return nil
}
