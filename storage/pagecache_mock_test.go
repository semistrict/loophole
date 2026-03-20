package storage

import (
	"sync"

	"github.com/semistrict/loophole/internal/safepoint"
)

// memPageCache is a simple in-memory PageCache for tests.
// No daemon, no mmap — just a map.
type memPageCache struct {
	mu    sync.Mutex
	pages map[string][]byte // "layerID:pageIdx" → data
}

func newMemPageCache() *memPageCache {
	return &memPageCache{pages: make(map[string][]byte)}
}

func (c *memPageCache) key(layerID string, pageIdx uint64) string {
	return layerID + ":" + string(rune(pageIdx))
}

func (c *memPageCache) GetPage(layerID string, pageIdx uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.pages[c.key(layerID, pageIdx)]
	if !ok {
		return nil
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return buf
}

func (c *memPageCache) GetPageRef(g safepoint.Guard, layerID string, pageIdx uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.pages[c.key(layerID, pageIdx)]
	if !ok {
		return nil
	}
	g.Register(data)
	return data
}

func (c *memPageCache) PutPage(layerID string, pageIdx uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := make([]byte, len(data))
	copy(buf, data)
	c.pages[c.key(layerID, pageIdx)] = buf
}

func (c *memPageCache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pages)
}

func (c *memPageCache) Close() error { return nil }
