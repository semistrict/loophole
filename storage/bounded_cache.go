package storage

import "sync"

const defaultMaxCacheEntries = 256

// boundedCache is a concurrency-safe bounded cache with random eviction.
// When the cache exceeds maxEntries, a random entry is evicted on insert.
type boundedCache[T any] struct {
	mu         sync.RWMutex
	entries    map[string]T
	maxEntries int
}

func (c *boundedCache[T]) init(maxEntries int) {
	if maxEntries <= 0 {
		maxEntries = defaultMaxCacheEntries
	}
	c.entries = make(map[string]T)
	c.maxEntries = maxEntries
}

func (c *boundedCache[T]) get(key string) (T, bool) {
	c.mu.RLock()
	v, ok := c.entries[key]
	c.mu.RUnlock()
	return v, ok
}

func (c *boundedCache[T]) put(key string, val T) {
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]T)
		if c.maxEntries <= 0 {
			c.maxEntries = defaultMaxCacheEntries
		}
	}
	c.entries[key] = val
	if len(c.entries) > c.maxEntries {
		// Evict one random entry (map iteration order is random in Go).
		for k := range c.entries {
			if k != key {
				delete(c.entries, k)
				break
			}
		}
	}
	c.mu.Unlock()
}

func (c *boundedCache[T]) delete(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

func (c *boundedCache[T]) clear() {
	c.mu.Lock()
	c.entries = make(map[string]T)
	c.mu.Unlock()
}
