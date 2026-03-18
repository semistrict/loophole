package storage

import "sync"

// singleflight deduplicates concurrent calls to the same key.
// Only one call per key executes; other callers block and share the result.
type singleflight[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

func (sf *singleflight[T]) do(key string, fn func() (T, error)) (T, error) {
	sf.mu.Lock()
	if sf.calls == nil {
		sf.calls = make(map[string]*call[T])
	}
	if c, ok := sf.calls[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call[T]{}
	c.wg.Add(1)
	sf.calls[key] = c
	sf.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.calls, key)
	sf.mu.Unlock()

	return c.val, c.err
}
