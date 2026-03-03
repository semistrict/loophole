package loophole

import "sync"

// XXX: we should probably just use sync.Map directly

// SyncMap is a typed wrapper around sync.Map.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (m *SyncMap[K, V]) Load(key K) (V, bool) {
	v, ok := m.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (m *SyncMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	v, loaded := m.m.LoadOrStore(key, value)
	return v.(V), loaded
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *SyncMap[K, V]) Range(fn func(key K, value V) bool) { // XXX: replace with range over func
	m.m.Range(func(k, v any) bool {
		return fn(k.(K), v.(V))
	})
}
