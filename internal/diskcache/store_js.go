//go:build js

package diskcache

import "fmt"

// Store is the platform-specific storage backend for DiskCache.
type Store interface {
	ReadSlot(slot int) ([]byte, error)
	ReadSlotPinned(slot int) ([]byte, error)
	WriteSlot(slot int, data []byte) error
	AllocArena(maxSlots int) error
	ReadBlob(key string) ([]byte, error)
	WriteBlob(key string, data []byte) error
	DeleteBlob(key string) error
	FreeSpace() int64
	MinReserve() int64
	Close() error
}

// indexedDBStore is a stub for WASM builds.
type indexedDBStore struct{}

func newIndexedDBStore() (*indexedDBStore, error) {
	return &indexedDBStore{}, nil
}

func newDefaultStore(_ string) (Store, error) {
	return newIndexedDBStore()
}

func (s *indexedDBStore) AllocArena(maxSlots int) error { return nil }
func (s *indexedDBStore) ReadSlot(slot int) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *indexedDBStore) ReadSlotPinned(slot int) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *indexedDBStore) WriteSlot(slot int, data []byte) error { return fmt.Errorf("not implemented") }
func (s *indexedDBStore) ReadBlob(key string) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *indexedDBStore) WriteBlob(key string, data []byte) error {
	return fmt.Errorf("not implemented")
}
func (s *indexedDBStore) DeleteBlob(key string) error { return fmt.Errorf("not implemented") }
func (s *indexedDBStore) FreeSpace() int64            { return 0 }
func (s *indexedDBStore) MinReserve() int64           { return 50 * 1024 * 1024 } // 50MB
func (s *indexedDBStore) Close() error                { return nil }
