//go:build js

package storage2

import "fmt"

// cacheStore is duplicated here for the js build tag.
// The canonical definition is in pagecache.go.

// indexedDBStore is a stub for WASM builds.
type indexedDBStore struct{}

func newIndexedDBStore() (*indexedDBStore, error) {
	return &indexedDBStore{}, nil
}

func newDefaultStore(_ string) (cacheStore, error) {
	return newIndexedDBStore()
}

func (s *indexedDBStore) AllocArena(maxSlots int) error { return nil }
func (s *indexedDBStore) ArenaSlots() int               { return 0 }
func (s *indexedDBStore) ReadSlot(slot int) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *indexedDBStore) WriteSlot(slot int, data []byte) error      { return fmt.Errorf("not implemented") }
func (s *indexedDBStore) LookupPage(key cacheKey) (int, bool, error) { return 0, false, nil }
func (s *indexedDBStore) InsertPage(key cacheKey, slot int) error {
	return fmt.Errorf("not implemented")
}
func (s *indexedDBStore) DeletePage(key cacheKey) (int, error) {
	return -1, fmt.Errorf("not implemented")
}
func (s *indexedDBStore) BumpCredits(keys []cacheKey) error { return nil }
func (s *indexedDBStore) AgeCredits() error                 { return nil }
func (s *indexedDBStore) EvictLow(count int) ([]int, error) { return nil, nil }
func (s *indexedDBStore) CountPages() (int, error)          { return 0, nil }
func (s *indexedDBStore) UsedSlots() ([]int, error)         { return nil, nil }
func (s *indexedDBStore) FreeSpace() int64                  { return 0 }
func (s *indexedDBStore) MinReserve() int64                 { return 50 * 1024 * 1024 }
func (s *indexedDBStore) Close() error                      { return nil }
