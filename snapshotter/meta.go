// Package snapshotter implements a containerd snapshotter proxy plugin
// backed by loophole volumes.
package snapshotter

import (
	"fmt"
	"sync"
	"time"
)

// SnapshotKind mirrors containerd's snapshot kind enum.
type SnapshotKind int

const (
	KindView      SnapshotKind = 1
	KindActive    SnapshotKind = 2
	KindCommitted SnapshotKind = 3
)

// SnapshotMeta is the per-snapshot metadata.
type SnapshotMeta struct {
	VolumeName string
	Parent     string
	Kind       SnapshotKind
	Labels     map[string]string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Mountpoint string
}

// snapshotStore is an in-memory metadata store for snapshot state.
type snapshotStore struct {
	mu    sync.Mutex
	snaps map[string]SnapshotMeta
}

func newSnapshotStore() *snapshotStore {
	return &snapshotStore{snaps: make(map[string]SnapshotMeta)}
}

func (s *snapshotStore) get(key string) (SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.snaps[key]
	if !ok {
		return SnapshotMeta{}, fmt.Errorf("snapshot %q not found", key)
	}
	return meta, nil
}

func (s *snapshotStore) put(key string, meta SnapshotMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps[key] = meta
}

func (s *snapshotStore) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snaps, key)
}

func (s *snapshotStore) list() map[string]SnapshotMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]SnapshotMeta, len(s.snaps))
	for k, v := range s.snaps {
		cp[k] = v
	}
	return cp
}
