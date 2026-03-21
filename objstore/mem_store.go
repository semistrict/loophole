package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OpType identifies a MemStore operation for fault injection and counting.
type OpType int

const (
	OpGet OpType = iota
	OpPutBytes
	OpPutBytesCAS
	OpPutReader
	OpPutIfNotExists
	OpDeleteObject
	OpListKeys
)

// Fault describes an injected fault for a MemStore operation.
// Hook, Delay, and Err are applied in order: Hook runs first,
// then Delay sleeps, then Err is returned (if non-nil).
// PostErr is different: the operation executes normally, then PostErr
// is returned instead of nil. This simulates "phantom" operations where
// the data lands in the store but the caller sees an error.
type Fault struct {
	Err         error                 // return this error (operation does NOT execute)
	PostErr     error                 // return this error AFTER the operation succeeds
	Delay       time.Duration         // sleep before executing (works with synctest)
	Hook        func()                // called before delay/error; use to synchronize with test goroutines
	ShouldFault func(key string) bool // when set, fault only applies if this returns true for the key
}

// faultKey identifies a fault rule: an operation type + optional key pattern.
type faultKey struct {
	op  OpType
	key string // full key pattern, or "" for all keys
}

// memStoreShared holds state shared across all sub-scoped MemStores.
type memStoreShared struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	metadata map[string]map[string]string // key → user metadata

	faultMu sync.RWMutex
	faults  map[faultKey]Fault

	counts  [7]atomic.Int64 // indexed by OpType
	bytesRx atomic.Int64    // bytes returned by Get/GetRange
	bytesTx atomic.Int64    // bytes written by Put*/PutReader
}

// MemStore is an in-memory ObjectStore, useful for tests and embedded use.
// Supports fault injection (errors and delays) and operation counting.
type MemStore struct {
	shared *memStoreShared
	prefix string
}

func NewMemStore() *MemStore {
	return &MemStore{
		shared: &memStoreShared{
			objects:  make(map[string][]byte),
			metadata: make(map[string]map[string]string),
			faults:   make(map[faultKey]Fault),
		},
	}
}

// SetFault programs a fault for the given operation type.
// If key is "", the fault applies to all keys for that operation.
// If key is non-empty, it matches against the full (prefix-qualified) key.
func (m *MemStore) SetFault(op OpType, key string, f Fault) {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	m.shared.faults[faultKey{op: op, key: key}] = f
}

// ClearFault removes a previously set fault.
func (m *MemStore) ClearFault(op OpType, key string) {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	delete(m.shared.faults, faultKey{op: op, key: key})
}

// ClearAllFaults removes all fault rules.
func (m *MemStore) ClearAllFaults() {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	clear(m.shared.faults)
}

// Count returns the number of times the given operation has been called.
func (m *MemStore) Count(op OpType) int64 {
	return m.shared.counts[op].Load()
}

// BytesRx returns total bytes returned by Get/GetRange.
func (m *MemStore) BytesRx() int64 { return m.shared.bytesRx.Load() }

// BytesTx returns total bytes written by Put operations.
func (m *MemStore) BytesTx() int64 { return m.shared.bytesTx.Load() }

// ResetCounts zeros all operation and byte counters.
func (m *MemStore) ResetCounts() {
	for i := range m.shared.counts {
		m.shared.counts[i].Store(0)
	}
	m.shared.bytesRx.Store(0)
	m.shared.bytesTx.Store(0)
}

// checkFault looks up and applies any matching fault. It checks for a
// key-specific fault first, then falls back to a wildcard fault.
// Returns a non-nil error if the operation should fail.
func (m *MemStore) checkFault(op OpType, fullKey string) error {
	m.shared.faultMu.RLock()
	// Key-specific fault takes priority.
	f, ok := m.shared.faults[faultKey{op: op, key: fullKey}]
	if !ok {
		// Wildcard fault for this op type.
		f, ok = m.shared.faults[faultKey{op: op}]
	}
	m.shared.faultMu.RUnlock()

	if !ok {
		return nil
	}
	if f.ShouldFault != nil && !f.ShouldFault(fullKey) {
		return nil
	}
	if f.Hook != nil {
		f.Hook()
	}
	if f.Delay > 0 {
		time.Sleep(f.Delay)
	}
	return f.Err
}

// checkPostFault returns a PostErr if one is set for this operation (error
// returned AFTER the operation succeeds — "phantom" fault).
func (m *MemStore) checkPostFault(op OpType, fullKey string) error {
	m.shared.faultMu.RLock()
	f, ok := m.shared.faults[faultKey{op: op, key: fullKey}]
	if !ok {
		f, ok = m.shared.faults[faultKey{op: op}]
	}
	m.shared.faultMu.RUnlock()
	if ok && f.PostErr != nil {
		if f.ShouldFault != nil && !f.ShouldFault(fullKey) {
			return nil
		}
		return f.PostErr
	}
	return nil
}

func (m *MemStore) count(op OpType) {
	m.shared.counts[op].Add(1)
}

func (m *MemStore) fullKey(key string) string {
	if m.prefix == "" {
		return key
	}
	return m.prefix + "/" + key
}

func (m *MemStore) At(path string) ObjectStore {
	p := path
	if m.prefix != "" {
		p = m.prefix + "/" + path
	}
	return &MemStore{
		shared: m.shared,
		prefix: p,
	}
}

func (m *MemStore) Get(_ context.Context, key string) (io.ReadCloser, string, error) {
	fk := m.fullKey(key)
	m.count(OpGet)
	if err := m.checkFault(OpGet, fk); err != nil {
		return nil, "", err
	}

	m.shared.mu.RLock()
	data, ok := m.shared.objects[fk]
	m.shared.mu.RUnlock()
	if !ok {
		return nil, "", fmt.Errorf("%s: %w", fk, ErrNotFound)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.bytesRx.Add(int64(len(cp)))
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	return io.NopCloser(bytes.NewReader(cp)), etag, nil
}

func (m *MemStore) PutBytes(_ context.Context, key string, data []byte) error {
	fk := m.fullKey(key)
	m.count(OpPutBytes)
	if err := m.checkFault(OpPutBytes, fk); err != nil {
		return err
	}

	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.objects[fk] = cp
	return nil
}

func (m *MemStore) PutBytesCAS(_ context.Context, key string, data []byte, etag string) (string, error) {
	fk := m.fullKey(key)
	m.count(OpPutBytesCAS)
	if err := m.checkFault(OpPutBytesCAS, fk); err != nil {
		return "", err
	}

	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	existing, ok := m.shared.objects[fk]
	existingEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(existing))
	if !ok {
		existingEtag = ""
	}
	if existingEtag != etag {
		return "", fmt.Errorf("CAS conflict: expected etag %s, got %s", etag, existingEtag)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.objects[fk] = cp
	newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(cp))
	return newEtag, nil
}

func (m *MemStore) PutReader(_ context.Context, key string, r io.Reader) error {
	fk := m.fullKey(key)
	m.count(OpPutReader)
	if err := m.checkFault(OpPutReader, fk); err != nil {
		return err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.shared.mu.Lock()
	m.shared.objects[fk] = data
	m.shared.mu.Unlock()
	if err := m.checkPostFault(OpPutReader, fk); err != nil {
		return err
	}
	return nil
}

func (m *MemStore) PutIfNotExists(_ context.Context, key string, data []byte, meta ...map[string]string) error {
	fk := m.fullKey(key)
	m.count(OpPutIfNotExists)
	if err := m.checkFault(OpPutIfNotExists, fk); err != nil {
		return err
	}

	m.shared.mu.Lock()
	if _, ok := m.shared.objects[fk]; ok {
		m.shared.mu.Unlock()
		return ErrExists
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.objects[fk] = cp
	if len(meta) > 0 && meta[0] != nil {
		mc := make(map[string]string, len(meta[0]))
		for k, v := range meta[0] {
			mc[k] = v
		}
		m.shared.metadata[fk] = mc
	}
	m.shared.mu.Unlock()
	if err := m.checkPostFault(OpPutIfNotExists, fk); err != nil {
		return err
	}
	return nil
}

func (m *MemStore) DeleteObjects(_ context.Context, keys []string) error {
	m.count(OpDeleteObject)
	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	for _, key := range keys {
		fk := m.fullKey(key)
		if err := m.checkFault(OpDeleteObject, fk); err != nil {
			return err
		}
		delete(m.shared.objects, fk)
	}
	return nil
}

func (m *MemStore) ListKeys(_ context.Context, prefix string) ([]ObjectInfo, error) {
	m.count(OpListKeys)
	if err := m.checkFault(OpListKeys, m.fullKey(prefix)); err != nil {
		return nil, err
	}

	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	fullPrefix := m.fullKey(prefix)
	var result []ObjectInfo
	for k, v := range m.shared.objects {
		if !strings.HasPrefix(k, fullPrefix) {
			continue
		}
		rel := k
		if m.prefix != "" {
			rel = strings.TrimPrefix(k, m.prefix+"/")
		}
		if prefix != "" {
			rel = strings.TrimPrefix(rel, prefix)
		}
		result = append(result, ObjectInfo{Key: rel, Size: int64(len(v))})
	}
	return result, nil
}

func (m *MemStore) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	fk := m.fullKey(key)
	m.count(OpGet)
	if err := m.checkFault(OpGet, fk); err != nil {
		return nil, "", err
	}

	m.shared.mu.RLock()
	data, ok := m.shared.objects[fk]
	m.shared.mu.RUnlock()
	if !ok {
		return nil, "", fmt.Errorf("%s: %w", fk, ErrNotFound)
	}
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	if int(offset) >= len(data) {
		return io.NopCloser(bytes.NewReader(nil)), etag, nil
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	cp := make([]byte, end-offset)
	copy(cp, data[offset:end])
	m.shared.bytesRx.Add(int64(len(cp)))
	return io.NopCloser(bytes.NewReader(cp)), etag, nil
}

func (m *MemStore) HeadMeta(_ context.Context, key string) (map[string]string, error) {
	fk := m.fullKey(key)
	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	if _, ok := m.shared.objects[fk]; !ok {
		return nil, fmt.Errorf("%s: %w", fk, ErrNotFound)
	}
	meta := m.shared.metadata[fk]
	if meta == nil {
		return map[string]string{}, nil
	}
	cp := make(map[string]string, len(meta))
	for k, v := range meta {
		cp[k] = v
	}
	return cp, nil
}

func (m *MemStore) SetMeta(_ context.Context, key string, meta map[string]string) error {
	fk := m.fullKey(key)
	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	if _, ok := m.shared.objects[fk]; !ok {
		return fmt.Errorf("%s: %w", fk, ErrNotFound)
	}
	cp := make(map[string]string, len(meta))
	for k, v := range meta {
		cp[k] = v
	}
	m.shared.metadata[fk] = cp
	return nil
}

// Keys returns all keys under the given full-key prefix. For test assertions.
func (m *MemStore) Keys(prefix string) []string {
	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	var keys []string
	for k := range m.shared.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

// GetObject returns raw bytes for a full key (not scoped). For test assertions.
func (m *MemStore) GetObject(fullKey string) ([]byte, bool) {
	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	d, ok := m.shared.objects[fullKey]
	return d, ok
}
