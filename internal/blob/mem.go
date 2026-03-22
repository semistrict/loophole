package blob

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

// OpType identifies a MemDriver operation for fault injection and counting.
type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpDelete
	OpList
	OpHead
	OpSetMeta
	opCount // sentinel for array sizing
)

// Fault describes an injected fault for a MemDriver operation.
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

// memDriverShared holds state shared across MemDriver instances (unused
// since MemDriver has no sub-scoping, but kept for interface consistency).
type memDriverShared struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	metadata map[string]map[string]string // key → user metadata

	faultMu sync.RWMutex
	faults  map[faultKey]Fault

	counts  [opCount]atomic.Int64
	bytesRx atomic.Int64 // bytes returned by Get
	bytesTx atomic.Int64 // bytes written by Put
}

// MemDriver is an in-memory Driver, useful for tests and embedded use.
// Supports fault injection (errors and delays) and operation counting.
type MemDriver struct {
	shared *memDriverShared
}

func NewMemDriver() *MemDriver {
	return &MemDriver{
		shared: &memDriverShared{
			objects:  make(map[string][]byte),
			metadata: make(map[string]map[string]string),
			faults:   make(map[faultKey]Fault),
		},
	}
}

// NewMemStore creates a MemDriver wrapped in a Store. Returns both so
// tests can use the driver for fault injection and the store for operations.
func NewMemStore() (*MemDriver, *Store) {
	d := NewMemDriver()
	return d, New(d)
}

// SetFault programs a fault for the given operation type.
// If key is "", the fault applies to all keys for that operation.
func (m *MemDriver) SetFault(op OpType, key string, f Fault) {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	m.shared.faults[faultKey{op: op, key: key}] = f
}

// ClearFault removes a previously set fault.
func (m *MemDriver) ClearFault(op OpType, key string) {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	delete(m.shared.faults, faultKey{op: op, key: key})
}

// ClearAllFaults removes all fault rules.
func (m *MemDriver) ClearAllFaults() {
	m.shared.faultMu.Lock()
	defer m.shared.faultMu.Unlock()
	clear(m.shared.faults)
}

// Count returns the number of times the given operation has been called.
func (m *MemDriver) Count(op OpType) int64 {
	return m.shared.counts[op].Load()
}

// BytesRx returns total bytes returned by Get.
func (m *MemDriver) BytesRx() int64 { return m.shared.bytesRx.Load() }

// BytesTx returns total bytes written by Put.
func (m *MemDriver) BytesTx() int64 { return m.shared.bytesTx.Load() }

// ResetCounts zeros all operation and byte counters.
func (m *MemDriver) ResetCounts() {
	for i := range m.shared.counts {
		m.shared.counts[i].Store(0)
	}
	m.shared.bytesRx.Store(0)
	m.shared.bytesTx.Store(0)
}

func (m *MemDriver) checkFault(op OpType, key string) error {
	m.shared.faultMu.RLock()
	f, ok := m.shared.faults[faultKey{op: op, key: key}]
	if !ok {
		f, ok = m.shared.faults[faultKey{op: op}]
	}
	m.shared.faultMu.RUnlock()

	if !ok {
		return nil
	}
	if f.ShouldFault != nil && !f.ShouldFault(key) {
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

func (m *MemDriver) checkPostFault(op OpType, key string) error {
	m.shared.faultMu.RLock()
	f, ok := m.shared.faults[faultKey{op: op, key: key}]
	if !ok {
		f, ok = m.shared.faults[faultKey{op: op}]
	}
	m.shared.faultMu.RUnlock()
	if ok && f.PostErr != nil {
		if f.ShouldFault != nil && !f.ShouldFault(key) {
			return nil
		}
		return f.PostErr
	}
	return nil
}

func (m *MemDriver) count(op OpType) {
	m.shared.counts[op].Add(1)
}

func (m *MemDriver) Get(_ context.Context, key string, opts GetOpts) (io.ReadCloser, int64, string, error) {
	m.count(OpGet)
	if err := m.checkFault(OpGet, key); err != nil {
		return nil, 0, "", err
	}

	m.shared.mu.RLock()
	data, ok := m.shared.objects[key]
	m.shared.mu.RUnlock()
	if !ok {
		return nil, 0, "", fmt.Errorf("%s: %w", key, ErrNotFound)
	}

	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	if opts.Length > 0 {
		if int(opts.Offset) >= len(data) {
			return io.NopCloser(bytes.NewReader(nil)), 0, etag, nil
		}
		end := opts.Offset + opts.Length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		cp := make([]byte, end-opts.Offset)
		copy(cp, data[opts.Offset:end])
		m.shared.bytesRx.Add(int64(len(cp)))
		return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), etag, nil
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.bytesRx.Add(int64(len(cp)))
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), etag, nil
}

func (m *MemDriver) Put(_ context.Context, key string, body io.Reader, opts PutOpts) (string, error) {
	m.count(OpPut)
	if err := m.checkFault(OpPut, key); err != nil {
		return "", err
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}

	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()

	if opts.IfMatch != "" {
		existing, ok := m.shared.objects[key]
		existingEtag := ""
		if ok {
			existingEtag = fmt.Sprintf(`"%x"`, sha256.Sum256(existing))
		}
		if existingEtag != opts.IfMatch {
			return "", fmt.Errorf("CAS conflict: expected etag %s, got %s", opts.IfMatch, existingEtag)
		}
	}

	if opts.IfNotExists {
		if _, ok := m.shared.objects[key]; ok {
			return "", ErrExists
		}
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	m.shared.objects[key] = cp
	m.shared.bytesTx.Add(int64(len(data)))

	if opts.Metadata != nil {
		mc := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			mc[k] = v
		}
		m.shared.metadata[key] = mc
	}

	newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(cp))

	if err := m.checkPostFault(OpPut, key); err != nil {
		return "", err
	}
	return newEtag, nil
}

func (m *MemDriver) Delete(_ context.Context, keys []string) error {
	m.count(OpDelete)
	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	for _, key := range keys {
		if err := m.checkFault(OpDelete, key); err != nil {
			return err
		}
		delete(m.shared.objects, key)
	}
	return nil
}

func (m *MemDriver) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	m.count(OpList)
	if err := m.checkFault(OpList, prefix); err != nil {
		return nil, err
	}

	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	var result []ObjectInfo
	for k, v := range m.shared.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		result = append(result, ObjectInfo{Key: k, Size: int64(len(v))})
	}
	return result, nil
}

func (m *MemDriver) Head(_ context.Context, key string) (map[string]string, error) {
	m.count(OpHead)
	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	if _, ok := m.shared.objects[key]; !ok {
		return nil, fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	meta := m.shared.metadata[key]
	if meta == nil {
		return map[string]string{}, nil
	}
	cp := make(map[string]string, len(meta))
	for k, v := range meta {
		cp[k] = v
	}
	return cp, nil
}

func (m *MemDriver) SetMeta(_ context.Context, key string, meta map[string]string) error {
	m.count(OpSetMeta)
	m.shared.mu.Lock()
	defer m.shared.mu.Unlock()
	if _, ok := m.shared.objects[key]; !ok {
		return fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	cp := make(map[string]string, len(meta))
	for k, v := range meta {
		cp[k] = v
	}
	m.shared.metadata[key] = cp
	return nil
}

// Keys returns all keys under the given prefix. For test assertions.
func (m *MemDriver) Keys(prefix string) []string {
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

// GetObject returns raw bytes for a key. For test assertions.
func (m *MemDriver) GetObject(key string) ([]byte, bool) {
	m.shared.mu.RLock()
	defer m.shared.mu.RUnlock()
	d, ok := m.shared.objects[key]
	return d, ok
}
