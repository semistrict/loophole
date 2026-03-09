//go:build !js

package storage2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/semistrict/loophole"
)

// Compile-time check.
var _ loophole.VolumeManager = (*Manager)(nil)

// volumeRef is the S3-persisted mapping from volume name to layer ID.
type volumeRef struct {
	LayerID       string `json:"layer_id"`
	Size          uint64 `json:"size,omitempty"`
	ReadOnly      bool   `json:"read_only,omitempty"`
	Type          string `json:"type,omitempty"`
	LeaseToken    string `json:"lease_token,omitempty"`
	WriteLeaseSeq uint64 `json:"write_lease_seq,omitempty"`
}

// managedVolume is the internal interface for volumes tracked by the Manager.
type managedVolume interface {
	loophole.Volume
	isReadOnly() bool
	flush() error
	close()
}

// Manager manages volumes backed by storage2 layers.
type Manager struct {
	store     loophole.ObjectStore
	cacheDir  string
	config    Config
	diskCache *PageCache
	lease     *loophole.LeaseManager
	fs        LocalFS
	idGen     func() string

	volRefs loophole.ObjectStore // store.At("volumes")

	mu         sync.Mutex
	volumes    map[string]managedVolume
	openFlight singleflight[managedVolume]
	onRelease  func(ctx context.Context, volumeName string)
}

// LocalFS abstracts local filesystem operations for memtable backing files.
type LocalFS interface {
	MkdirAll(path string, perm uint32) error
}

// OSLocalFS is the default LocalFS using the OS filesystem.
type OSLocalFS struct{}

func (OSLocalFS) MkdirAll(path string, perm uint32) error {
	return ensureMemDir(path)
}

// NewVolumeManager creates a Manager.
func NewVolumeManager(store loophole.ObjectStore, cacheDir string, config Config, fs LocalFS, diskCache *PageCache) *Manager {
	store = loophole.NewRetryStore(store)
	config.setDefaults()
	if fs == nil {
		fs = OSLocalFS{}
	}
	lease := loophole.NewLeaseManager(store.At("leases"))
	m := &Manager{
		store:     store,
		cacheDir:  cacheDir,
		config:    config,
		diskCache: diskCache,
		lease:     lease,
		fs:        fs,
		idGen:     uuid.NewString,
		volRefs:   store.At("volumes"),
		volumes:   make(map[string]managedVolume),
	}
	lease.Handle("release", m.handleRelease)
	return m
}

// SetOnRelease sets a callback invoked when a remote break-lease request
// is received, before the volume is closed at the storage layer.
func (m *Manager) SetOnRelease(fn func(ctx context.Context, volumeName string)) {
	m.onRelease = fn
}

func (m *Manager) NewVolume(ctx context.Context, name string, size uint64, volType string) (loophole.Volume, error) {
	slog.Info("storage2: NewVolume", "name", name, "size", size, "type", volType)
	if size == 0 {
		size = DefaultVolumeSize
	}

	layerID := m.idGen()

	// Write initial index.json with created_at in object metadata.
	idx := layerIndex{NextSeq: 1}
	idxData, err := json.Marshal(idx)
	if err != nil {
		return nil, err
	}
	layerStore := m.store.At("layers/" + layerID)
	if err := layerStore.PutIfNotExists(ctx, "index.json", idxData, map[string]string{
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return nil, fmt.Errorf("create layer index: %w", err)
	}

	// Write volume ref (with lease token).
	ref := volumeRef{LayerID: layerID, Size: size, Type: volType}
	if err := m.putVolumeRefNew(ctx, name, ref); err != nil {
		return nil, err
	}

	return m.openVolume(ctx, name, ref)
}

func (m *Manager) OpenVolume(ctx context.Context, name string) (loophole.Volume, error) {
	m.mu.Lock()
	if v, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		return v, nil
	}
	m.mu.Unlock()

	v, err := m.openFlight.do(name, func() (managedVolume, error) {
		// Re-check under singleflight in case another caller just opened it.
		m.mu.Lock()
		if v, ok := m.volumes[name]; ok {
			m.mu.Unlock()
			return v, nil
		}
		m.mu.Unlock()

		ref, err := m.getVolumeRef(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("resolve volume %q: %w", name, err)
		}
		return m.openVolume(ctx, name, ref)
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (m *Manager) GetVolume(name string) loophole.Volume {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.volumes[name]
	if !ok {
		return nil
	}
	return v
}

func (m *Manager) Volumes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.volumes))
	for name := range m.volumes {
		names = append(names, name)
	}
	return names
}

func (m *Manager) ListAllVolumes(ctx context.Context) ([]string, error) {
	objects, err := m.volRefs.ListKeys(ctx, "")
	if err != nil {
		return nil, err
	}
	names := make([]string, len(objects))
	for i, obj := range objects {
		names[i] = obj.Key
	}
	return names, nil
}

func (m *Manager) ListVolumesByType(ctx context.Context, volType string) ([]string, error) {
	objects, err := m.volRefs.ListKeys(ctx, "")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, obj := range objects {
		ref, _, err := loophole.ReadJSON[volumeRef](ctx, m.volRefs, obj.Key)
		if err != nil {
			continue
		}
		if ref.Type == volType {
			names = append(names, obj.Key)
		}
	}
	return names, nil
}

func (m *Manager) DeleteVolume(ctx context.Context, name string) error {
	m.mu.Lock()
	if _, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		return fmt.Errorf("volume %q is open; close it first", name)
	}
	m.mu.Unlock()

	// Acquire lease to ensure no other daemon is using this volume.
	if _, err := m.acquireVolumeLease(ctx, name); err != nil {
		return err
	}
	defer m.releaseVolumeLease(ctx, name)

	if err := m.volRefs.DeleteObject(ctx, name); err != nil {
		return fmt.Errorf("delete volume ref %q: %w", name, err)
	}
	return nil
}

func (m *Manager) PageSize() int {
	return PageSize
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	vols := make([]managedVolume, 0, len(m.volumes))
	for _, v := range m.volumes {
		vols = append(vols, v)
	}
	m.volumes = make(map[string]managedVolume)
	m.mu.Unlock()

	for _, v := range vols {
		if !v.isReadOnly() {
			if err := v.flush(); err != nil {
				slog.Warn("flush on close failed", "volume", v.Name(), "error", err)
			}
			m.releaseVolumeLease(ctx, v.Name())
		}
		v.close()
	}
	if err := m.lease.Close(ctx); err != nil {
		return err
	}
	return nil
}

// BreakLease requests the remote holder to release a volume.
func (m *Manager) BreakLease(ctx context.Context, volumeName string, force bool) (bool, error) {
	ref, etag, err := loophole.ReadJSON[volumeRef](ctx, m.volRefs, volumeName)
	if err != nil {
		return false, fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}
	if ref.LeaseToken == "" {
		return false, fmt.Errorf("volume %q has no active lease", volumeName)
	}

	oldToken := ref.LeaseToken

	if oldToken == m.lease.Token() {
		return false, fmt.Errorf("volume %q is leased by this daemon; use unmount or stop instead", volumeName)
	}

	// Try the polite RPC first.
	slog.Info("break-lease: requesting release", "volume", volumeName, "token", oldToken)
	_, rpcErr := m.lease.Call(ctx, oldToken, "release", map[string]string{"volume": volumeName})
	graceful := rpcErr == nil
	if rpcErr != nil {
		if !force {
			return false, fmt.Errorf("holder did not respond for volume %q (use -f to force): %w", volumeName, rpcErr)
		}
		slog.Warn("break-lease: holder did not respond, force-clearing", "volume", volumeName, "err", rpcErr)
	}

	// Clear the token if the holder didn't do it.
	// Use the etag from the initial read for CAS.
	ref.LeaseToken = ""
	data, err := json.Marshal(ref)
	if err != nil {
		return graceful, err
	}
	if _, err := m.volRefs.PutBytesCAS(ctx, volumeName, data, etag); err != nil {
		return graceful, fmt.Errorf("clear lease token: %w", err)
	}

	// Delete the lease file (best-effort).
	if err := m.store.At("leases").DeleteObject(ctx, oldToken+".json"); err != nil {
		slog.Warn("delete stale lease file", "token", oldToken, "error", err)
	}

	return graceful, nil
}

// --- internal ---

func (m *Manager) getVolumeRef(ctx context.Context, name string) (volumeRef, error) {
	ref, _, err := loophole.ReadJSON[volumeRef](ctx, m.volRefs, name)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func (m *Manager) openVolume(ctx context.Context, name string, ref volumeRef) (managedVolume, error) {
	// Check object metadata on index.json (HEAD, no body) to see if frozen.
	layerStore := m.store.At("layers/" + ref.LayerID)
	meta, _ := layerStore.HeadMeta(ctx, "index.json")
	if meta["frozen_at"] != "" {
		return m.openFrozenVolume(ctx, name, ref)
	}

	// Acquire write lease before opening the layer so we have the
	// writeLeaseSeq for file naming.
	var writeLeaseSeq uint64
	if !ref.ReadOnly && ref.LeaseToken != m.lease.Token() {
		seq, err := m.acquireVolumeLease(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("acquire lease for %q: %w", name, err)
		}
		writeLeaseSeq = seq
	}

	cacheDir := m.cacheDir + "/layers/" + ref.LayerID
	ly, err := openLayer(ctx, m.store, ref.LayerID, m.config, m.diskCache, cacheDir)
	if err != nil {
		if writeLeaseSeq > 0 {
			m.releaseVolumeLease(ctx, name)
		}
		return nil, fmt.Errorf("open layer %q: %w", ref.LayerID, err)
	}
	ly.writeLeaseSeq = writeLeaseSeq

	if m.config.FlushInterval > 0 {
		ly.startPeriodicFlush(ctx)
	}

	v := newVolume(name, ref.Size, ref.Type, ly, m)
	if ref.ReadOnly {
		v.readOnly.Store(true)
	}

	m.mu.Lock()
	if existing, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		ly.Close()
		if !ref.ReadOnly {
			m.releaseVolumeLease(ctx, name)
		}
		return existing, nil
	}
	m.volumes[name] = v
	m.mu.Unlock()

	return v, nil
}

func (m *Manager) openFrozenVolume(ctx context.Context, name string, ref volumeRef) (managedVolume, error) {
	ly, err := openFrozenLayer(ctx, m.store, ref.LayerID, m.config, m.diskCache)
	if err != nil {
		return nil, fmt.Errorf("open frozen layer %q: %w", ref.LayerID, err)
	}

	v := newFrozenVolume(name, ref.Size, ref.Type, ly, m)

	m.mu.Lock()
	if existing, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		ly.Close()
		return existing, nil
	}
	m.volumes[name] = v
	m.mu.Unlock()

	return v, nil
}

func (m *Manager) putVolumeRefNew(ctx context.Context, name string, ref volumeRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := m.volRefs.PutIfNotExists(ctx, name, data); err != nil {
		if errors.Is(err, loophole.ErrExists) {
			return fmt.Errorf("volume %q already exists", name)
		}
		return fmt.Errorf("create volume ref: %w", err)
	}
	return nil
}

func (m *Manager) putVolumeRef(ctx context.Context, name string, ref volumeRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := m.volRefs.PutIfNotExists(ctx, name, data); err != nil {
		if errors.Is(err, loophole.ErrExists) {
			return fmt.Errorf("volume %q already exists", name)
		}
		return err
	}
	return nil
}

func (m *Manager) closeVolume(name string) {
	m.mu.Lock()
	delete(m.volumes, name)
	m.mu.Unlock()
}

// acquireVolumeLease checks the volume ref's lease token and writes ours.
// Atomically increments WriteLeaseSeq in the same CAS and returns the new value.
func (m *Manager) acquireVolumeLease(ctx context.Context, name string) (uint64, error) {
	if err := m.lease.EnsureStarted(ctx); err != nil {
		return 0, fmt.Errorf("start lease: %w", err)
	}
	var seq uint64
	err := loophole.ModifyJSON[volumeRef](ctx, m.volRefs, name, func(ref *volumeRef) error {
		if err := m.lease.CheckAvailable(ctx, ref.LeaseToken); err != nil {
			return fmt.Errorf("volume %s: %w", name, err)
		}
		ref.LeaseToken = m.lease.Token()
		ref.WriteLeaseSeq++
		seq = ref.WriteLeaseSeq
		return nil
	})
	return seq, err
}

// releaseVolumeLease clears the lease token from the volume ref if it matches ours.
func (m *Manager) releaseVolumeLease(ctx context.Context, name string) {
	if err := loophole.ModifyJSON[volumeRef](ctx, m.volRefs, name, func(ref *volumeRef) error {
		if ref.LeaseToken == m.lease.Token() {
			ref.LeaseToken = ""
		}
		return nil
	}); err != nil {
		slog.Warn("release volume lease", "volume", name, "error", err)
	}
}

// handleRelease is the lease RPC handler for "release" requests.
func (m *Manager) handleRelease(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("decode release params: %w", err)
	}

	slog.Info("release: releasing volume", "volume", req.Volume)

	// Let the backend unmount/detach first.
	if m.onRelease != nil {
		m.onRelease(ctx, req.Volume)
	}

	m.mu.Lock()
	v, ok := m.volumes[req.Volume]
	if ok {
		delete(m.volumes, req.Volume)
	}
	m.mu.Unlock()

	if !ok {
		return map[string]string{"status": "ok"}, nil
	}

	if err := v.flush(); err != nil {
		slog.Warn("release: flush failed", "volume", req.Volume, "error", err)
	}
	if !v.isReadOnly() {
		m.releaseVolumeLease(ctx, req.Volume)
	}
	v.close()
	return map[string]string{"status": "ok"}, nil
}
