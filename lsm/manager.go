package lsm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/diskcache"
)

// Compile-time check.
var _ loophole.VolumeManager = (*Manager)(nil)

// volumeRef is the S3-persisted mapping from volume name to timeline ID.
type volumeRef struct {
	TimelineID string `json:"timeline_id"`
	Size       uint64 `json:"size,omitempty"`      // 0 = default
	ReadOnly   bool   `json:"read_only,omitempty"` // true for snapshots
	Type       string `json:"type,omitempty"`      // "ext4", "sqlite", or "" (unknown)
}

// Manager implements IVolumeManager using the LSM storage layer.
// Each volume is backed by a Timeline.
type Manager struct {
	store     loophole.ObjectStore // root of the bucket/prefix
	cacheDir  string
	config    Config
	diskCache *diskcache.DiskCache
	lease     *loophole.LeaseManager
	fs        LocalFS
	idGen     func() string

	timelines loophole.ObjectStore // store.At("timelines")
	volRefs   loophole.ObjectStore // store.At("volumes")

	mu         sync.Mutex
	volumes    map[string]*volume // open volumes by name
	openFlight singleflight.Group // deduplicates concurrent OpenVolume calls

	onRelease func(ctx context.Context, volumeName string) // called before storage-layer close
}

// NewVolumeManager creates and initializes an LSM Manager.
// fs is optional — nil uses OSLocalFS.
func NewVolumeManager(store loophole.ObjectStore, cacheDir string, config Config, fs LocalFS, diskCache *diskcache.DiskCache) *Manager {
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
		timelines: store.At("timelines"),
		volRefs:   store.At("volumes"),
		volumes:   make(map[string]*volume),
	}
	lease.Handle("release", m.handleRelease)
	return m
}

// SetOnRelease sets a callback invoked when a remote break-lease request
// is received, before the volume is closed at the storage layer. The
// callback should unmount/detach the volume via the backend.
func (m *Manager) SetOnRelease(fn func(ctx context.Context, volumeName string)) {
	m.onRelease = fn
}

func (m *Manager) NewVolume(ctx context.Context, name string, size uint64, volType string) (loophole.Volume, error) {
	slog.Info("lsm: NewVolume", "name", name, "size", size, "type", volType)
	if size == 0 {
		size = DefaultVolumeSize
	}

	timelineID := m.idGen()

	// Write timeline meta.json.
	meta := TimelineMeta{
		CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if _, err := m.timelines.At(timelineID).PutIfNotExists(ctx, "meta.json", metaData); err != nil {
		return nil, fmt.Errorf("create timeline meta: %w", err)
	}

	// Write volume ref.
	ref := volumeRef{TimelineID: timelineID, Size: size, Type: volType}
	refData, err := json.Marshal(ref)
	if err != nil {
		return nil, err
	}
	created, err := m.volRefs.PutIfNotExists(ctx, name, refData)
	if err != nil {
		return nil, fmt.Errorf("create volume ref: %w", err)
	}
	if !created {
		return nil, fmt.Errorf("volume %q already exists", name)
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

	// Deduplicate concurrent opens of the same volume.
	v, err, _ := m.openFlight.Do(name, func() (any, error) {
		// Double-check after winning the flight.
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
	return v.(*volume), nil
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

	ref, err := m.getVolumeRef(ctx, name)
	if err != nil {
		return fmt.Errorf("resolve volume %q: %w", name, err)
	}

	// Acquire lease on the timeline to ensure no other daemon is using it.
	if m.lease != nil {
		if err := m.lease.EnsureStarted(ctx); err != nil {
			return fmt.Errorf("start lease: %w", err)
		}
		tlStore := m.timelines.At(ref.TimelineID)
		err := loophole.ModifyJSON[TimelineMeta](ctx, tlStore, "meta.json", func(meta *TimelineMeta) error {
			if err := m.lease.CheckAvailable(ctx, meta.LeaseToken); err != nil {
				return fmt.Errorf("timeline %s: %w", ref.TimelineID, err)
			}
			meta.LeaseToken = m.lease.Token()
			return nil
		})
		if err != nil {
			return fmt.Errorf("acquire lease for %q: %w", name, err)
		}
		// Release the lease after deleting the ref.
		defer func() {
			_ = loophole.ModifyJSON[TimelineMeta](ctx, tlStore, "meta.json", func(meta *TimelineMeta) error {
				if meta.LeaseToken == m.lease.Token() {
					meta.LeaseToken = ""
				}
				return nil
			})
		}()
	}

	if err := m.volRefs.DeleteObject(ctx, name); err != nil {
		return fmt.Errorf("delete volume ref %q: %w", name, err)
	}
	// TODO: background GC of timeline data
	return nil
}

func (m *Manager) PageSize() int {
	return PageSize
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	vols := make([]*volume, 0, len(m.volumes))
	for _, v := range m.volumes {
		vols = append(vols, v)
	}
	m.volumes = make(map[string]*volume)
	m.mu.Unlock()

	for _, v := range vols {
		if m.lease != nil {
			_ = v.timeline.releaseLease(ctx, m.lease.Token())
		}
		v.timeline.close(ctx)
	}
	if m.lease != nil {
		if err := m.lease.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

// --- internal ---

func (m *Manager) getVolumeRef(ctx context.Context, name string) (volumeRef, error) {
	ref, _, err := loophole.ReadJSON[volumeRef](ctx, m.volRefs, name)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func (m *Manager) openVolume(ctx context.Context, name string, ref volumeRef) (*volume, error) {
	tl, err := m.openTimeline(ctx, m.timelines.At(ref.TimelineID), ref.TimelineID)
	if err != nil {
		return nil, fmt.Errorf("open timeline %q: %w", ref.TimelineID, err)
	}

	// Start periodic flush for this top-level timeline only (not ancestors).
	if m.config.FlushInterval > 0 {
		tl.startPeriodicFlush()
	}

	// Acquire write lease if lease manager is configured.
	if m.lease != nil {
		if err := m.lease.EnsureStarted(ctx); err != nil {
			tl.close(ctx)
			return nil, fmt.Errorf("start lease: %w", err)
		}
		if err := tl.acquireLease(ctx, m.lease); err != nil {
			tl.close(ctx)
			return nil, fmt.Errorf("acquire lease for %q: %w", name, err)
		}
	}

	v := newVolume(name, ref.Size, ref.Type, tl, m)
	if ref.ReadOnly {
		v.readOnly.Store(true)
	}

	m.mu.Lock()
	if existing, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		tl.close(ctx)
		return existing, nil
	}
	m.volumes[name] = v
	m.mu.Unlock()

	return v, nil
}

func (m *Manager) closeVolume(name string) {
	m.mu.Lock()
	delete(m.volumes, name)
	m.mu.Unlock()
}

// handleRelease is the lease RPC handler for "release" requests.
// It unmounts/detaches the volume via the onRelease callback (if set),
// then flushes and closes the volume at the storage layer.
func (m *Manager) handleRelease(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		Volume string `json:"volume"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("decode release params: %w", err)
	}

	slog.Info("release: releasing volume", "volume", req.Volume)

	// Let the backend unmount/detach first (kernel unmount, NBD disconnect, etc.).
	// This may close the volume via ReleaseRef, removing it from m.volumes.
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
		// Already closed by onRelease — that's fine.
		return map[string]string{"status": "ok"}, nil
	}

	if err := v.Flush(ctx); err != nil {
		slog.Warn("release: flush failed", "volume", req.Volume, "error", err)
	}
	_ = v.timeline.releaseLease(ctx, m.lease.Token())
	v.timeline.close(ctx)
	return map[string]string{"status": "ok"}, nil
}

// BreakLease requests the remote holder to release a volume. If the
// holder responds, the lease is cleanly handed over (graceful=true).
// If force is true and the holder doesn't respond, the lease is
// force-cleared. If force is false, an error is returned instead.
func (m *Manager) BreakLease(ctx context.Context, volumeName string, force bool) (bool, error) {
	ref, err := m.getVolumeRef(ctx, volumeName)
	if err != nil {
		return false, fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}

	tlStore := m.timelines.At(ref.TimelineID)
	meta, _, err := loophole.ReadJSON[TimelineMeta](ctx, tlStore, "meta.json")
	if err != nil {
		return false, fmt.Errorf("read timeline meta: %w", err)
	}

	if meta.LeaseToken == "" {
		return false, fmt.Errorf("volume %q has no active lease", volumeName)
	}

	oldToken := meta.LeaseToken

	if m.lease != nil && oldToken == m.lease.Token() {
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
	if err := loophole.ModifyJSON[TimelineMeta](ctx, tlStore, "meta.json", func(m *TimelineMeta) error {
		if m.LeaseToken == "" {
			return nil // already cleared
		}
		if m.LeaseToken != oldToken {
			return fmt.Errorf("lease token changed unexpectedly (expected %q, got %q)", oldToken, m.LeaseToken)
		}
		m.LeaseToken = ""
		return nil
	}); err != nil {
		return false, fmt.Errorf("clear lease token: %w", err)
	}

	// Delete the lease file (best-effort — it may already be gone).
	_ = m.store.At("leases").DeleteObject(ctx, oldToken+".json")

	return graceful, nil
}
