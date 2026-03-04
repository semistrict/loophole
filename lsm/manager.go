package lsm

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/semistrict/loophole"
)

// Compile-time check.
var _ loophole.VolumeManager = (*Manager)(nil)

// volumeRef is the S3-persisted mapping from volume name to timeline ID.
type volumeRef struct {
	TimelineID string `json:"timeline_id"`
	Size       uint64 `json:"size,omitempty"`      // 0 = default
	ReadOnly   bool   `json:"read_only,omitempty"` // true for snapshots
}

// Manager implements IVolumeManager using the LSM storage layer.
// Each volume is backed by a Timeline.
type Manager struct {
	store     loophole.ObjectStore // root of the bucket/prefix
	cacheDir  string
	config    Config
	pageCache *PageCache
	lease     *loophole.LeaseManager // nil disables lease enforcement
	fs        LocalFS
	clock     Clock

	timelines loophole.ObjectStore // store.At("timelines")
	volRefs   loophole.ObjectStore // store.At("volumes")

	mu         sync.Mutex
	volumes    map[string]*volume // open volumes by name
	openFlight singleflight.Group // deduplicates concurrent OpenVolume calls
}

// NewManager creates and initializes an LSM Manager.
// lease, fs, and clock are optional — nil disables lease enforcement / uses
// production defaults (OSLocalFS, RealClock).
func NewManager(store loophole.ObjectStore, cacheDir string, config Config, lease *loophole.LeaseManager, fs LocalFS, clock Clock) *Manager {
	config.setDefaults()
	if fs == nil {
		fs = OSLocalFS{}
	}
	if clock == nil {
		clock = RealClock{}
	}
	pc, err := NewPageCache(filepath.Join(cacheDir, "pages.cache"), config.PageCacheBytes)
	if err != nil {
		panic(fmt.Sprintf("lsm: create page cache: %v", err))
	}
	m := &Manager{
		store:     store,
		cacheDir:  cacheDir,
		config:    config,
		pageCache: pc,
		lease:     lease,
		fs:        fs,
		clock:     clock,

		timelines: store.At("timelines"),
		volRefs:   store.At("volumes"),
		volumes:   make(map[string]*volume),
	}
	return m
}

func (m *Manager) NewVolume(ctx context.Context, name string, size uint64) (loophole.Volume, error) {
	if size == 0 {
		size = DefaultVolumeSize
	}

	timelineID := uuid.NewString()

	// Write timeline meta.json.
	meta := TimelineMeta{
		CreatedAt: m.clock.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if _, err := m.timelines.At(timelineID).PutIfNotExists(ctx, "meta.json", metaData); err != nil {
		return nil, fmt.Errorf("create timeline meta: %w", err)
	}

	// Write volume ref.
	ref := volumeRef{TimelineID: timelineID, Size: size}
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

func (m *Manager) DeleteVolume(ctx context.Context, name string) error {
	m.mu.Lock()
	if _, ok := m.volumes[name]; ok {
		m.mu.Unlock()
		return fmt.Errorf("volume %q is open; close it first", name)
	}
	m.mu.Unlock()

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
	return m.pageCache.Close()
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
	tl, err := openTimeline(ctx, m.timelines.At(ref.TimelineID), m.timelines, ref.TimelineID, m.cacheDir, m.config, m.pageCache, m.fs, m.clock)
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

	v := newVolume(name, ref.Size, tl, m)
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
