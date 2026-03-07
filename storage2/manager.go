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

// volumeRef is the S3-persisted mapping from volume name to layer ID.
type volumeRef struct {
	TimelineID string `json:"timeline_id"` // kept as timeline_id for compat
	Size       uint64 `json:"size,omitempty"`
	ReadOnly   bool   `json:"read_only,omitempty"`
	Type       string `json:"type,omitempty"`
}

// Manager manages volumes backed by storage2 layers.
type Manager struct {
	store     loophole.ObjectStore
	cacheDir  string
	config    Config
	diskCache *PageCache
	fs        LocalFS
	idGen     func() string

	volRefs loophole.ObjectStore // store.At("volumes")

	mu      sync.Mutex
	volumes map[string]*volume
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
	config.setDefaults()
	if fs == nil {
		fs = OSLocalFS{}
	}
	return &Manager{
		store:     store,
		cacheDir:  cacheDir,
		config:    config,
		diskCache: diskCache,
		fs:        fs,
		idGen:     uuid.NewString,
		volRefs:   store.At("volumes"),
		volumes:   make(map[string]*volume),
	}
}

func (m *Manager) NewVolume(ctx context.Context, name string, size uint64, volType string) (loophole.Volume, error) {
	slog.Info("storage2: NewVolume", "name", name, "size", size, "type", volType)
	if size == 0 {
		size = DefaultVolumeSize
	}

	layerID := m.idGen()

	// Write layer meta.json.
	meta := layerMeta{
		CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	layerStore := m.store.At("layers/" + layerID)
	if err := layerStore.PutIfNotExists(ctx, "meta.json", metaData); err != nil {
		return nil, fmt.Errorf("create layer meta: %w", err)
	}

	// Write volume ref.
	ref := volumeRef{TimelineID: layerID, Size: size, Type: volType}
	refData, err := json.Marshal(ref)
	if err != nil {
		return nil, err
	}
	if err := m.volRefs.PutIfNotExists(ctx, name, refData); err != nil {
		if errors.Is(err, loophole.ErrExists) {
			return nil, fmt.Errorf("volume %q already exists", name)
		}
		return nil, fmt.Errorf("create volume ref: %w", err)
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

	ref, err := m.getVolumeRef(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolve volume %q: %w", name, err)
	}
	return m.openVolume(ctx, name, ref)
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
	return nil
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
		v.layer.Close()
	}
	return nil
}

func (m *Manager) getVolumeRef(ctx context.Context, name string) (volumeRef, error) {
	ref, _, err := loophole.ReadJSON[volumeRef](ctx, m.volRefs, name)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func (m *Manager) openVolume(ctx context.Context, name string, ref volumeRef) (*volume, error) {
	cacheDir := m.cacheDir + "/layers/" + ref.TimelineID
	ly, err := openLayer(ctx, m.store, ref.TimelineID, m.config, m.diskCache, cacheDir)
	if err != nil {
		return nil, fmt.Errorf("open layer %q: %w", ref.TimelineID, err)
	}

	if m.config.FlushInterval > 0 {
		ly.startPeriodicFlush(ctx)
	}

	v := newVolume(name, ref.Size, ref.Type, ly, m)
	if ref.ReadOnly {
		v.readOnly.Store(true)
	}

	m.mu.Lock()
	m.volumes[name] = v
	m.mu.Unlock()

	return v, nil
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
