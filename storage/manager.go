package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/objstore"
)

// Manager manages a single volume backed by a storage layer.
// Each process manages at most one open volume at a time.
//
// Set exported fields before calling any methods. Internal state is
// lazily initialized on first use.
type Manager struct {
	ObjectStore objstore.ObjectStore
	CacheDir    string // page cache daemon directory; empty = no persistent cache
	config      Config
	fs          localFS

	// lazily initialized
	store     objstore.ObjectStore
	diskCache PageCache
	safepoint *safepoint.Safepoint
	volRefs   objstore.ObjectStore
	initOnce  sync.Once
	initErr   error

	mu     sync.Mutex
	cond   *sync.Cond
	volume *Volume
}

// localFS abstracts local filesystem operations for dirty pages backing files.
type localFS interface {
	MkdirAll(path string, perm uint32) error
}

// osLocalFS is the default localFS using the OS filesystem.
type osLocalFS struct{}

func (osLocalFS) MkdirAll(path string, perm uint32) error {
	return ensureMemDir(path)
}

func (m *Manager) init() error {
	m.initOnce.Do(func() {
		m.config.setDefaults()
		m.store = m.ObjectStore
		if m.fs == nil {
			m.fs = osLocalFS{}
		}
		m.volRefs = m.store.At("volumes")
		m.cond = sync.NewCond(&m.mu)

		m.safepoint = safepoint.New()

		if m.diskCache == nil && m.CacheDir != "" {
			pc, err := cached.NewPageCache(filepath.Join(m.CacheDir, "diskcache"), m.safepoint)
			if err != nil {
				m.initErr = fmt.Errorf("open page cache: %w", err)
				return
			}
			m.diskCache = pc
		}
	})
	return m.initErr
}

func (m *Manager) NewVolume(p CreateParams) (*Volume, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	ctx := context.Background()
	name := p.Volume
	size := p.Size
	volType := p.Type
	if err := ValidateVolumeName(name); err != nil {
		return nil, err
	}
	if p.Parent != "" {
		if err := ValidateVolumeName(p.Parent); err != nil {
			return nil, fmt.Errorf("invalid parent volume: %w", err)
		}
	}
	slog.Info("storage: NewVolume", "name", name, "size", size, "type", volType)
	if size == 0 {
		size = DefaultVolumeSize
	}

	layerID := newLayerID()

	// Write initial index.json with created_at in object metadata.
	idx := layerIndex{NextSeq: 1, LayoutGen: 1}
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
	ref := volumeRef{LayerID: layerID, Size: size, Type: volType, Parent: p.Parent, Labels: p.Labels}
	if err := putVolumeRefNew(ctx, m.volRefs, name, ref); err != nil {
		return nil, err
	}

	return m.openVolume(name, ref, false)
}

func (m *Manager) OpenVolume(name string) (*Volume, error) {
	return m.openVolumeByName(name, false)
}

// OpenVolumeReadOnly opens a volume for reads only. It does not acquire a
// write lease or start background flush loops, so it can safely follow an
// actively written volume.
func (m *Manager) OpenVolumeReadOnly(name string) (*Volume, error) {
	return m.openVolumeByName(name, true)
}

func (m *Manager) openVolumeByName(name string, readOnly bool) (*Volume, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	if err := ValidateVolumeName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.volume != nil && m.volume.Name() == name {
		v := m.volume
		m.mu.Unlock()
		return v, nil
	}
	m.mu.Unlock()

	ref, err := getVolumeRef(context.Background(), m.volRefs, name)
	if err != nil {
		return nil, fmt.Errorf("resolve volume %q: %w", name, err)
	}
	return m.openVolume(name, ref, readOnly)
}

func (m *Manager) GetVolume(name string) *Volume {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.volume != nil && m.volume.Name() == name {
		return m.volume
	}
	return nil
}

// Volume returns the single open volume, or nil.
func (m *Manager) Volume() *Volume {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volume
}

func (m *Manager) Volumes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.volume != nil {
		return []string{m.volume.Name()}
	}
	return nil
}

// WaitClosed blocks until the named volume is no longer open in this manager,
// or until ctx is cancelled.
func (m *Manager) WaitClosed(ctx context.Context, name string) error {
	done := ctx.Done()
	m.mu.Lock()
	for {
		if m.volume == nil || m.volume.Name() != name {
			m.mu.Unlock()
			return nil
		}
		select {
		case <-done:
			m.mu.Unlock()
			return ctx.Err()
		default:
		}
		ch := make(chan struct{})
		go func() {
			select {
			case <-done:
				m.cond.Broadcast()
			case <-ch:
			}
		}()
		m.cond.Wait()
		close(ch)
	}
}

// CloseVolume releases the manager's ref on a volume, triggering destruction
// if no other refs remain (e.g. mounts). Use this after Unmount to fully close
// a volume that was opened via OpenVolume/NewVolume.
func (m *Manager) CloseVolume(name string) error {
	if err := ValidateVolumeName(name); err != nil {
		return err
	}
	m.mu.Lock()
	v := m.volume
	m.mu.Unlock()
	if v == nil || v.Name() != name {
		return nil
	}
	return v.ReleaseRef()
}

func (m *Manager) PageSize() int {
	return PageSize
}

// Store returns the underlying object store.
func (m *Manager) Store() objstore.ObjectStore {
	if err := m.init(); err != nil {
		slog.Error("manager: init for store failed", "error", err)
		return nil
	}
	return m.store
}

func (m *Manager) Close() error {
	if err := m.init(); err != nil {
		return err
	}
	ctx := context.Background()

	m.mu.Lock()
	v := m.volume
	m.volume = nil
	m.mu.Unlock()

	if v != nil {
		v.layer.beginShutdown()
		slog.Info("manager close: flushing", "volume", v.Name())
		if err := v.flush(); err != nil {
			slog.Warn("flush on close failed", "volume", v.Name(), "error", err)
		}
		slog.Info("manager close: releasing lease", "volume", v.Name())
		v.releaseLease(ctx)
		slog.Info("manager close: closing volume", "volume", v.Name())
		v.close()
		slog.Info("manager close: volume closed", "volume", v.Name())
	}
	if m.diskCache != nil {
		util.SafeClose(m.diskCache, "close manager disk cache")
		m.diskCache = nil
	}
	return nil
}

// --- internal ---

func (m *Manager) openVolume(name string, ref volumeRef, readOnly bool) (*Volume, error) {
	// Check object metadata on index.json (HEAD, no body) to see if frozen.
	ctx := context.Background()
	layerStore := m.store.At("layers/" + ref.LayerID)
	meta, _ := layerStore.HeadMeta(ctx, "index.json")
	if meta["frozen_at"] != "" {
		return nil, fmt.Errorf("volume %q points to frozen layer %q; use a checkpoint clone instead", name, ref.LayerID)
	}

	v := newVolume(name, ref.Size, ref.Type, nil, m)
	v.readOnly = readOnly

	// Acquire write lease before opening the layer so we have the
	// writeLeaseSeq for file naming.
	var writeLeaseSeq uint64
	if !readOnly && ref.LeaseToken != v.leaseToken() {
		seq, err := v.acquireLease(ctx)
		if err != nil {
			v.closeLeaseSession(ctx)
			return nil, fmt.Errorf("acquire lease for %q: %w", name, err)
		}
		writeLeaseSeq = seq
	}

	ly, err := openLayer(ctx, layerParams{
		store:     m.store,
		id:        ref.LayerID,
		config:    m.config,
		diskCache: m.diskCache,
		safepoint: m.safepoint,
	})
	if err != nil {
		if writeLeaseSeq > 0 {
			v.releaseLease(ctx)
		}
		v.closeLeaseSession(ctx)
		return nil, fmt.Errorf("open layer %q: %w", ref.LayerID, err)
	}
	ly.writeLeaseSeq = writeLeaseSeq
	if readOnly {
		ly.stopPeriodicFlush()
	}
	v.layer = ly

	m.mu.Lock()
	if m.volume != nil {
		if m.volume.Name() == name {
			existing := m.volume
			m.mu.Unlock()
			v.close()
			if writeLeaseSeq > 0 {
				v.releaseLease(ctx)
			}
			return existing, nil
		}
		current := m.volume.Name()
		m.mu.Unlock()
		v.close()
		if writeLeaseSeq > 0 {
			v.releaseLease(ctx)
		}
		return nil, fmt.Errorf("manager already has volume %q open; cannot open %q", current, name)
	}
	m.volume = v
	m.mu.Unlock()

	return v, nil
}

func (m *Manager) closeVolume(name string) {
	m.mu.Lock()
	if m.volume != nil && m.volume.Name() == name {
		m.volume = nil
	}
	m.cond.Broadcast()
	m.mu.Unlock()
}
