package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/objstore"
)

// checkpointRef is the S3-persisted metadata for a volume checkpoint.
type checkpointRef struct {
	LayerID   string `json:"layer_id"`
	CreatedAt string `json:"created_at"`
}

// volumeRef is the S3-persisted mapping from volume name to layer ID.
type volumeRef struct {
	LayerID       string            `json:"layer_id"`
	Size          uint64            `json:"size,omitempty"`
	Type          string            `json:"type,omitempty"`
	LeaseToken    string            `json:"lease_token,omitempty"`
	WriteLeaseSeq uint64            `json:"write_lease_seq,omitempty"`
	Parent        string            `json:"parent,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

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
	workDir       string
	store         objstore.ObjectStore // retry-wrapped ObjectStore
	diskCache     PageCache
	ownsDiskCache bool
	safepoint     *safepoint.Safepoint
	lease         *objstore.LeaseManager
	idGen         func() string
	volRefs       objstore.ObjectStore
	initOnce      sync.Once
	initErr       error

	mu        sync.Mutex
	cond      *sync.Cond
	volume    *Volume
	onRelease func(ctx context.Context, volumeName string)
}

// localFS abstracts local filesystem operations for memtable backing files.
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
		m.store = objstore.NewRetryStore(m.ObjectStore)
		if m.fs == nil {
			m.fs = osLocalFS{}
		}
		if m.idGen == nil {
			m.idGen = uuid.NewString
		}
		m.volRefs = m.store.At("volumes")
		m.lease = objstore.NewLeaseManager(m.store.At("leases"))
		m.cond = sync.NewCond(&m.mu)
		m.lease.Handle("release", m.handleRelease)

		m.safepoint = safepoint.New()

		workDir, err := os.MkdirTemp("", "loophole-work-*")
		if err != nil {
			m.initErr = fmt.Errorf("create work dir: %w", err)
			return
		}
		m.workDir = workDir

		if m.diskCache == nil && m.CacheDir != "" {
			pc, err := cached.NewPageCache(filepath.Join(m.CacheDir, "diskcache"), m.safepoint)
			if err != nil {
				m.initErr = fmt.Errorf("open page cache: %w", err)
				return
			}
			m.diskCache = pc
			m.ownsDiskCache = true
		}
	})
	return m.initErr
}

// SetOnRelease sets a callback invoked when a remote break-lease request
// is received, before the volume is closed at the storage layer.
func (m *Manager) SetOnRelease(fn func(ctx context.Context, volumeName string)) {
	m.onRelease = fn
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

	layerID := m.idGen()

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
	if err := m.putVolumeRefNew(ctx, name, ref); err != nil {
		return nil, err
	}

	return m.openVolume(name, ref)
}

func (m *Manager) OpenVolume(name string) (*Volume, error) {
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

	ref, err := m.getVolumeRef(context.Background(), name)
	if err != nil {
		return nil, fmt.Errorf("resolve volume %q: %w", name, err)
	}
	return m.openVolume(name, ref)
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

// ListAllVolumes returns the names of all volumes in the store.
func ListAllVolumes(ctx context.Context, store objstore.ObjectStore) ([]string, error) {
	volRefs := store.At("volumes")
	objects, err := volRefs.ListKeys(ctx, "")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, obj := range objects {
		// Keys are "{name}/index.json" or "{name}/checkpoints/...".
		// Extract the first path segment as the volume name.
		name, _, ok := strings.Cut(obj.Key, "/")
		if ok && name != "" && ValidateVolumeName(name) == nil {
			seen[name] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (m *Manager) VolumeInfo(ctx context.Context, name string) (VolumeInfo, error) {
	if err := m.init(); err != nil {
		return VolumeInfo{}, err
	}
	if err := ValidateVolumeName(name); err != nil {
		return VolumeInfo{}, err
	}
	ref, err := m.getVolumeRef(ctx, name)
	if err != nil {
		return VolumeInfo{}, err
	}
	return VolumeInfo{
		Name:   name,
		Size:   ref.Size,
		Type:   ref.Type,
		Parent: ref.Parent,
		Labels: ref.Labels,
	}, nil
}

func (m *Manager) UpdateLabels(ctx context.Context, name string, labels map[string]string) error {
	if err := m.init(); err != nil {
		return err
	}
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	return objstore.ModifyJSON[volumeRef](ctx, m.volRefs, key, func(ref *volumeRef) error {
		ref.Labels = labels
		return nil
	})
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

func (m *Manager) DeleteVolume(ctx context.Context, name string) error {
	if err := m.init(); err != nil {
		return err
	}
	if err := ValidateVolumeName(name); err != nil {
		return err
	}
	m.mu.Lock()
	if m.volume != nil && m.volume.Name() == name {
		m.mu.Unlock()
		return fmt.Errorf("volume %q is open; close it first", name)
	}
	m.mu.Unlock()

	// Acquire lease to ensure no other daemon is using this volume.
	if _, err := m.acquireVolumeLease(ctx, name); err != nil {
		return err
	}
	defer m.releaseVolumeLease(ctx, name)

	// Delete checkpoints first.
	cpPrefix, err := checkpointPrefix(name)
	if err != nil {
		return err
	}
	cpKeys, _ := m.volRefs.ListKeys(ctx, cpPrefix)
	for _, obj := range cpKeys {
		_ = m.volRefs.DeleteObject(ctx, obj.Key)
	}
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	if err := m.volRefs.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("delete volume ref %q: %w", name, err)
	}
	return nil
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
		slog.Info("manager close: flushing", "volume", v.Name())
		if err := v.flush(); err != nil {
			slog.Warn("flush on close failed", "volume", v.Name(), "error", err)
		}
		slog.Info("manager close: releasing lease", "volume", v.Name())
		m.releaseVolumeLease(ctx, v.Name())
		slog.Info("manager close: closing volume", "volume", v.Name())
		v.close()
		slog.Info("manager close: volume closed", "volume", v.Name())
	}
	slog.Info("manager close: closing lease manager")
	if err := m.lease.Close(ctx); err != nil {
		return err
	}
	if m.ownsDiskCache && m.diskCache != nil {
		util.SafeClose(m.diskCache, "close manager disk cache")
		m.diskCache = nil
	}
	if m.workDir != "" {
		if err := os.RemoveAll(m.workDir); err != nil {
			return fmt.Errorf("remove manager work dir: %w", err)
		}
		m.workDir = ""
	}
	return nil
}

// BreakLease requests the remote holder to release a volume.
func (m *Manager) BreakLease(ctx context.Context, volumeName string, force bool) (bool, error) {
	if err := m.init(); err != nil {
		return false, err
	}
	key, err := volumeIndexKey(volumeName)
	if err != nil {
		return false, err
	}
	ref, etag, err := objstore.ReadJSON[volumeRef](ctx, m.volRefs, key)
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
	if _, err := m.volRefs.PutBytesCAS(ctx, key, data, etag); err != nil {
		return graceful, fmt.Errorf("clear lease token: %w", err)
	}

	// Delete the lease file (best-effort).
	if err := m.store.At("leases").DeleteObject(ctx, oldToken+".json"); err != nil {
		slog.Warn("delete stale lease file", "token", oldToken, "error", err)
	}

	return graceful, nil
}

// ForceClearLease clears a volume's lease token without contacting the holder.
// Use this only when the caller has already determined the holder is dead.
func (m *Manager) ForceClearLease(ctx context.Context, volumeName string) error {
	if err := m.init(); err != nil {
		return err
	}
	key, err := volumeIndexKey(volumeName)
	if err != nil {
		return err
	}
	ref, etag, err := objstore.ReadJSON[volumeRef](ctx, m.volRefs, key)
	if err != nil {
		return fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}
	if ref.LeaseToken == "" {
		return nil
	}
	ref.LeaseToken = ""
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if _, err := m.volRefs.PutBytesCAS(ctx, key, data, etag); err != nil {
		return fmt.Errorf("clear lease token: %w", err)
	}
	return nil
}

// --- internal ---

func (m *Manager) getVolumeRef(ctx context.Context, name string) (volumeRef, error) {
	key, err := volumeIndexKey(name)
	if err != nil {
		return volumeRef{}, err
	}
	ref, _, err := objstore.ReadJSON[volumeRef](ctx, m.volRefs, key)
	if err != nil {
		return volumeRef{}, err
	}
	return ref, nil
}

func (m *Manager) openVolume(name string, ref volumeRef) (*Volume, error) {
	// Check object metadata on index.json (HEAD, no body) to see if frozen.
	ctx := context.Background()
	layerStore := m.store.At("layers/" + ref.LayerID)
	meta, _ := layerStore.HeadMeta(ctx, "index.json")
	if meta["frozen_at"] != "" {
		return nil, fmt.Errorf("volume %q points to frozen layer %q; use a checkpoint clone instead", name, ref.LayerID)
	}

	// Acquire write lease before opening the layer so we have the
	// writeLeaseSeq for file naming.
	var writeLeaseSeq uint64
	if ref.LeaseToken != m.lease.Token() {
		seq, err := m.acquireVolumeLease(ctx, name)
		if err != nil {
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
		workDir:   filepath.Join(m.workDir, "layers", ref.LayerID),
	})
	if err != nil {
		if writeLeaseSeq > 0 {
			m.releaseVolumeLease(ctx, name)
		}
		return nil, fmt.Errorf("open layer %q: %w", ref.LayerID, err)
	}
	ly.writeLeaseSeq = writeLeaseSeq
	ly.lease = m.lease

	if m.config.FlushInterval > 0 {
		// Use a background context — ctx may be a short-lived HTTP request
		// context that gets cancelled when the handler returns.
		ly.startPeriodicFlush(context.Background())
	}

	v := newVolume(name, ref.Size, ref.Type, ly, m)

	m.mu.Lock()
	if m.volume != nil {
		if m.volume.Name() == name {
			existing := m.volume
			m.mu.Unlock()
			ly.Close()
			m.releaseVolumeLease(ctx, name)
			return existing, nil
		}
		m.mu.Unlock()
		ly.Close()
		m.releaseVolumeLease(ctx, name)
		return nil, fmt.Errorf("manager already has volume %q open; cannot open %q", m.volume.Name(), name)
	}
	m.volume = v
	m.mu.Unlock()

	return v, nil
}

func (m *Manager) putVolumeRefNew(ctx context.Context, name string, ref volumeRef) error {
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := m.volRefs.PutIfNotExists(ctx, key, data); err != nil {
		if errors.Is(err, objstore.ErrExists) {
			return fmt.Errorf("volume %q already exists", name)
		}
		return fmt.Errorf("create volume ref: %w", err)
	}
	return nil
}

func (m *Manager) putVolumeRef(ctx context.Context, name string, ref volumeRef) error {
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := m.volRefs.PutIfNotExists(ctx, key, data); err != nil {
		if errors.Is(err, objstore.ErrExists) {
			return fmt.Errorf("volume %q already exists", name)
		}
		return err
	}
	return nil
}

func (m *Manager) closeVolume(name string) {
	m.mu.Lock()
	if m.volume != nil && m.volume.Name() == name {
		m.volume = nil
	}
	m.cond.Broadcast()
	m.mu.Unlock()
}

// acquireVolumeLease checks the volume ref's lease token and writes ours.
// Atomically increments WriteLeaseSeq in the same CAS and returns the new value.
func (m *Manager) acquireVolumeLease(ctx context.Context, name string) (uint64, error) {
	if err := m.lease.EnsureStarted(ctx); err != nil {
		return 0, fmt.Errorf("start lease: %w", err)
	}
	key, err := volumeIndexKey(name)
	if err != nil {
		return 0, err
	}
	var seq uint64
	err = objstore.ModifyJSON[volumeRef](ctx, m.volRefs, key, func(ref *volumeRef) error {
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

// relayerVolume atomically updates a volume ref to point to a new layer ID
// and increments WriteLeaseSeq. Used after Snapshot to switch the parent
// volume to a new layer so the old layer is never written to again.
func (m *Manager) relayerVolume(ctx context.Context, name string, newLayerID string) (uint64, error) {
	key, err := volumeIndexKey(name)
	if err != nil {
		return 0, err
	}
	var seq uint64
	err = objstore.ModifyJSON[volumeRef](ctx, m.volRefs, key, func(ref *volumeRef) error {
		ref.LayerID = newLayerID
		ref.WriteLeaseSeq++
		seq = ref.WriteLeaseSeq
		return nil
	})
	return seq, err
}

// releaseVolumeLease clears the lease token from the volume ref if it matches ours.
func (m *Manager) releaseVolumeLease(ctx context.Context, name string) {
	key, err := volumeIndexKey(name)
	if err != nil {
		slog.Warn("release volume lease", "volume", name, "error", err)
		return
	}
	if err := objstore.ModifyJSON[volumeRef](ctx, m.volRefs, key, func(ref *volumeRef) error {
		if ref.LeaseToken == m.lease.Token() {
			ref.LeaseToken = ""
		}
		return nil
	}); err != nil {
		if errors.Is(err, objstore.ErrNotFound) {
			return
		}
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
	v := m.volume
	if v != nil && v.Name() == req.Volume {
		m.volume = nil
	} else {
		v = nil
	}
	m.mu.Unlock()

	if v == nil {
		return map[string]string{"status": "ok"}, nil
	}

	if err := v.flush(); err != nil {
		slog.Warn("release: flush failed", "volume", req.Volume, "error", err)
	}
	m.releaseVolumeLease(ctx, req.Volume)
	v.close()
	return map[string]string{"status": "ok"}, nil
}

// putCheckpoint writes a checkpoint ref under volumes/{name}/checkpoints/{ts}/index.json.
// It generates a timestamp ID and handles collisions by incrementing the second.
func (m *Manager) putCheckpoint(ctx context.Context, volumeName string, layerID string) (string, error) {
	if err := ValidateVolumeName(volumeName); err != nil {
		return "", err
	}
	now := time.Now().UTC()
	ts := now.Format("20060102150405")

	// Check for collision and increment if needed.
	key, err := checkpointIndexKey(volumeName, ts)
	if err != nil {
		return "", err
	}
	for attempt := range 60 {
		ref := checkpointRef{
			LayerID:   layerID,
			CreatedAt: now.Format(time.RFC3339),
		}
		data, err := json.Marshal(ref)
		if err != nil {
			return "", err
		}
		err = m.volRefs.PutIfNotExists(ctx, key, data)
		if err == nil {
			return ts, nil
		}
		if !errors.Is(err, objstore.ErrExists) {
			return "", fmt.Errorf("write checkpoint ref: %w", err)
		}
		// Collision — increment second.
		now = now.Add(time.Second)
		ts = now.Format("20060102150405")
		key, err = checkpointIndexKey(volumeName, ts)
		if err != nil {
			return "", err
		}
		_ = attempt
	}
	return "", fmt.Errorf("checkpoint timestamp collision after 60 attempts")
}

// ListCheckpoints returns all checkpoints for a volume, sorted by ID (oldest first).
func (m *Manager) ListCheckpoints(ctx context.Context, volumeName string) ([]CheckpointInfo, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	prefix, err := checkpointPrefix(volumeName)
	if err != nil {
		return nil, err
	}
	objects, err := m.volRefs.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var checkpoints []CheckpointInfo
	for _, obj := range objects {
		// Key format: "{vol}/checkpoints/{ts}/index.json"
		rest := strings.TrimPrefix(obj.Key, prefix)
		ts, _, ok := strings.Cut(rest, "/")
		if !ok || ts == "" {
			continue
		}
		t, err := time.Parse("20060102150405", ts)
		if err != nil {
			continue
		}
		checkpoints = append(checkpoints, CheckpointInfo{
			ID:        ts,
			CreatedAt: t,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].ID < checkpoints[j].ID
	})
	return checkpoints, nil
}

// CloneFromCheckpoint creates a new volume by cloning from a checkpoint's frozen layer.
// This is a metadata-only operation: it reads the checkpoint's layer index and writes
// a new layer index + volume ref that reference the same block ranges.
func (m *Manager) CloneFromCheckpoint(ctx context.Context, volumeName, checkpointID, cloneName string) error {
	if err := m.init(); err != nil {
		return err
	}
	if err := ValidateVolumeName(cloneName); err != nil {
		return err
	}
	cpKey, err := checkpointIndexKey(volumeName, checkpointID)
	if err != nil {
		return err
	}
	cpRef, _, err := objstore.ReadJSON[checkpointRef](ctx, m.volRefs, cpKey)
	if err != nil {
		return fmt.Errorf("read checkpoint %s/%s: %w", volumeName, checkpointID, err)
	}

	volRef, err := m.getVolumeRef(ctx, volumeName)
	if err != nil {
		return fmt.Errorf("read volume ref %q: %w", volumeName, err)
	}

	// Read the checkpoint layer's index — this is all we need to create the clone.
	idx, _, err := objstore.ReadJSON[layerIndex](ctx, m.store.At("layers/"+cpRef.LayerID), "index.json")
	if err != nil {
		return fmt.Errorf("read checkpoint layer index %q: %w", cpRef.LayerID, err)
	}

	childID := m.idGen()
	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index for clone: %w", err)
	}

	ref := volumeRef{
		LayerID: childID,
		Size:    volRef.Size,
		Type:    volRef.Type,
	}
	refData, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("marshal volume ref: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := m.store.At("layers/"+childID).PutIfNotExists(gctx, "index.json", idxData, map[string]string{
			"created_at": time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			return fmt.Errorf("create clone index: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := m.volRefs.PutIfNotExists(gctx, cloneName+"/index.json", refData); err != nil {
			return fmt.Errorf("create clone ref: %w", err)
		}
		return nil
	})
	return g.Wait()
}

func volumeIndexKey(name string) (string, error) {
	if err := ValidateVolumeName(name); err != nil {
		return "", err
	}
	return name + "/index.json", nil
}

func checkpointPrefix(volumeName string) (string, error) {
	if err := ValidateVolumeName(volumeName); err != nil {
		return "", err
	}
	return volumeName + "/checkpoints/", nil
}

func checkpointIndexKey(volumeName, checkpointID string) (string, error) {
	prefix, err := checkpointPrefix(volumeName)
	if err != nil {
		return "", err
	}
	if err := ValidateCheckpointID(checkpointID); err != nil {
		return "", err
	}
	return prefix + checkpointID + "/index.json", nil
}
