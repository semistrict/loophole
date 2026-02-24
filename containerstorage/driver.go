// Package containerstorage implements containers/storage drivers backed by
// loophole. The graph driver maps each layer to a loophole volume mounted as
// ext4 — Create with a parent does a loophole clone (instant, zero-copy CoW)
// instead of a full directory copy. Diff operations are handled by
// NaiveDiffDriver, which walks the mounted ext4 filesystems.
package containerstorage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	graphdriver "go.podman.io/storage/drivers"
	"go.podman.io/storage/pkg/archive"
	"go.podman.io/storage/pkg/directory"
	"go.podman.io/storage/pkg/idtools"
	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/ext4"
)

func init() {
	graphdriver.MustRegister("loophole", Init)
}

// Driver implements graphdriver.ProtoDriver backed by loophole volumes.
type Driver struct {
	home     string
	mountDir string

	inst   loophole.Instance
	client *client.Client

	naiveDiff graphdriver.DiffDriver
	updater   graphdriver.LayerIDMapUpdater

	mu     sync.Mutex
	layers map[string]*layerInfo
}

type layerInfo struct {
	volume string
}

func Init(home string, options graphdriver.Options) (graphdriver.Driver, error) {
	var s3raw, binPath string
	for _, opt := range options.DriverOptions {
		if v, ok := strings.CutPrefix(opt, "loophole.url="); ok {
			s3raw = v
		}
		if v, ok := strings.CutPrefix(opt, "loophole.bin="); ok {
			binPath = v
		}
	}
	if s3raw == "" {
		return nil, fmt.Errorf("loophole driver requires loophole.url=s3://bucket/prefix option")
	}

	inst, err := loophole.ParseInstance(s3raw)
	if err != nil {
		return nil, err
	}
	dir := loophole.DefaultDir()

	// Start the daemon if it isn't already running. The daemon owns the
	// VolumeManager, FUSE server, and leases — we just talk to it over UDS.
	if err := client.EnsureDaemon(dir, inst, binPath); err != nil {
		return nil, fmt.Errorf("ensure daemon: %w", err)
	}

	c := client.New(dir, inst)

	mountDir := filepath.Join(home, "mounts")
	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, err
	}

	// Clean up stale ext4 mounts from a previous process.
	// Init has no ctx parameter (graphdriver.InitFunc signature).
	cleanupStaleMounts(context.TODO(), mountDir)

	d := &Driver{
		home:     home,
		mountDir: mountDir,
		inst:     inst,
		client:   c,
		layers:   make(map[string]*layerInfo),
	}

	d.updater = graphdriver.NewNaiveLayerIDMapUpdater(d)
	d.naiveDiff = graphdriver.NewNaiveDiffDriver(d, d.updater)

	return d, nil
}

func (d *Driver) String() string { return "loophole" }

func (d *Driver) Status() [][2]string {
	status := [][2]string{
		{"S3 URL", d.inst.S3URL()},
	}

	// Status() has no ctx parameter (graphdriver.Driver interface).
	ctx := context.TODO()
	st, err := d.client.Status(ctx)
	if err == nil {
		status = append(status, [2]string{"Volumes (open)", fmt.Sprintf("%d", len(st.Volumes))})
		status = append(status, [2]string{"Mounts (daemon)", fmt.Sprintf("%d", len(st.Mounts))})
	}

	d.mu.Lock()
	mounted := len(d.layers)
	d.mu.Unlock()
	status = append(status, [2]string{"Layers (local)", fmt.Sprintf("%d", mounted)})

	return status
}

func (d *Driver) Metadata(id string) (map[string]string, error) {
	return map[string]string{"volume": id}, nil
}

func (d *Driver) CreateReadWrite(id, parent string, opts *graphdriver.CreateOpts) error {
	// No ctx parameter (graphdriver.Driver interface).
	return d.create(context.TODO(), id, parent)
}

func (d *Driver) Create(id, parent string, opts *graphdriver.CreateOpts) error {
	return d.create(context.TODO(), id, parent)
}

func (d *Driver) CreateFromTemplate(id, template string, templateIDMappings *idtools.IDMappings, parent string, parentIDMappings *idtools.IDMappings, opts *graphdriver.CreateOpts, readWrite bool) error {
	return d.create(context.TODO(), id, template)
}

func (d *Driver) create(ctx context.Context, id, parent string) error {
	mountpoint := d.mountpoint(id)

	if parent == "" {
		// New root layer: mount creates the volume if needed.
		if err := d.client.Mount(ctx, id, mountpoint); err != nil {
			return fmt.Errorf("mount layer %q: %w", id, err)
		}
	} else {
		// Ensure parent is mounted through this daemon. If the daemon
		// restarted, the old FUSE-backed mount is stale and needs to be
		// replaced with a fresh one through the new FUSE server.
		parentMP := d.mountpoint(parent)
		if err := d.client.Mount(ctx, parent, parentMP); err != nil {
			return fmt.Errorf("mount parent %q: %w", parent, err)
		}
		// Clone from parent. The daemon handles freeze/thaw internally.
		if err := d.client.Clone(ctx, parentMP, id, mountpoint); err != nil {
			return fmt.Errorf("clone layer %q from %q: %w", id, parent, err)
		}
	}

	// Set root directory permissions to match containers/storage convention.
	// mkfs.ext4 creates the root with 0o755, but drivers use 0o555.
	if parent == "" {
		if err := os.Chmod(mountpoint, 0o555); err != nil {
			return fmt.Errorf("chmod layer root %q: %w", id, err)
		}
	}

	d.mu.Lock()
	d.layers[id] = &layerInfo{volume: id}
	d.mu.Unlock()

	return nil
}

func (d *Driver) Remove(id string) error {
	// No ctx parameter (graphdriver.Driver interface).
	ctx := context.TODO()
	mountpoint := d.mountpoint(id)

	if err := d.client.Unmount(ctx, mountpoint); err != nil {
		slog.Warn("unmount failed during Remove", "mountpoint", mountpoint, "error", err)
	}
	os.Remove(mountpoint)

	d.mu.Lock()
	delete(d.layers, id)
	d.mu.Unlock()

	return nil
}

func (d *Driver) DeferredRemove(id string) (graphdriver.CleanupTempDirFunc, error) {
	err := d.Remove(id)
	return func() error { return nil }, err
}

func (d *Driver) GetTempDirRootDirs() []string {
	return []string{filepath.Join(d.home, "tmp")}
}

func (d *Driver) Get(id string, options graphdriver.MountOpts) (string, error) {
	mp := d.mountpoint(id)
	if d.isMounted(mp) {
		return mp, nil
	}

	// Layer exists but isn't mounted (e.g. after daemon restart). Mount it.
	// No ctx parameter (graphdriver.Driver interface).
	ctx := context.TODO()
	if err := d.client.Mount(ctx, id, mp); err != nil {
		return "", fmt.Errorf("mount layer %q: %w", id, err)
	}

	d.mu.Lock()
	d.layers[id] = &layerInfo{volume: id}
	d.mu.Unlock()

	return mp, nil
}

func (d *Driver) Put(id string) error {
	return nil
}

func (d *Driver) Exists(id string) bool {
	d.mu.Lock()
	_, ok := d.layers[id]
	d.mu.Unlock()
	if ok {
		return true
	}
	// Check if the volume exists in the store (survives restarts).
	// No ctx parameter (graphdriver.Driver interface).
	ctx := context.TODO()
	volumes, err := d.client.ListVolumes(ctx)
	if err != nil {
		return false
	}
	for _, v := range volumes {
		if v == id {
			return true
		}
	}
	return false
}

func (d *Driver) ListLayers() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	layers := make([]string, 0, len(d.layers))
	for id := range d.layers {
		layers = append(layers, id)
	}
	return layers, nil
}

func (d *Driver) ReadWriteDiskUsage(id string) (*directory.DiskUsage, error) {
	return directory.Usage(d.mountpoint(id))
}

func (d *Driver) AdditionalImageStores() []string { return nil }

func (d *Driver) Cleanup() error {
	// Don't unmount from the daemon — mounts persist across short-lived
	// podman processes. Only Remove() triggers actual unmounts.
	d.mu.Lock()
	d.layers = make(map[string]*layerInfo)
	d.mu.Unlock()
	return nil
}

func (d *Driver) Dedup(req graphdriver.DedupArgs) (graphdriver.DedupResult, error) {
	return graphdriver.DedupResult{}, nil
}

// --- Diff operations: delegated to NaiveDiffDriver ---

func (d *Driver) Diff(id string, idMappings *idtools.IDMappings, parent string, parentMappings *idtools.IDMappings, mountLabel string) (io.ReadCloser, error) {
	return d.naiveDiff.Diff(id, idMappings, parent, parentMappings, mountLabel)
}

func (d *Driver) Changes(id string, idMappings *idtools.IDMappings, parent string, parentMappings *idtools.IDMappings, mountLabel string) ([]archive.Change, error) {
	changes, err := d.naiveDiff.Changes(id, idMappings, parent, parentMappings, mountLabel)
	if err != nil {
		return nil, err
	}
	// Filter out lost+found — ext4 creates it in every filesystem.
	filtered := changes[:0]
	for _, c := range changes {
		if c.Path != "/lost+found" {
			filtered = append(filtered, c)
		}
	}
	return filtered, nil
}

func (d *Driver) ApplyDiff(id string, options graphdriver.ApplyDiffOpts) (int64, error) {
	return d.naiveDiff.ApplyDiff(id, options)
}

func (d *Driver) DiffSize(id string, idMappings *idtools.IDMappings, parent string, parentMappings *idtools.IDMappings, mountLabel string) (int64, error) {
	return d.naiveDiff.DiffSize(id, idMappings, parent, parentMappings, mountLabel)
}

// --- LayerIDMapUpdater ---

func (d *Driver) UpdateLayerIDMap(id string, toContainer, toHost *idtools.IDMappings, mountLabel string) error {
	return d.updater.UpdateLayerIDMap(id, toContainer, toHost, mountLabel)
}

func (d *Driver) SupportsShifting(uidmap, gidmap []idtools.IDMap) bool {
	return d.updater.SupportsShifting(uidmap, gidmap)
}

// --- helpers ---

func (d *Driver) mountpoint(id string) string {
	return filepath.Join(d.mountDir, id)
}

// isMounted checks if path is an active mount point.
func (d *Driver) isMounted(path string) bool {
	return ext4.IsMounted(path)
}

// cleanupStaleMounts unmounts any ext4 filesystems left in mountDir from a
// previous process. Each subdirectory is a potential mount point.
func cleanupStaleMounts(ctx context.Context, mountDir string) {
	entries, err := os.ReadDir(mountDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		mp := filepath.Join(mountDir, e.Name())
		if ext4.IsMounted(mp) {
			if err := ext4.Unmount(ctx, mp); err != nil {
				slog.Warn("cleanup stale mount failed", "mountpoint", mp, "error", err)
			}
		}
	}
}
