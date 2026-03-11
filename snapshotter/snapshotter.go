package snapshotter

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
	snapshotspb "github.com/semistrict/loophole/internal/containerdapi/services/snapshots/v1"
	typespb "github.com/semistrict/loophole/internal/containerdapi/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultVolumeSize = 16 * 1024 * 1024 * 1024 // 16 GB

// Snapshotter implements the containerd snapshots gRPC service backed by
// loophole volumes.
type Snapshotter struct {
	snapshotspb.UnimplementedSnapshotsServer

	backend           fsbackend.Service
	snaps             *snapshotStore
	snapshotDir       string
	DefaultVolumeSize uint64 // 0 means use defaultVolumeSize (16 GB)
}

// New creates a new Snapshotter.
func New(backend fsbackend.Service, snapshotDir string) *Snapshotter {
	return &Snapshotter{
		backend:     backend,
		snaps:       newSnapshotStore(),
		snapshotDir: snapshotDir,
	}
}

// safeDirName encodes a containerd key for use as a directory name.
func safeDirName(key string) string {
	s := strings.ReplaceAll(key, "%", "%25")
	s = strings.ReplaceAll(s, "/", "%2F")
	return s
}

func (s *Snapshotter) mountpoint(key string) string {
	return s.snapshotDir + "/" + safeDirName(key)
}

func (s *Snapshotter) bindMount(mountpoint string, ro bool) []*typespb.Mount {
	opts := []string{"rbind", "rw"}
	if ro {
		opts = []string{"rbind", "ro"}
	}
	return []*typespb.Mount{{
		Type:    "bind",
		Source:  mountpoint,
		Options: opts,
	}}
}

// Prepare creates a new writable snapshot.
func (s *Snapshotter) Prepare(ctx context.Context, req *snapshotspb.PrepareSnapshotRequest) (*snapshotspb.PrepareSnapshotResponse, error) {
	key := req.GetKey()
	parent := req.GetParent()
	slog.Info("snapshotter: Prepare", "key", key, "parent", parent)

	if _, err := s.snaps.get(key); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "snapshot %q already exists", key)
	}

	mp := s.mountpoint(key)
	if err := os.MkdirAll(mp, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir %s: %v", mp, err)
	}

	if parent == "" {
		// Fresh volume, no parent.
		volSize := s.DefaultVolumeSize
		if volSize == 0 {
			volSize = defaultVolumeSize
		}
		if err := s.backend.Create(ctx, loophole.CreateParams{
			Volume: key,
			Size:   volSize,
			Type:   loophole.VolumeTypeExt4,
			Labels: req.GetLabels(),
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "create volume: %v", err)
		}
		if err := s.backend.Mount(ctx, key, mp); err != nil {
			return nil, status.Errorf(codes.Internal, "mount: %v", err)
		}
	} else {
		// Clone from committed parent.
		parentMeta, err := s.snaps.get(parent)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "parent %q: %v", parent, err)
		}

		parentMP := s.mountpoint(parent)
		// Ensure parent is mounted for clone.
		if parentMeta.Mountpoint == "" {
			if err := s.backend.Mount(ctx, parentMeta.VolumeName, parentMP); err != nil {
				return nil, status.Errorf(codes.Internal, "mount parent: %v", err)
			}
			parentMeta.Mountpoint = parentMP
			s.snaps.put(parent, parentMeta)
		}

		if err := s.backend.Clone(ctx, parentMeta.Mountpoint, key, mp); err != nil {
			return nil, status.Errorf(codes.Internal, "clone: %v", err)
		}
	}

	now := time.Now()
	s.snaps.put(key, SnapshotMeta{
		VolumeName: key,
		Parent:     parent,
		Kind:       KindActive,
		Labels:     req.GetLabels(),
		CreatedAt:  now,
		UpdatedAt:  now,
		Mountpoint: mp,
	})

	return &snapshotspb.PrepareSnapshotResponse{
		Mounts: s.bindMount(mp, false),
	}, nil
}

// View creates a read-only snapshot view.
func (s *Snapshotter) View(ctx context.Context, req *snapshotspb.ViewSnapshotRequest) (*snapshotspb.ViewSnapshotResponse, error) {
	key := req.GetKey()
	parent := req.GetParent()
	slog.Info("snapshotter: View", "key", key, "parent", parent)

	if _, err := s.snaps.get(key); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "snapshot %q already exists", key)
	}

	parentMeta, err := s.snaps.get(parent)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "parent %q: %v", parent, err)
	}

	mp := s.mountpoint(key)
	if err := os.MkdirAll(mp, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir: %v", err)
	}

	parentMP := s.mountpoint(parent)
	if parentMeta.Mountpoint == "" {
		if err := s.backend.Mount(ctx, parentMeta.VolumeName, parentMP); err != nil {
			return nil, status.Errorf(codes.Internal, "mount parent: %v", err)
		}
		parentMeta.Mountpoint = parentMP
		s.snaps.put(parent, parentMeta)
	}

	if err := s.backend.Clone(ctx, parentMeta.Mountpoint, key, mp); err != nil {
		return nil, status.Errorf(codes.Internal, "clone for view: %v", err)
	}

	now := time.Now()
	s.snaps.put(key, SnapshotMeta{
		VolumeName: key,
		Parent:     parent,
		Kind:       KindView,
		Labels:     req.GetLabels(),
		CreatedAt:  now,
		UpdatedAt:  now,
		Mountpoint: mp,
	})

	return &snapshotspb.ViewSnapshotResponse{
		Mounts: s.bindMount(mp, true),
	}, nil
}

// Mounts returns the mounts for an existing snapshot.
func (s *Snapshotter) Mounts(ctx context.Context, req *snapshotspb.MountsRequest) (*snapshotspb.MountsResponse, error) {
	key := req.GetKey()
	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Re-mount if needed.
	if meta.Mountpoint == "" {
		mp := s.mountpoint(key)
		if err := os.MkdirAll(mp, 0o755); err != nil {
			return nil, status.Errorf(codes.Internal, "mkdir: %v", err)
		}
		if err := s.backend.Mount(ctx, meta.VolumeName, mp); err != nil {
			return nil, status.Errorf(codes.Internal, "mount: %v", err)
		}
		meta.Mountpoint = mp
		s.snaps.put(key, meta)
	}

	ro := meta.Kind == KindView
	return &snapshotspb.MountsResponse{
		Mounts: s.bindMount(meta.Mountpoint, ro),
	}, nil
}

// Commit freezes an active snapshot and re-registers it under a new name.
func (s *Snapshotter) Commit(ctx context.Context, req *snapshotspb.CommitSnapshotRequest) (*emptypb.Empty, error) {
	name := req.GetName()
	key := req.GetKey()
	slog.Info("snapshotter: Commit", "name", name, "key", key)

	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if meta.Kind != KindActive {
		return nil, status.Errorf(codes.FailedPrecondition, "snapshot %q is not active", key)
	}

	if err := s.backend.FreezeVolume(ctx, meta.VolumeName, false); err != nil {
		return nil, status.Errorf(codes.Internal, "freeze: %v", err)
	}

	now := time.Now()
	labels := req.GetLabels()
	if labels == nil {
		labels = meta.Labels
	}

	s.snaps.delete(key)
	s.snaps.put(name, SnapshotMeta{
		VolumeName: meta.VolumeName,
		Parent:     meta.Parent,
		Kind:       KindCommitted,
		Labels:     labels,
		CreatedAt:  meta.CreatedAt,
		UpdatedAt:  now,
	})

	return &emptypb.Empty{}, nil
}

// Remove deletes a snapshot.
func (s *Snapshotter) Remove(ctx context.Context, req *snapshotspb.RemoveSnapshotRequest) (*emptypb.Empty, error) {
	key := req.GetKey()
	slog.Info("snapshotter: Remove", "key", key)

	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if meta.Mountpoint != "" {
		if err := s.backend.Unmount(ctx, meta.Mountpoint); err != nil {
			slog.Warn("snapshotter: unmount on remove", "key", key, "error", err)
		}
	}

	// After unmount, the FUSE RELEASE callback may still be in-flight.
	// Wait for the volume to be fully closed before deleting.
	if err := s.backend.VM().WaitClosed(ctx, meta.VolumeName); err != nil {
		slog.Warn("snapshotter: wait closed on remove", "key", key, "error", err)
	}

	if err := s.backend.VM().DeleteVolume(ctx, meta.VolumeName); err != nil {
		slog.Warn("snapshotter: delete volume on remove", "key", key, "error", err)
	}

	s.snaps.delete(key)
	return &emptypb.Empty{}, nil
}

// Stat returns metadata for a snapshot.
func (s *Snapshotter) Stat(ctx context.Context, req *snapshotspb.StatSnapshotRequest) (*snapshotspb.StatSnapshotResponse, error) {
	key := req.GetKey()
	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &snapshotspb.StatSnapshotResponse{
		Info: metaToInfo(key, meta),
	}, nil
}

// Update updates snapshot metadata (labels only).
func (s *Snapshotter) Update(ctx context.Context, req *snapshotspb.UpdateSnapshotRequest) (*snapshotspb.UpdateSnapshotResponse, error) {
	info := req.GetInfo()
	if info == nil {
		return nil, status.Errorf(codes.InvalidArgument, "info required")
	}
	key := info.GetName()

	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	meta.Labels = info.GetLabels()
	meta.UpdatedAt = time.Now()
	s.snaps.put(key, meta)

	// Also persist labels to the S3 volume ref.
	if err := s.backend.VM().UpdateLabels(ctx, meta.VolumeName, meta.Labels); err != nil {
		slog.Warn("snapshotter: update labels on S3 ref", "key", key, "error", err)
	}

	return &snapshotspb.UpdateSnapshotResponse{
		Info: metaToInfo(key, meta),
	}, nil
}

// List streams all snapshots.
func (s *Snapshotter) List(req *snapshotspb.ListSnapshotsRequest, srv snapshotspb.Snapshots_ListServer) error {
	all := s.snaps.list()
	var infos []*snapshotspb.Info
	for key, meta := range all {
		infos = append(infos, metaToInfo(key, meta))
	}
	// Send in a single batch.
	if len(infos) > 0 {
		if err := srv.Send(&snapshotspb.ListSnapshotsResponse{Info: infos}); err != nil {
			return err
		}
	}
	return nil
}

// Usage returns disk usage for a snapshot (approximate).
func (s *Snapshotter) Usage(ctx context.Context, req *snapshotspb.UsageRequest) (*snapshotspb.UsageResponse, error) {
	key := req.GetKey()
	meta, err := s.snaps.get(key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Best-effort: report the volume size from S3 metadata.
	info, err := s.backend.VM().VolumeInfo(ctx, meta.VolumeName)
	if err != nil {
		return &snapshotspb.UsageResponse{}, nil
	}
	return &snapshotspb.UsageResponse{
		Size: int64(info.Size),
	}, nil
}

// Cleanup is a no-op for now.
func (s *Snapshotter) Cleanup(ctx context.Context, req *snapshotspb.CleanupRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func metaToInfo(key string, meta SnapshotMeta) *snapshotspb.Info {
	kind := snapshotspb.Kind_UNKNOWN
	switch meta.Kind {
	case KindView:
		kind = snapshotspb.Kind_VIEW
	case KindActive:
		kind = snapshotspb.Kind_ACTIVE
	case KindCommitted:
		kind = snapshotspb.Kind_COMMITTED
	}
	info := &snapshotspb.Info{
		Name:      key,
		Parent:    meta.Parent,
		Kind:      kind,
		CreatedAt: timestamppb.New(meta.CreatedAt),
		UpdatedAt: timestamppb.New(meta.UpdatedAt),
		Labels:    meta.Labels,
	}
	return info
}
