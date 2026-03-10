//go:build linux

package daemon

import (
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	snapshotspb "github.com/semistrict/loophole/internal/containerdapi/services/snapshots/v1"
	"github.com/semistrict/loophole/snapshotter"
)

func (d *Daemon) startSnapshotter(sockPath string) error {
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return err
	}
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove stale snapshotter socket", "path", sockPath, "error", err)
	}

	snapshotDir := "/var/lib/loophole/snapshots"
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return err
	}

	ss := snapshotter.New(d.backend, snapshotDir)

	srv := grpc.NewServer()
	snapshotspb.RegisterSnapshotsServer(srv, ss)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}

	d.grpcSrv = srv
	d.grpcLn = ln
	slog.Info("snapshotter gRPC server started", "socket", sockPath)

	go func() {
		if err := srv.Serve(ln); err != nil {
			slog.Error("snapshotter gRPC server error", "error", err)
		}
	}()

	return nil
}

func (d *Daemon) stopSnapshotter() {
	if d.grpcSrv != nil {
		d.grpcSrv.GracefulStop()
	}
	if d.grpcLn != nil {
		d.grpcLn.Close()
	}
}
