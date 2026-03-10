package snapshotter_test

import (
	"context"

	snapshotspb "github.com/semistrict/loophole/internal/containerdapi/services/snapshots/v1"
	"google.golang.org/grpc/metadata"
)

// listStream is a fake Snapshots_ListServer that captures responses.
type listStream struct {
	responses []*snapshotspb.ListSnapshotsResponse
}

func (s *listStream) Send(resp *snapshotspb.ListSnapshotsResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *listStream) SetHeader(metadata.MD) error  { return nil }
func (s *listStream) SendHeader(metadata.MD) error { return nil }
func (s *listStream) SetTrailer(metadata.MD)       {}
func (s *listStream) Context() context.Context     { return context.Background() }
func (s *listStream) SendMsg(any) error            { return nil }
func (s *listStream) RecvMsg(any) error            { return nil }
