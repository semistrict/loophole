package loophole

import "context"

// Volume types.
const (
	VolumeTypeExt4   = "ext4"
	VolumeTypeXFS    = "xfs"
	VolumeTypeSQLite = "sqlite"
)

// CreateParams holds the parameters for creating a new volume.
type CreateParams struct {
	Volume   string `json:"volume"`
	Size     uint64 `json:"size,omitempty"`
	NoFormat bool   `json:"no_format,omitempty"`
	Type     string `json:"type,omitempty"`
}

// VolumeManager manages the lifecycle of volumes.
type VolumeManager interface {
	NewVolume(ctx context.Context, name string, size uint64, volType string) (Volume, error)
	OpenVolume(ctx context.Context, name string) (Volume, error)
	GetVolume(name string) Volume
	Volumes() []string
	ListAllVolumes(ctx context.Context) ([]string, error)
	ListVolumesByType(ctx context.Context, volType string) ([]string, error)
	DeleteVolume(ctx context.Context, name string) error
	PageSize() int
	Close(ctx context.Context) error
}

// Volume is a named, mountable block device.
type Volume interface {
	Name() string
	Size() uint64
	ReadOnly() bool
	VolumeType() string
	Read(ctx context.Context, buf []byte, offset uint64) (int, error)
	// ReadAt returns n bytes starting at offset without copying into a
	// caller-provided buffer. The returned slice is pinned in the page
	// cache until release is called. Callers must not modify the slice.
	ReadAt(ctx context.Context, offset uint64, n int) (buf []byte, release func(), err error)
	Write(ctx context.Context, data []byte, offset uint64) error
	PunchHole(ctx context.Context, offset, length uint64) error
	ZeroRange(ctx context.Context, offset, length uint64) error
	Flush(ctx context.Context) error
	Snapshot(ctx context.Context, snapshotName string) error
	Clone(ctx context.Context, cloneName string) (Volume, error)
	CopyFrom(ctx context.Context, src Volume, srcOff, dstOff, length uint64) (uint64, error)
	Freeze(ctx context.Context) error
	Refresh(ctx context.Context) error
	AcquireRef() error
	ReleaseRef(ctx context.Context) error
}
