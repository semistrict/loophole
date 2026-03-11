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
	Volume   string            `json:"volume"`
	Size     uint64            `json:"size,omitempty,string"`
	NoFormat bool              `json:"no_format,omitempty"`
	Type     string            `json:"type,omitempty"`
	Parent   string            `json:"parent,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// VolumeInfo describes a volume's metadata.
type VolumeInfo struct {
	Name     string
	Size     uint64
	ReadOnly bool
	Type     string
	Parent   string
	Labels   map[string]string
}

// VolumeManager manages the lifecycle of volumes.
type VolumeManager interface {
	NewVolume(p CreateParams) (Volume, error)
	OpenVolume(name string) (Volume, error)
	GetVolume(name string) Volume
	Volumes() []string
	ListAllVolumes(ctx context.Context) ([]string, error)
	ListVolumesByType(ctx context.Context, volType string) ([]string, error)
	DeleteVolume(ctx context.Context, name string) error
	WaitClosed(ctx context.Context, name string) error
	VolumeInfo(ctx context.Context, name string) (VolumeInfo, error)
	UpdateLabels(ctx context.Context, name string, labels map[string]string) error
	// BreakLease attempts to release a volume's lease. If force is false,
	// only the polite RPC is tried; if the holder doesn't respond, an error
	// is returned. If force is true, the lease is cleared regardless.
	// Returns true if the holder responded gracefully.
	BreakLease(ctx context.Context, name string, force bool) (graceful bool, err error)
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
	Write(data []byte, offset uint64) error
	PunchHole(offset, length uint64) error
	ZeroRange(offset, length uint64) error
	Flush() error
	// FlushLocal freezes the memtable and notifies the background flush loop
	// without waiting for the S3 upload. Falls back to synchronous Flush if
	// no background loop is running.
	FlushLocal() error
	Snapshot(snapshotName string) error
	Clone(cloneName string) (Volume, error)
	CopyFrom(src Volume, srcOff, dstOff, length uint64) (uint64, error)
	Freeze() error
	Refresh(ctx context.Context) error
	AcquireRef() error
	ReleaseRef() error

	// OnBeforeFreeze registers a hook called before the volume is frozen.
	// Hooks fire in LIFO order. If any hook returns an error, freeze aborts.
	OnBeforeFreeze(fn func() error)

	// OnBeforeClose registers a hook called when the last ref is released.
	// Hooks fire in LIFO order. Errors are logged but don't prevent close.
	OnBeforeClose(fn func())
}
