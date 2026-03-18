package storage

import (
	"time"
)

// Volume types.
const (
	VolumeTypeExt4 = "ext4"
)

// CheckpointInfo describes a volume checkpoint.
type CheckpointInfo struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

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
	Name     string            `json:"name"`
	Size     uint64            `json:"size"`
	ReadOnly bool              `json:"read_only,omitempty"`
	Type     string            `json:"type,omitempty"`
	Parent   string            `json:"parent,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// DirectPage is a contiguous range of full logical pages to persist through
// the direct writeback path. Offset must be 4KB-aligned and len(Data) must
// be a positive multiple of 4KB (i.e. one or more complete pages).
type DirectPage struct {
	Offset uint64
	Data   []byte
}
