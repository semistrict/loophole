package storage

import (
	"fmt"
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
	Name   string            `json:"name"`
	Size   uint64            `json:"size"`
	Type   string            `json:"type,omitempty"`
	Parent string            `json:"parent,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// ValidateCheckpointID ensures checkpoint IDs remain a single safe path segment.
func ValidateCheckpointID(id string) error {
	if len(id) != 14 {
		return fmt.Errorf("invalid checkpoint id %q", id)
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return fmt.Errorf("invalid checkpoint id %q", id)
		}
	}
	return nil
}
