package loophole

import (
	"fmt"
	"strings"
	"unicode"
)

// ValidateVolumeName rejects names that cannot safely be embedded into object keys.
func ValidateVolumeName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("volume name must not be empty")
	case strings.Contains(name, ".."):
		return fmt.Errorf("invalid volume name %q: must not contain \"..\"", name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("invalid volume name %q: must not contain path separators", name)
	}
	for _, r := range name {
		if r == 0 {
			return fmt.Errorf("invalid volume name %q: must not contain NUL", name)
		}
		if unicode.IsControl(r) {
			return fmt.Errorf("invalid volume name %q: must not contain control characters", name)
		}
	}
	return nil
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
