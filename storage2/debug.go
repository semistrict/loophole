package storage2

import "github.com/semistrict/loophole"

// DebugInfo returns storage2-specific debug info when the volume is backed by
// one of the concrete storage2 volume implementations.
func DebugInfo(vol loophole.Volume) (VolumeDebugInfo, bool) {
	switch v := vol.(type) {
	case *volume:
		return v.DebugInfo(), true
	case *frozenVolume:
		return v.DebugInfo(), true
	default:
		return VolumeDebugInfo{}, false
	}
}
