package storage

// DebugInfo returns storage-specific debug info when the volume is backed by
// the concrete storage volume implementation.
func DebugInfo(vol *Volume) (VolumeDebugInfo, bool) {
	if vol == nil {
		return VolumeDebugInfo{}, false
	}
	return vol.DebugInfo(), true
}
