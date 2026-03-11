//go:build !linux && !darwin

package mmap

// platformState is empty on unsupported platforms.
type platformState struct{}

func anonMmap(size int) ([]byte, error)                             { return nil, ErrNotSupported }
func munmap(data []byte) error                                      { return nil }
func (mr *MappedRegion) platformInit() error                        { return ErrNotSupported }
func (mr *MappedRegion) platformSignalStop()                        {}
func (mr *MappedRegion) platformClose() error                       { return nil }
func (mr *MappedRegion) platformPostInit() error                    { return nil }
func (mr *MappedRegion) platformSyncDirtyPages() error              { return nil }
func (mr *MappedRegion) platformPageSpan(addr uint64) (uint64, int) { return addr, pageSize }
func (mr *MappedRegion) platformProtectCleanPage(uint64) error      { return ErrNotSupported }
func (mr *MappedRegion) platformMarkDirty(uint64) error             { return ErrNotSupported }
func (mr *MappedRegion) platformPrepareFlush(uint64) error          { return ErrNotSupported }
func (mr *MappedRegion) platformEvictPage(uint64) error             { return ErrNotSupported }
func (mr *MappedRegion) handleFaults()                              {}
