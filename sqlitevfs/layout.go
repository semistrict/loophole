package sqlitevfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/semistrict/loophole"
)

const (
	superblockSize = 65536 // 64KB — one volume page
	magic          = "SQVFS001"
	version        = 1
	numRegions     = 4

	headerSize      = 64                                      // magic(8) + version(4) + numRegions(4) + reserved(48)
	regionEntrySize = 96                                      // name(64) + start(8) + capacity(8) + fileSize(8) + flags(8)
	regionTableOff  = headerSize                              // 64
	regionTableEnd  = headerSize + numRegions*regionEntrySize // 448

	flagExists = 1

	// Minimum volume size to format.
	MinVolumeSize = 256 * 1024 * 1024 // 256MB

	// Fixed region sizes.
	shmRegionSize = 1 * 1024 * 1024 // 1MB

	// Minimum sizes for journal and WAL.
	minJournalSize = 64 * 1024 * 1024 // 64MB
	minWALSize     = 64 * 1024 * 1024 // 64MB
)

// Region indices.
const (
	RegionMainDB  = 0
	RegionJournal = 1
	RegionWAL     = 2
	RegionSHM     = 3
)

// Region names as stored in the superblock.
var regionNames = [numRegions]string{
	"main.db",
	"main.db-journal",
	"main.db-wal",
	"main.db-shm",
}

// RegionEntry describes a file region within the volume.
type RegionEntry struct {
	Name           string
	RegionStart    uint64
	RegionCapacity uint64
	FileSize       uint64
	Flags          uint64
}

// Exists returns whether this region has been created (opened with CREATE).
func (r *RegionEntry) Exists() bool {
	return r.Flags&flagExists != 0
}

// Superblock is the on-disk metadata at the start of a volume.
type Superblock struct {
	Regions [numRegions]RegionEntry
}

// regionForName returns the region index for a SQLite filename, or -1.
func regionForName(name string) int {
	for i, n := range regionNames {
		if n == name {
			return i
		}
	}
	return -1
}

// DefaultRegions computes region boundaries for a given volume size.
// Layout (from the end): SHM(1MB), WAL(max(64MB, size/16)), Journal(max(64MB, size/16)), MainDB(rest).
func DefaultRegions(volumeSize uint64) [numRegions]RegionEntry {
	walSize := volumeSize / 16
	if walSize < minWALSize {
		walSize = minWALSize
	}
	journalSize := volumeSize / 16
	if journalSize < minJournalSize {
		journalSize = minJournalSize
	}

	// Work backwards from end.
	shmStart := volumeSize - shmRegionSize
	walStart := shmStart - walSize
	journalStart := walStart - journalSize
	mainDBStart := uint64(superblockSize)

	var regions [numRegions]RegionEntry
	regions[RegionMainDB] = RegionEntry{
		Name:           regionNames[RegionMainDB],
		RegionStart:    mainDBStart,
		RegionCapacity: journalStart - mainDBStart,
	}
	regions[RegionJournal] = RegionEntry{
		Name:           regionNames[RegionJournal],
		RegionStart:    journalStart,
		RegionCapacity: journalSize,
	}
	regions[RegionWAL] = RegionEntry{
		Name:           regionNames[RegionWAL],
		RegionStart:    walStart,
		RegionCapacity: walSize,
	}
	regions[RegionSHM] = RegionEntry{
		Name:           regionNames[RegionSHM],
		RegionStart:    shmStart,
		RegionCapacity: shmRegionSize,
	}
	return regions
}

// FormatVolume writes an initial superblock to a volume.
func FormatVolume(ctx context.Context, vol loophole.Volume) (*Superblock, error) {
	size := vol.Size()
	if size < MinVolumeSize {
		return nil, fmt.Errorf("volume too small: %d bytes (minimum %d)", size, MinVolumeSize)
	}
	sb := &Superblock{
		Regions: DefaultRegions(size),
	}
	if err := writeSuperblock(ctx, vol, sb); err != nil {
		return nil, err
	}
	return sb, nil
}

// ReadSuperblock reads and validates the superblock from a volume.
func ReadSuperblock(ctx context.Context, vol loophole.Volume) (*Superblock, error) {
	buf := make([]byte, superblockSize)
	if _, err := vol.Read(ctx, buf, 0); err != nil {
		return nil, fmt.Errorf("read superblock: %w", err)
	}
	return parseSuperblock(buf)
}

func parseSuperblock(buf []byte) (*Superblock, error) {
	if len(buf) < regionTableEnd {
		return nil, errors.New("superblock too short")
	}
	if string(buf[0:8]) != magic {
		return nil, errors.New("invalid superblock magic")
	}
	ver := binary.LittleEndian.Uint32(buf[8:12])
	if ver != version {
		return nil, fmt.Errorf("unsupported superblock version %d", ver)
	}
	nr := binary.LittleEndian.Uint32(buf[12:16])
	if nr != numRegions {
		return nil, fmt.Errorf("unexpected region count %d", nr)
	}

	sb := &Superblock{}
	for i := range numRegions {
		off := regionTableOff + i*regionEntrySize
		entry := &sb.Regions[i]
		// Name: 64 bytes null-terminated.
		nameBytes := buf[off : off+64]
		end := 0
		for end < 64 && nameBytes[end] != 0 {
			end++
		}
		entry.Name = string(nameBytes[:end])
		entry.RegionStart = binary.LittleEndian.Uint64(buf[off+64:])
		entry.RegionCapacity = binary.LittleEndian.Uint64(buf[off+72:])
		entry.FileSize = binary.LittleEndian.Uint64(buf[off+80:])
		entry.Flags = binary.LittleEndian.Uint64(buf[off+88:])
	}
	return sb, nil
}

// writeSuperblock serializes and writes the superblock to the volume.
func writeSuperblock(ctx context.Context, vol loophole.Volume, sb *Superblock) error {
	buf := make([]byte, superblockSize)
	copy(buf[0:8], magic)
	binary.LittleEndian.PutUint32(buf[8:12], version)
	binary.LittleEndian.PutUint32(buf[12:16], numRegions)
	// bytes 16..63 reserved (zeros)

	for i := range numRegions {
		off := regionTableOff + i*regionEntrySize
		entry := &sb.Regions[i]
		// Name: write up to 64 bytes, zero-padded.
		copy(buf[off:off+64], entry.Name)
		binary.LittleEndian.PutUint64(buf[off+64:], entry.RegionStart)
		binary.LittleEndian.PutUint64(buf[off+72:], entry.RegionCapacity)
		binary.LittleEndian.PutUint64(buf[off+80:], entry.FileSize)
		binary.LittleEndian.PutUint64(buf[off+88:], entry.Flags)
	}
	return vol.Write(ctx, buf, 0)
}
