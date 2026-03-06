package lsm

import "errors"

var errMemLayerCleanedUp = errors.New("memlayer already cleaned up")

// ErrMemLayerFull is returned when all memlayer slots are exhausted.
// Callers should freeze the current memlayer and retry.
var ErrMemLayerFull = errors.New("memlayer full")

// memEntry is an index entry for a single page in the memLayer.
type memEntry struct {
	seq       uint64
	slot      int  // index into file slab
	tombstone bool // true = PunchHole, read as zeros
}

// sortedEntry is a memEntry with its pageAddr, used for serialization.
type sortedEntry struct {
	pageAddr uint64
	memEntry
}
