package lsm

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
)

// AncestorRef records a timeline's parent relationship.
type AncestorRef struct {
	ParentID    string
	AncestorSeq uint64
}

// OracleEventKind identifies what happened.
type OracleEventKind int

const (
	EventWrite OracleEventKind = iota
	EventPunchHole
	EventFlush
	EventCrash
	EventBranch
)

func (k OracleEventKind) String() string {
	switch k {
	case EventWrite:
		return "WRITE"
	case EventPunchHole:
		return "PUNCH"
	case EventFlush:
		return "FLUSH"
	case EventCrash:
		return "CRASH"
	case EventBranch:
		return "BRANCH"
	default:
		return "?"
	}
}

// OracleEvent records a single mutation for post-mortem analysis.
type OracleEvent struct {
	Seq        int // global event counter
	Kind       OracleEventKind
	NodeID     string
	TimelineID string
	PageAddr   uint64   // only for Write/PunchHole
	Pages      []uint64 // pages affected by a Flush
	ParentID   string   // only for Branch
	DataHash   uint32   // first 4 bytes of data, for quick visual matching
}

func (e OracleEvent) String() string {
	switch e.Kind {
	case EventWrite:
		return fmt.Sprintf("[%04d] WRITE    node=%-8s tl=%s page=%d hash=%08x", e.Seq, e.NodeID, e.TimelineID[:8], e.PageAddr, e.DataHash)
	case EventPunchHole:
		return fmt.Sprintf("[%04d] PUNCH    node=%-8s tl=%s page=%d", e.Seq, e.NodeID, e.TimelineID[:8], e.PageAddr)
	case EventFlush:
		return fmt.Sprintf("[%04d] FLUSH    node=%-8s tl=%s pages=%v", e.Seq, e.NodeID, e.TimelineID[:8], e.Pages)
	case EventCrash:
		return fmt.Sprintf("[%04d] CRASH    node=%-8s", e.Seq, e.NodeID)
	case EventBranch:
		return fmt.Sprintf("[%04d] BRANCH   parent=%s -> child=%s", e.Seq, e.ParentID[:8], e.TimelineID[:8])
	default:
		return fmt.Sprintf("[%04d] ?", e.Seq)
	}
}

// Oracle is a reference model that tracks expected state for verification.
//
// Written data has two states:
//   - "known flushed": an explicit Flush/Fsync succeeded — data is definitely
//     in S3 and must survive crashes.
//   - "maybe flushed": written but not explicitly flushed — visible to reads on
//     the writing node, but may or may not survive a crash.
//
// On crash, maybe-flushed pages become ambiguous: the page could contain either
// the written value (auto-flush saved it) or the last known-flushed value (it
// wasn't persisted). Both outcomes are valid.
type Oracle struct {
	mu sync.Mutex

	// Per-timeline known-flushed pages: timelineID → pageAddr → data.
	flushed map[string]map[uint64][]byte

	// Per-node maybe-flushed pages: nodeID → timelineID → pageAddr → data.
	maybeFlushed map[string]map[string]map[uint64][]byte

	// Per-timeline ambiguous pages: timelineID → pageAddr → []data.
	// Each entry is a set of values that are all valid for this page.
	// Created when a crash makes maybe-flushed data's fate unknown.
	// Cleared for a page when an explicit flush writes a definitive value.
	ambiguous map[string]map[uint64][][]byte

	// Ancestor relationships.
	ancestors map[string]AncestorRef

	// Event log for post-mortem debugging.
	events  []OracleEvent
	nextSeq int
}

func NewOracle() *Oracle {
	return &Oracle{
		flushed:      make(map[string]map[uint64][]byte),
		maybeFlushed: make(map[string]map[string]map[uint64][]byte),
		ambiguous:    make(map[string]map[uint64][][]byte),
		ancestors:    make(map[string]AncestorRef),
	}
}

func (o *Oracle) logEvent(e OracleEvent) {
	e.Seq = o.nextSeq
	o.nextSeq++
	o.events = append(o.events, e)
}

// RecordPunchHole records a punch hole (pages revert to zeros) for a node's timeline.
func (o *Oracle) RecordPunchHole(nodeID, timelineID string, pageAddr uint64) {
	o.mu.Lock()
	o.logEvent(OracleEvent{Kind: EventPunchHole, NodeID: nodeID, TimelineID: timelineID, PageAddr: pageAddr})
	o.mu.Unlock()
	o.RecordWrite(nodeID, timelineID, pageAddr, make([]byte, PageSize))
}

// RecordWrite records a maybe-flushed write for a node's timeline.
func (o *Oracle) RecordWrite(nodeID, timelineID string, pageAddr uint64, data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var hash uint32
	if len(data) >= 4 {
		hash = uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	}
	o.logEvent(OracleEvent{Kind: EventWrite, NodeID: nodeID, TimelineID: timelineID, PageAddr: pageAddr, DataHash: hash})

	if o.maybeFlushed[nodeID] == nil {
		o.maybeFlushed[nodeID] = make(map[string]map[uint64][]byte)
	}
	if o.maybeFlushed[nodeID][timelineID] == nil {
		o.maybeFlushed[nodeID][timelineID] = make(map[uint64][]byte)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	o.maybeFlushed[nodeID][timelineID][pageAddr] = cp
}

// RecordFlush promotes a node's maybe-flushed writes for a timeline to known-flushed.
// Also clears any ambiguity for those pages since we now know the definitive value.
func (o *Oracle) RecordFlush(nodeID, timelineID string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	nodePages := o.maybeFlushed[nodeID]
	if nodePages == nil {
		return
	}
	pages := nodePages[timelineID]
	if pages == nil {
		return
	}

	// Record which pages are being flushed.
	var flushedPages []uint64
	for addr := range pages {
		flushedPages = append(flushedPages, addr)
	}
	o.logEvent(OracleEvent{Kind: EventFlush, NodeID: nodeID, TimelineID: timelineID, Pages: flushedPages})

	if o.flushed[timelineID] == nil {
		o.flushed[timelineID] = make(map[uint64][]byte)
	}
	for addr, data := range pages {
		o.flushed[timelineID][addr] = data
		// This page now has a definitive value — clear any ambiguity.
		if amb := o.ambiguous[timelineID]; amb != nil {
			delete(amb, addr)
		}
	}
	delete(nodePages, timelineID)
}

// RecordCrash handles a node crash. Maybe-flushed pages become ambiguous:
// they could contain the written value (auto-flush saved it) or the last
// known-flushed value.
func (o *Oracle) RecordCrash(nodeID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logEvent(OracleEvent{Kind: EventCrash, NodeID: nodeID})

	// Move maybe-flushed pages to the ambiguous set.
	for timelineID, pages := range o.maybeFlushed[nodeID] {
		if o.ambiguous[timelineID] == nil {
			o.ambiguous[timelineID] = make(map[uint64][][]byte)
		}
		for addr, data := range pages {
			// The valid values are: the maybe-flushed data, plus whatever
			// was already valid (known-flushed value or previous ambiguous values).
			o.ambiguous[timelineID][addr] = append(o.ambiguous[timelineID][addr], data)
		}
	}
	delete(o.maybeFlushed, nodeID)
}

// RecordBranch records an ancestor relationship (snapshot or clone).
func (o *Oracle) RecordBranch(childID, parentID string, branchSeq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logEvent(OracleEvent{Kind: EventBranch, TimelineID: childID, ParentID: parentID})
	o.ancestors[childID] = AncestorRef{ParentID: parentID, AncestorSeq: branchSeq}

	// The child starts with a copy of the parent's flushed state.
	if o.flushed[childID] == nil {
		o.flushed[childID] = make(map[uint64][]byte)
	}
	if parentPages := o.flushed[parentID]; parentPages != nil {
		for addr, data := range parentPages {
			if _, exists := o.flushed[childID][addr]; !exists {
				cp := make([]byte, len(data))
				copy(cp, data)
				o.flushed[childID][addr] = cp
			}
		}
	}

	// The child also inherits the parent's ambiguous pages — auto-flushed
	// data that may or may not have been persisted before a crash is equally
	// ambiguous for the child.
	if parentAmb := o.ambiguous[parentID]; parentAmb != nil {
		if o.ambiguous[childID] == nil {
			o.ambiguous[childID] = make(map[uint64][][]byte)
		}
		for addr, vals := range parentAmb {
			for _, v := range vals {
				cp := make([]byte, len(v))
				copy(cp, v)
				o.ambiguous[childID][addr] = append(o.ambiguous[childID][addr], cp)
			}
		}
	}
}

// ExpectedRead returns the expected data for a page read on a live node.
// Checks maybe-flushed writes first, then known-flushed.
// Always returns a fresh copy — safe to mutate.
func (o *Oracle) ExpectedRead(nodeID, timelineID string, pageAddr uint64) []byte {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check maybe-flushed (most recent writes on this node).
	if nodePages := o.maybeFlushed[nodeID]; nodePages != nil {
		if tlPages := nodePages[timelineID]; tlPages != nil {
			if data, ok := tlPages[pageAddr]; ok {
				cp := make([]byte, len(data))
				copy(cp, data)
				return cp
			}
		}
	}

	return o.flushedOrZero(timelineID, pageAddr)
}

// VerifyRead checks that actual data matches expected for a live-node read.
func (o *Oracle) VerifyRead(nodeID, timelineID string, pageAddr uint64, actual []byte) bool {
	expected := o.ExpectedRead(nodeID, timelineID, pageAddr)
	return bytes.Equal(actual, expected)
}

// VerifyReadAfterCrash checks a read on a volume reopened after crashes.
// The actual value must be one of:
//   - the known-flushed value
//   - any ambiguous value (from maybe-flushed data whose fate is unknown)
//   - zeros (if no known-flushed data exists and no ambiguous data matched)
func (o *Oracle) VerifyReadAfterCrash(timelineID string, pageAddr uint64, actual []byte) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check known-flushed.
	expected := o.flushedOrZero(timelineID, pageAddr)
	if bytes.Equal(actual, expected) {
		return true
	}

	// Check ambiguous values.
	if amb := o.ambiguous[timelineID]; amb != nil {
		for _, candidate := range amb[pageAddr] {
			if bytes.Equal(actual, candidate) {
				return true
			}
		}
	}

	return false
}

// flushedOrZero returns the known-flushed value for a page, or zeros.
// Caller must hold o.mu.
func (o *Oracle) flushedOrZero(timelineID string, pageAddr uint64) []byte {
	if tlPages := o.flushed[timelineID]; tlPages != nil {
		if data, ok := tlPages[pageAddr]; ok {
			cp := make([]byte, len(data))
			copy(cp, data)
			return cp
		}
	}
	return make([]byte, PageSize)
}

// FlushedPages returns a copy of all known-flushed pages for a timeline.
func (o *Oracle) FlushedPages(timelineID string) map[uint64][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[uint64][]byte)
	if tlPages := o.flushed[timelineID]; tlPages != nil {
		for addr, data := range tlPages {
			cp := make([]byte, len(data))
			copy(cp, data)
			result[addr] = cp
		}
	}
	return result
}

func isZeroPage(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// PageHistory returns all events that affected the given page on the given timeline
// (including branches that created this timeline). This is the key debugging tool:
// on mismatch, call this to see the full lifecycle of the page.
func (o *Oracle) PageHistory(timelineID string, pageAddr uint64) string {
	o.mu.Lock()
	defer o.mu.Unlock()

	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Oracle history for tl=%s page=%d ===\n", timelineID[:8], pageAddr)

	// Collect all timeline IDs in the ancestor chain.
	ancestors := map[string]bool{timelineID: true}
	cur := timelineID
	for {
		ref, ok := o.ancestors[cur]
		if !ok {
			break
		}
		ancestors[ref.ParentID] = true
		cur = ref.ParentID
	}

	for _, e := range o.events {
		relevant := false
		switch e.Kind {
		case EventWrite, EventPunchHole:
			relevant = ancestors[e.TimelineID] && e.PageAddr == pageAddr
		case EventFlush:
			if ancestors[e.TimelineID] {
				for _, p := range e.Pages {
					if p == pageAddr {
						relevant = true
						break
					}
				}
			}
		case EventCrash:
			relevant = true
		case EventBranch:
			relevant = e.TimelineID == timelineID || ancestors[e.TimelineID]
		}
		if relevant {
			sb.WriteString("  ")
			sb.WriteString(e.String())
			sb.WriteString("\n")
		}
	}

	// Current oracle state.
	if tlPages := o.flushed[timelineID]; tlPages != nil {
		if data, ok := tlPages[pageAddr]; ok {
			fmt.Fprintf(&sb, "  FLUSHED: zero=%v hash=%08x\n", isZeroPage(data), data[0:4])
		} else {
			sb.WriteString("  FLUSHED: not present\n")
		}
	} else {
		sb.WriteString("  FLUSHED: no timeline entry\n")
	}

	if amb := o.ambiguous[timelineID]; amb != nil {
		if vals := amb[pageAddr]; len(vals) > 0 {
			fmt.Fprintf(&sb, "  AMBIGUOUS: %d possible values\n", len(vals))
		}
	}

	if ref, ok := o.ancestors[timelineID]; ok {
		fmt.Fprintf(&sb, "  ANCESTOR: parent=%s\n", ref.ParentID[:8])
	}

	return sb.String()
}
