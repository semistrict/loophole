package storage

import (
	"bytes"
	"fmt"
	"slices"
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
	EventRead // verified read — logged for determinism checks
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
	case EventRead:
		return "READ"
	default:
		return "?"
	}
}

// OracleEvent records a single mutation for post-mortem analysis.
type OracleEvent struct {
	Seq      int // global event counter
	Kind     OracleEventKind
	NodeID   string
	LayerID  string
	PageIdx  PageIdx   // only for Write/PunchHole
	Pages    []PageIdx // pages affected by a Flush
	ParentID string    // only for Branch
	DataHash uint32    // first 4 bytes of data, for quick visual matching
}

func (e OracleEvent) String() string {
	switch e.Kind {
	case EventWrite:
		return fmt.Sprintf("[%04d] WRITE    node=%-8s tl=%s page=%d hash=%08x", e.Seq, e.NodeID, e.LayerID[:8], e.PageIdx, e.DataHash)
	case EventPunchHole:
		return fmt.Sprintf("[%04d] PUNCH    node=%-8s tl=%s page=%d", e.Seq, e.NodeID, e.LayerID[:8], e.PageIdx)
	case EventFlush:
		return fmt.Sprintf("[%04d] FLUSH    node=%-8s tl=%s pages=%v", e.Seq, e.NodeID, e.LayerID[:8], e.Pages)
	case EventCrash:
		return fmt.Sprintf("[%04d] CRASH    node=%-8s", e.Seq, e.NodeID)
	case EventBranch:
		return fmt.Sprintf("[%04d] BRANCH   parent=%s -> child=%s", e.Seq, e.ParentID[:8], e.LayerID[:8])
	case EventRead:
		return fmt.Sprintf("[%04d] READ     node=%-8s tl=%s page=%d hash=%08x", e.Seq, e.NodeID, e.LayerID[:8], e.PageIdx, e.DataHash)
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

	// Per-timeline known-flushed pages: timelineID → pageIdx → data.
	flushed map[string]map[PageIdx][]byte

	// Per-node maybe-flushed pages: nodeID → timelineID → pageIdx → data.
	maybeFlushed map[string]map[string]map[PageIdx][]byte

	// Per-timeline ambiguous pages: timelineID → pageIdx → []data.
	// Each entry is a set of values that are all valid for this page.
	// Created when a crash makes maybe-flushed data's fate unknown.
	// Cleared for a page when an explicit flush writes a definitive value.
	ambiguous map[string]map[PageIdx][][]byte

	// Ancestor relationships.
	ancestors map[string]AncestorRef

	// Event log for post-mortem debugging.
	events  []OracleEvent
	nextSeq int
}

func NewOracle() *Oracle {
	return &Oracle{
		flushed:      make(map[string]map[PageIdx][]byte),
		maybeFlushed: make(map[string]map[string]map[PageIdx][]byte),
		ambiguous:    make(map[string]map[PageIdx][][]byte),
		ancestors:    make(map[string]AncestorRef),
	}
}

func (o *Oracle) logEvent(e OracleEvent) {
	e.Seq = o.nextSeq
	o.nextSeq++
	o.events = append(o.events, e)
}

// RecordPunchHole records a punch hole (pages revert to zeros) for a node's timeline.
func (o *Oracle) RecordPunchHole(nodeID, timelineID string, pageIdx PageIdx) {
	o.mu.Lock()
	o.logEvent(OracleEvent{Kind: EventPunchHole, NodeID: nodeID, LayerID: timelineID, PageIdx: pageIdx})
	o.mu.Unlock()
	o.RecordWrite(nodeID, timelineID, pageIdx, make([]byte, PageSize))
}

// RecordWrite records a maybe-flushed write for a node's timeline.
func (o *Oracle) RecordWrite(nodeID, timelineID string, pageIdx PageIdx, data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var hash uint32
	if len(data) >= 4 {
		hash = uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	}
	o.logEvent(OracleEvent{Kind: EventWrite, NodeID: nodeID, LayerID: timelineID, PageIdx: pageIdx, DataHash: hash})

	if o.maybeFlushed[nodeID] == nil {
		o.maybeFlushed[nodeID] = make(map[string]map[PageIdx][]byte)
	}
	if o.maybeFlushed[nodeID][timelineID] == nil {
		o.maybeFlushed[nodeID][timelineID] = make(map[PageIdx][]byte)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	o.maybeFlushed[nodeID][timelineID][pageIdx] = cp
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

	// Record which pages are being flushed (sorted for determinism).
	var flushedPages []PageIdx
	for addr := range pages {
		flushedPages = append(flushedPages, addr)
	}
	slices.Sort(flushedPages)
	o.logEvent(OracleEvent{Kind: EventFlush, NodeID: nodeID, LayerID: timelineID, Pages: flushedPages})

	if o.flushed[timelineID] == nil {
		o.flushed[timelineID] = make(map[PageIdx][]byte)
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
			o.ambiguous[timelineID] = make(map[PageIdx][][]byte)
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
	o.logEvent(OracleEvent{Kind: EventBranch, LayerID: childID, ParentID: parentID})
	o.ancestors[childID] = AncestorRef{ParentID: parentID, AncestorSeq: branchSeq}

	// The child starts with a copy of the parent's flushed state.
	if o.flushed[childID] == nil {
		o.flushed[childID] = make(map[PageIdx][]byte)
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
			o.ambiguous[childID] = make(map[PageIdx][][]byte)
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
func (o *Oracle) ExpectedRead(nodeID, timelineID string, pageIdx PageIdx) []byte {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check maybe-flushed (most recent writes on this node).
	if nodePages := o.maybeFlushed[nodeID]; nodePages != nil {
		if tlPages := nodePages[timelineID]; tlPages != nil {
			if data, ok := tlPages[pageIdx]; ok {
				cp := make([]byte, len(data))
				copy(cp, data)
				return cp
			}
		}
	}

	return o.flushedOrZero(timelineID, pageIdx)
}

// RecordRead logs a verified read into the event stream. This makes the
// determinism check stronger: if two runs with the same seed produce
// different read results, the event logs diverge.
func (o *Oracle) RecordRead(nodeID, timelineID string, pageIdx PageIdx, data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var hash uint32
	if len(data) >= 4 {
		hash = uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	}
	o.logEvent(OracleEvent{Kind: EventRead, NodeID: nodeID, LayerID: timelineID, PageIdx: pageIdx, DataHash: hash})
}

// VerifyRead checks that actual data matches expected for a live-node read.
func (o *Oracle) VerifyRead(nodeID, timelineID string, pageIdx PageIdx, actual []byte) bool {
	expected := o.ExpectedRead(nodeID, timelineID, pageIdx)
	return bytes.Equal(actual, expected)
}

// VerifyReadAfterCrash checks a read on a volume reopened after crashes.
// The actual value must be one of:
//   - the known-flushed value
//   - any ambiguous value (from maybe-flushed data whose fate is unknown)
//   - zeros (if no known-flushed data exists and no ambiguous data matched)
func (o *Oracle) VerifyReadAfterCrash(timelineID string, pageIdx PageIdx, actual []byte) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check known-flushed.
	expected := o.flushedOrZero(timelineID, pageIdx)
	if bytes.Equal(actual, expected) {
		return true
	}

	// Check ambiguous values.
	if amb := o.ambiguous[timelineID]; amb != nil {
		for _, candidate := range amb[pageIdx] {
			if bytes.Equal(actual, candidate) {
				return true
			}
		}
	}

	return false
}

// flushedOrZero returns the known-flushed value for a page, or zeros.
// Caller must hold o.mu.
func (o *Oracle) flushedOrZero(timelineID string, pageIdx PageIdx) []byte {
	if tlPages := o.flushed[timelineID]; tlPages != nil {
		if data, ok := tlPages[pageIdx]; ok {
			cp := make([]byte, len(data))
			copy(cp, data)
			return cp
		}
	}
	return make([]byte, PageSize)
}

// FlushedPages returns a copy of all known-flushed pages for a timeline.
func (o *Oracle) FlushedPages(timelineID string) map[PageIdx][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[PageIdx][]byte)
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
func (o *Oracle) PageHistory(timelineID string, pageIdx PageIdx) string {
	o.mu.Lock()
	defer o.mu.Unlock()

	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Oracle history for tl=%s page=%d ===\n", timelineID[:8], pageIdx)

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
			relevant = ancestors[e.LayerID] && e.PageIdx == pageIdx
		case EventFlush:
			if ancestors[e.LayerID] {
				for _, p := range e.Pages {
					if p == pageIdx {
						relevant = true
						break
					}
				}
			}
		case EventCrash:
			relevant = true
		case EventBranch:
			relevant = e.LayerID == timelineID || ancestors[e.LayerID]
		}
		if relevant {
			sb.WriteString("  ")
			sb.WriteString(e.String())
			sb.WriteString("\n")
		}
	}

	// Current oracle state.
	if tlPages := o.flushed[timelineID]; tlPages != nil {
		if data, ok := tlPages[pageIdx]; ok {
			fmt.Fprintf(&sb, "  FLUSHED: zero=%v hash=%08x\n", isZeroPage(data), data[0:4])
		} else {
			sb.WriteString("  FLUSHED: not present\n")
		}
	} else {
		sb.WriteString("  FLUSHED: no timeline entry\n")
	}

	if amb := o.ambiguous[timelineID]; amb != nil {
		if vals := amb[pageIdx]; len(vals) > 0 {
			fmt.Fprintf(&sb, "  AMBIGUOUS: %d possible values\n", len(vals))
		}
	}

	if ref, ok := o.ancestors[timelineID]; ok {
		fmt.Fprintf(&sb, "  ANCESTOR: parent=%s\n", ref.ParentID[:8])
	}

	return sb.String()
}

// TimelineHistory returns all events involving the given timeline and its
// ancestor chain. Used when a page mismatch needs broader timeline context.
func (o *Oracle) TimelineHistory(timelineID string) string {
	o.mu.Lock()
	defer o.mu.Unlock()

	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Oracle timeline history for tl=%s ===\n", timelineID[:8])

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
		case EventWrite, EventPunchHole, EventFlush, EventRead:
			relevant = ancestors[e.LayerID]
		case EventBranch:
			relevant = ancestors[e.LayerID] || ancestors[e.ParentID]
		case EventCrash:
			relevant = true
		}
		if relevant {
			sb.WriteString("  ")
			sb.WriteString(e.String())
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// EventLog returns all oracle events as a deterministic string sequence.
// Used to verify simulation determinism: two runs with the same seed must
// produce identical event logs.
func (o *Oracle) EventLog() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	log := make([]string, len(o.events))
	for i, e := range o.events {
		log[i] = e.String()
	}
	return log
}
