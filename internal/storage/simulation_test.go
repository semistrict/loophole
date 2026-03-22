package storage

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/stretchr/testify/require"
)

// SimConfig controls the simulation parameters.
type SimConfig struct {
	NumNodes       int
	DevicePages    int // number of 4KB pages per volume
	MaxLineages    int
	OpsPerNode     int
	CrashRate      float64 // probability of crash per tick
	S3Faults       FaultConfig
	FlushThreshold int64
	FlushInterval  time.Duration // periodic auto-flush interval; 0 disables it in the main simulation
}

// Simulation runs a multi-node deterministic simulation.
type Simulation struct {
	t      *testing.T
	rng    *mrand.Rand
	store  *SimObjectStore
	nodes  []*SimNode
	oracle *Oracle
	config SimConfig

	// All timeline IDs created during the simulation.
	timelineIDs []string
	// Volume name → timeline ID mapping (mirrors what's in S3).
	volumeTimelines map[string]string
	// Volume name → owning node ID (simple lease model).
	leases map[string]string
	// Monotonic volume name counter (never reuses names after deletion).
	nextVolID int
	// Monotonic timeline ID counter for deterministic layer ID generation.
	nextTLID int
	// Deleted volume names (removed from volumeTimelines).
	deletedVolumes map[string]bool
	// Parent timeline ID → set of child timeline IDs.
	timelineChildren map[string]map[string]bool
	// Last cloned volume name for deep clone chains.
	lastClonedVolume string
	// All managers ever created (for cleanup, including crashed ones).
	allManagers []*Manager
	// Shared in-memory page cache for all managers.
	pageCache *memPageCache
}

// SimNode represents a single simulated instance.
// Each open volume gets its own Manager (one volume per manager).
type SimNode struct {
	id       string
	fs       *SimLocalFS
	managers map[string]*Manager // volume name → manager
	volumes  []string            // volume names this node has open
	alive    bool
}

func NewSimulation(t *testing.T, seed uint64, config SimConfig) *Simulation {
	rng := mrand.New(mrand.NewPCG(seed, 0))

	store := NewSimObjectStore(rng, config.S3Faults)
	_, _, err := FormatVolumeSet(t.Context(), store.Store())
	require.NoError(t, err)

	sim := &Simulation{
		t:                t,
		rng:              rng,
		store:            store,
		oracle:           NewOracle(),
		config:           config,
		volumeTimelines:  make(map[string]string),
		leases:           make(map[string]string),
		deletedVolumes:   make(map[string]bool),
		timelineChildren: make(map[string]map[string]bool),
		nextTLID:         1,
		pageCache:        newMemPageCache(),
	}

	// Create nodes.
	for i := range config.NumNodes {
		node := &SimNode{
			id:       fmt.Sprintf("node-%d", i),
			fs:       NewSimLocalFS(),
			managers: make(map[string]*Manager),
			alive:    true,
		}
		sim.nodes = append(sim.nodes, node)
	}

	return sim
}

func (sim *Simulation) checkpointAndClone(ctx context.Context, v *Volume, cloneName string) error {
	cpID, err := v.Checkpoint()
	if err != nil {
		return fmt.Errorf("checkpoint before clone: %w", err)
	}
	return Clone(ctx, sim.store.Store(), v.Name(), cpID, cloneName)
}

func (sim *Simulation) nextLayerID() string {
	id := fmt.Sprintf("%08x-simtl-%08x", sim.nextTLID, sim.nextTLID)
	sim.nextTLID++
	return id
}

func (sim *Simulation) newManager(cacheDir string, fs localFS) *Manager {
	flushInterval := sim.config.FlushInterval
	if flushInterval == 0 {
		// The simulation already drives explicit flushes heavily. Leave
		// periodic auto-flush disabled by default so long high-fanout runs
		// don't spend most of their time in background flush contention.
		flushInterval = -1
	}
	m := &Manager{
		BlobStore: sim.store.Store(),
		config: Config{
			FlushThreshold: sim.config.FlushThreshold,
			FlushInterval:  flushInterval,
		},
		fs:        fs,
		diskCache: sim.pageCache,
	}
	return m
}

func (sim *Simulation) useDeterministicLayerIDs() func() {
	prev := newLayerID
	newLayerID = sim.nextLayerID
	return func() {
		newLayerID = prev
	}
}

// managerFor returns the manager for a volume on a node, creating one if needed.
func (sim *Simulation) managerFor(node *SimNode, volName string) *Manager {
	if m, ok := node.managers[volName]; ok {
		return m
	}
	m := sim.newManager(sim.t.TempDir(), node.fs)
	node.managers[volName] = m
	sim.allManagers = append(sim.allManagers, m)
	return m
}

// getVolume returns the *Volume for a name on a node, or nil.
func (sim *Simulation) getVolume(node *SimNode, name string) *Volume {
	m, ok := node.managers[name]
	if !ok {
		return nil
	}
	return m.GetVolume(name)
}

func (sim *Simulation) fillRandomBytes(dst []byte) {
	for i := range dst {
		dst[i] = byte(sim.rng.IntN(256))
	}
}

func (sim *Simulation) canAddLineages(n int) bool {
	if sim.config.MaxLineages <= 0 {
		return true
	}
	return len(sim.timelineIDs)+n <= sim.config.MaxLineages
}

// Run executes the simulation for OpsPerNode ticks per node.
func (sim *Simulation) Run() {
	ctx := sim.t.Context()

	// Create an initial volume on node 0.
	sim.createVolume(ctx, sim.nodes[0], "vol-0")
	sim.nextVolID = 1

	for tick := range sim.config.OpsPerNode {
		for _, node := range sim.nodes {
			if !node.alive {
				// Try to recover with some probability.
				if sim.rng.Float64() < 0.1 {
					sim.recoverNode(ctx, node)
				}
				continue
			}

			sim.store.InjectFaults()
			sim.doOperation(ctx, node, tick)
			sim.store.ClearFaults()
		}

		// Advance the fake clock to let periodic flush goroutines fire.
		// Randomize the sleep duration (0-5ms) so different ticks exercise
		// different race windows between auto-flush and explicit operations.
		time.Sleep(time.Duration(sim.rng.IntN(6)) * time.Millisecond)

		if debugCountersEnabled() && tick%50 == 0 {
			sim.t.Logf("tick %d: %d volumes, %d timelines, goroutines=%d", tick, len(sim.volumeTimelines), len(sim.timelineIDs), runtime.NumGoroutine())
		}
	}

	// Flush all live volumes so the full scan sees consistent state.
	sim.store.ClearFaults()
	for _, node := range sim.nodes {
		if !node.alive {
			continue
		}
		for _, volName := range node.volumes {
			v := sim.getVolume(node, volName)
			if v == nil {
				continue
			}
			if err := v.Flush(); err == nil {
				timelineID := sim.volumeTimelines[volName]
				sim.oracle.RecordFlush(node.id, timelineID)
			}
		}
	}
}

// opEntry pairs a weight with an operation function. doOperation picks
// an operation by sampling from the cumulative weight distribution, so
// adding a new op only requires appending to the table — no manual
// threshold arithmetic.
type opEntry struct {
	weight float64
	fn     func(ctx context.Context, node *SimNode)
}

func (sim *Simulation) opTable() []opEntry {
	return []opEntry{
		{30, sim.opWrite},
		{7, sim.opPartialWrite},
		{5, sim.opPunchHole},
		{18, sim.opRead},
		{8, sim.opFlush},
		{8, sim.opSnapshot},
		{5, sim.opClone},
		{3, sim.opCloneNoFlush},
		{5, sim.opFlushAndVerify},
		{3, sim.opCreateVolume},
		{2, sim.opCopyFrom},
		{2, sim.opDeepClone},
		{2, sim.opCloneFromSnapshot},
		{5, sim.opCheckpoint},
		{2, sim.opCloneFromCheckpoint},
		{2, sim.opVerifySharedVolume},
		{3, sim.opVerify},
		{2, sim.opSnapshotIsolation},
		{2, sim.opOpen},
		{2, sim.opDeleteVolume},
		{2, sim.opCloseVolume},
		{3, sim.opConcurrentWrite},
		{2, sim.opWriteAllPages},
		{sim.config.CrashRate * 100, sim.opCrash},
		// Remaining weight is implicitly a no-op.
	}
}

func (sim *Simulation) doOperation(ctx context.Context, node *SimNode, tick int) {
	table := sim.opTable()

	r := sim.rng.Float64() * 100 // weights are expressed per-100
	var cum float64
	for _, e := range table {
		cum += e.weight
		if r < cum {
			e.fn(ctx, node)
			return
		}
	}
	// No-op (rest period).
	_ = tick
}

func (sim *Simulation) opWrite(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]

	// Write 1-10 random pages.
	numPages := sim.rng.IntN(10) + 1
	maxPage := PageIdx(sim.config.DevicePages)

	wrote := false
	for range numPages {
		pageIdx := PageIdx(sim.rng.Uint64N(uint64(maxPage)))
		data := make([]byte, PageSize)
		sim.fillRandomBytes(data)

		err := v.Write(data, pageIdx.ByteOffset())

		if err == nil {
			if traceLayerEnabled(timelineID) && tracePageEnabled(pageIdx) {
				sim.t.Logf("TRACE opWrite success tl=%s page=%d", timelineID, pageIdx)
			}
			sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)
			wrote = true
			continue
		}

		// Full-page writes can fail before the retry put runs under
		// backpressure, so only record the write if we can confirm the page
		// is now visible on the live volume.
		verify := make([]byte, PageSize)
		readN, readErr := v.Read(ctx, verify, pageIdx.ByteOffset())
		if traceLayerEnabled(timelineID) && tracePageEnabled(pageIdx) {
			sim.t.Logf("TRACE opWrite err tl=%s page=%d err=%v readErr=%v readN=%d verifyEq=%v gotZero=%v",
				timelineID, pageIdx, err, readErr, readN, readErr == nil && bytes.Equal(verify, data), bytes.Equal(verify, zeroPage[:]))
		}
		if readErr == nil && bytes.Equal(verify, data) {
			sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)
			wrote = true
		}

		break
	}

	if !wrote {
		return
	}

	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

func (sim *Simulation) opPartialWrite(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)
	pageIdx := PageIdx(sim.rng.Uint64N(uint64(maxPage)))

	// Pick a random sub-page offset and length (1-2048 bytes).
	writeLen := sim.rng.IntN(2048) + 1
	maxOff := PageSize - writeLen
	writeOff := sim.rng.IntN(maxOff + 1)

	data := make([]byte, writeLen)
	sim.fillRandomBytes(data)

	// Read the actual page from the volume to use as the base for the
	// oracle's expected value. This ensures the oracle agrees with the
	// engine's read-modify-write result even if prior untracked writes
	// (from faulted operations) modified the page.
	basePage := make([]byte, PageSize)
	if _, err := v.Read(ctx, basePage, pageIdx.ByteOffset()); err != nil {
		// Can't read the base page (S3 fault) — skip this operation.
		// We haven't written anything, so no divergence.
		return
	}
	expected := make([]byte, PageSize)
	copy(expected, basePage)
	copy(expected[writeOff:writeOff+writeLen], data)

	offset := pageIdx.ByteOffset() + uint64(writeOff)
	err := v.Write(data, offset)

	if err == nil {
		sim.oracle.RecordWrite(node.id, timelineID, pageIdx, expected)
	} else {
		// Partial writes can also fail before the merged page is staged under
		// backpressure, so only record the write if the live page now matches
		// the expected read-modify-write result.
		verify := make([]byte, PageSize)
		if _, readErr := v.Read(ctx, verify, pageIdx.ByteOffset()); readErr == nil && bytes.Equal(verify, expected) {
			sim.oracle.RecordWrite(node.id, timelineID, pageIdx, expected)
		} else {
			return
		}
	}
	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

// reportMismatch dumps both oracle history and timeline layer state for a
// mismatched page, then fatals. This is the single diagnostic entry point
// for all mismatch types (inline reads and full scan).
func (sim *Simulation) reportMismatch(ctx context.Context, vol *Volume, volName, nodeID, timelineID string, pageIdx PageIdx, got, expected []byte) {
	isZero := func(b []byte) bool {
		for _, v := range b {
			if v != 0 {
				return false
			}
		}
		return true
	}

	sim.t.Logf("=== MISMATCH: vol=%s tl=%s page=%d gotZero=%v expZero=%v ===",
		volName, timelineID, pageIdx, isZero(got), isZero(expected))
	sim.t.Logf("got:      %x...", got[:32])
	sim.t.Logf("expected: %x...", expected[:32])

	// Find the first byte offset where got and expected differ.
	firstDiff := -1
	for i := range got {
		if i < len(expected) && got[i] != expected[i] {
			firstDiff = i
			break
		}
	}
	if firstDiff >= 0 {
		end := firstDiff + 32
		if end > len(got) {
			end = len(got)
		}
		sim.t.Logf("first diff at byte %d: got[%d:]=%x... exp[%d:]=%x...",
			firstDiff, firstDiff, got[firstDiff:end], firstDiff, expected[firstDiff:end])
	}

	// Oracle history: full lifecycle of this page.
	sim.t.Log(sim.oracle.PageHistory(timelineID, pageIdx))
	sim.t.Log(sim.oracle.TimelineHistory(timelineID))

	// Count ALL oracle events for this timeline (not just this page).
	var writeCount, flushCount, branchCount int
	sim.oracle.mu.Lock()
	for _, e := range sim.oracle.events {
		switch e.Kind {
		case EventWrite:
			if e.LayerID == timelineID {
				writeCount++
			}
		case EventFlush:
			if e.LayerID == timelineID {
				flushCount++
			}
		case EventBranch:
			if e.LayerID == timelineID || e.ParentID == timelineID {
				branchCount++
			}
		}
	}
	flushedPageCount := len(sim.oracle.flushed[timelineID])
	ambigPageCount := len(sim.oracle.ambiguous[timelineID])
	sim.oracle.mu.Unlock()
	sim.t.Logf("oracle summary for tl=%s: writes=%d flushes=%d branches=%d flushedPages=%d ambigPages=%d",
		timelineID[:8], writeCount, flushCount, branchCount, flushedPageCount, ambigPageCount)

	// Timeline layer inspector: what each layer sees.
	{
		v := vol
		sim.t.Log(v.layer.DebugPage(ctx, pageIdx))
	}

	sim.t.FailNow()
}

func (sim *Simulation) opRead(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	maxPage := PageIdx(sim.config.DevicePages)
	pageIdx := PageIdx(sim.rng.Uint64N(uint64(maxPage)))

	buf := make([]byte, PageSize)
	_, err := v.Read(ctx, buf, pageIdx.ByteOffset())
	if err != nil {
		// S3 faults can cause read failures. Expected.
		return
	}

	timelineID := sim.volumeTimelines[volName]
	sim.oracle.RecordRead(node.id, timelineID, pageIdx, buf)
	if !sim.oracle.VerifyRead(node.id, timelineID, pageIdx, buf) {
		expected := sim.oracle.ExpectedRead(node.id, timelineID, pageIdx)
		sim.reportMismatch(ctx, v, volName, node.id, timelineID, pageIdx, buf, expected)
	}
}

func (sim *Simulation) opFlush(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	err := v.Flush()
	if err != nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, timelineID)
}

func (sim *Simulation) opSnapshot(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	// Flush parent before snapshotting so unflushed writes are visible
	// to the child. Snapshot freezes the memLayer but doesn't flush to S3.
	if err := v.Flush(); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	// Capture branchSeq before snapshot (nextSeq at the time of branching).
	var branchSeq uint64
	{
		pv := v
		branchSeq = pv.layer.nextSeq.Load()
	}

	snapName := fmt.Sprintf("%s-snap-%d", volName, sim.nextVolID)
	sim.nextVolID++

	err := checkpointAndClone(sim.t, v, snapName)
	if err != nil {
		return
	}

	// Mark the clone as frozen directly in the store. This avoids the
	// expensive open+freeze cycle (which acquires a lease, starts periodic
	// flush goroutines, etc.) and matches what the old Snapshot() did: just
	// write a read-only ref.
	ref, err := sim.getVolRef(ctx, snapName)
	if err != nil {
		return
	}

	// Set frozen_at on the layer's index.json metadata so openVolume
	// recognizes it as a frozen layer (avoids writable open path).
	layerStore := sim.store.Store().At("layers/" + ref.LayerID)
	if err := layerStore.SetMeta(ctx, "index.json", map[string]string{
		"frozen_at": time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return
	}

	sim.volumeTimelines[snapName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, branchSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true

	// Re-layering: parent now has a new layer ID.
	sim.updateParentAfterRelayer(ctx, volName, parentTL, branchSeq)
}

func (sim *Simulation) opClone(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	cloneName := fmt.Sprintf("%s-clone-%d", volName, sim.nextVolID)
	sim.nextVolID++

	// Flush parent before cloning so unflushed writes are visible to the child.
	// Clone freezes the memLayer but doesn't flush to S3, so the child
	// (loaded from S3) won't see unflushed data unless we flush first.
	if err := v.Flush(); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	// Log parent's nextSeq before clone for diagnostics.
	var parentNextSeq uint64
	{
		pv := v
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := sim.checkpointAndClone(ctx, v, cloneName); err != nil {
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Read the clone's volume ref.
	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	// Log the clone's ancestorSeq for diagnostics.
	{
		cv := clone
		sim.t.Logf("[%s] CLONE parent=%s (nextSeq=%d) -> child=%s (ancestorSeq=%d)",
			node.id, parentTL[:8], parentNextSeq, ref.LayerID[:8], cv.layer.nextSeq.Load())
	}

	sim.volumeTimelines[cloneName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, parentNextSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true

	sim.lastClonedVolume = cloneName

	// Re-layering: parent now has a new layer ID.
	sim.updateParentAfterRelayer(ctx, volName, parentTL, parentNextSeq)

	_ = clone
}

func (sim *Simulation) opCloneNoFlush(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	cloneName := fmt.Sprintf("%s-clone-%d", volName, sim.nextVolID)
	sim.nextVolID++

	parentTL := sim.volumeTimelines[volName]

	// Capture branchSeq before clone.
	var parentNextSeq uint64
	{
		pv := v
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	// Checkpoint flushes internally, so unflushed data is persisted.
	if err := sim.checkpointAndClone(ctx, v, cloneName); err != nil {
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Checkpoint internally flushed parent data — record it in oracle.
	sim.oracle.RecordFlush(node.id, parentTL)

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	{
		cv := clone
		sim.t.Logf("[%s] CLONE-NOFLUSH parent=%s (nextSeq=%d) -> child=%s (ancestorSeq=%d)",
			node.id, parentTL[:8], parentNextSeq, ref.LayerID[:8], cv.layer.nextSeq.Load())
	}

	sim.volumeTimelines[cloneName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, parentNextSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true

	sim.lastClonedVolume = cloneName

	// Re-layering: parent now has a new layer ID.
	sim.updateParentAfterRelayer(ctx, volName, parentTL, parentNextSeq)

	_ = clone
}

func (sim *Simulation) opPunchHole(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)

	// Punch 1-5 contiguous pages.
	startPage := PageIdx(sim.rng.Uint64N(uint64(maxPage)))
	numPages := PageIdx(sim.rng.IntN(5) + 1)
	if startPage+numPages > maxPage {
		numPages = maxPage - startPage
	}

	offset := startPage.ByteOffset()
	length := uint64(numPages) * PageSize
	if err := v.PunchHole(offset, length); err != nil {
		// PunchHole can fail after staging some tombstones under backpressure.
		// Reconcile against the live volume so the oracle tracks any pages that
		// are already observably zero on this node.
		reconciled := false
		verify := make([]byte, PageSize)
		for pg := startPage; pg < startPage+numPages; pg++ {
			if _, readErr := v.Read(ctx, verify, pg.ByteOffset()); readErr != nil {
				continue
			}
			if bytes.Equal(verify, zeroPage[:]) {
				sim.oracle.RecordPunchHole(node.id, timelineID, pg)
				reconciled = true
			}
		}
		if reconciled {
			return
		}
		return
	}

	// Record in oracle, then flush.
	for pg := startPage; pg < startPage+numPages; pg++ {
		sim.oracle.RecordPunchHole(node.id, timelineID, pg)
	}
	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

func (sim *Simulation) opFlushAndVerify(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	// Flush first so oracle is consistent.
	if err := v.Flush(); err != nil {
		return
	}
	timelineID := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, timelineID)

	// Post-flush verification: read all pages and check against oracle.
	maxPage := PageIdx(sim.config.DevicePages)
	for pageIdx := PageIdx(0); pageIdx < maxPage; pageIdx++ {
		buf := make([]byte, PageSize)
		_, err := v.Read(ctx, buf, pageIdx.ByteOffset())
		if err != nil {
			// S3 faults can cause read failures during verification.
			return
		}
		if !sim.oracle.VerifyRead(node.id, timelineID, pageIdx, buf) {
			expected := sim.oracle.ExpectedRead(node.id, timelineID, pageIdx)
			sim.t.Logf("POST-FLUSH MISMATCH vol=%s", volName)
			sim.reportMismatch(ctx, v, volName, node.id, timelineID, pageIdx, buf, expected)
		}
	}
}

func (sim *Simulation) pickReadOnlyVolume(ctx context.Context) string {
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for name := range sim.volumeTimelines {
		volNames = append(volNames, name)
	}
	sort.Strings(volNames)
	for _, name := range volNames {
		// Check if the volume's layer is frozen via its metadata.
		ref, err := sim.getVolRef(ctx, name)
		if err != nil {
			continue
		}
		layerStore := sim.store.Store().At("layers/" + ref.LayerID)
		meta, err := layerStore.HeadMeta(ctx, "index.json")
		if err != nil {
			continue
		}
		if meta["frozen_at"] != "" {
			return name
		}
	}
	return ""
}

func (sim *Simulation) opVerifySharedVolume(ctx context.Context, node *SimNode) {
	_ = ctx
	_ = node
}

func (sim *Simulation) opCreateVolume(ctx context.Context, node *SimNode) {
	// Limit total timeline creation to keep the simulation tractable.
	if !sim.canAddLineages(1) {
		return
	}
	name := fmt.Sprintf("vol-%d", sim.nextVolID)
	sim.nextVolID++

	v, err := sim.managerFor(node, name).NewVolume(CreateParams{Volume: name, Size: uint64(sim.config.DevicePages) * PageSize})
	if err != nil {
		return // S3 faults can cause creation failures
	}
	_ = v

	ref, err := sim.getVolRef(ctx, name)
	if err != nil {
		return
	}

	sim.volumeTimelines[name] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[name] = node.id
	node.volumes = append(node.volumes, name)
	sim.t.Logf("[%s] CREATE %s tl=%s", node.id, name, ref.LayerID[:8])
}

func (sim *Simulation) opCopyFrom(ctx context.Context, node *SimNode) {
	if len(node.volumes) < 2 {
		return
	}

	// Pick two different volumes.
	i := sim.rng.IntN(len(node.volumes))
	j := sim.rng.IntN(len(node.volumes) - 1)
	if j >= i {
		j++
	}
	dstName := node.volumes[i]
	srcName := node.volumes[j]

	dst := sim.getVolume(node, dstName)
	src := sim.getVolume(node, srcName)
	if dst == nil || src == nil {
		return
	}

	maxPage := PageIdx(sim.config.DevicePages)
	numPages := PageIdx(sim.rng.IntN(10) + 1)
	if numPages > maxPage {
		numPages = maxPage
	}

	srcStart := PageIdx(sim.rng.Uint64N(uint64(maxPage - numPages + 1)))
	dstStart := PageIdx(sim.rng.Uint64N(uint64(maxPage - numPages + 1)))

	srcOff := srcStart.ByteOffset()
	dstOff := dstStart.ByteOffset()
	length := uint64(numPages) * PageSize

	srcTL := sim.volumeTimelines[srcName]
	dstTL := sim.volumeTimelines[dstName]

	copied, err := dst.CopyFrom(src, srcOff, dstOff, length)

	// Record in oracle: pages that were actually copied. Write enters the
	// memLayer before auto-flush, so always record based on copied (which
	// now includes the page whose Write triggered a flush error).
	pagesCopied := PageIdx(copied / PageSize)
	for pg := PageIdx(0); pg < pagesCopied; pg++ {
		expected := sim.oracle.ExpectedRead(node.id, srcTL, srcStart+pg)
		if traceLayerEnabled(dstTL) && tracePageEnabled(dstStart+pg) {
			sim.t.Logf("TRACE opCopyFrom record tl=%s page=%d gotZero=%v", dstTL, dstStart+pg, bytes.Equal(expected, zeroPage[:]))
		}
		sim.oracle.RecordWrite(node.id, dstTL, dstStart+pg, expected)
	}

	if err != nil {
		// Some pages were written (in memLayer) but auto-flush failed.
		// Try to flush what's there so oracle stays consistent.
		if fErr := dst.Flush(); fErr != nil {
			return
		}
		sim.oracle.RecordFlush(node.id, dstTL)
		return
	}

	if err := dst.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, dstTL)
}

func (sim *Simulation) opConcurrentWrite(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)

	// Pre-generate all write data from the sim RNG before spawning goroutines.
	// The RNG is not safe for concurrent use.
	type writeOp struct {
		pageIdx PageIdx
		data    []byte
	}
	const numWriters = 3
	writerOps := make([][]writeOp, numWriters)
	for w := range numWriters {
		numPages := sim.rng.IntN(5) + 1
		writerOps[w] = make([]writeOp, numPages)
		for i := range numPages {
			writerOps[w][i] = writeOp{
				pageIdx: PageIdx(sim.rng.Uint64N(uint64(maxPage))),
				data:    make([]byte, PageSize),
			}
			sim.fillRandomBytes(writerOps[w][i].data)
		}
	}

	// Spawn concurrent writers.
	var wg sync.WaitGroup
	for w := range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, op := range writerOps[w] {
				v.Write(op.data, op.pageIdx.ByteOffset())
			}
		}()
	}
	wg.Wait()

	// Record writes in oracle. The order of concurrent writes to the same
	// page is non-deterministic, so we read back each written page to get
	// the actual value and record that.
	writtenPages := make(map[PageIdx]struct{})
	for _, ops := range writerOps {
		for _, op := range ops {
			writtenPages[op.pageIdx] = struct{}{}
		}
	}
	for pageIdx := range writtenPages {
		buf := make([]byte, PageSize)
		if _, err := v.Read(ctx, buf, pageIdx.ByteOffset()); err != nil {
			continue
		}
		if traceLayerEnabled(timelineID) && tracePageEnabled(pageIdx) {
			sim.t.Logf("TRACE opConcurrentWrite record tl=%s page=%d gotZero=%v", timelineID, pageIdx, bytes.Equal(buf, zeroPage[:]))
		}
		sim.oracle.RecordWrite(node.id, timelineID, pageIdx, buf)
	}

	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

func (sim *Simulation) opWriteAllPages(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)

	for pageIdx := PageIdx(0); pageIdx < maxPage; pageIdx++ {
		data := make([]byte, PageSize)
		sim.fillRandomBytes(data)

		err := v.Write(data, pageIdx.ByteOffset())
		if err == nil {
			if traceLayerEnabled(timelineID) && tracePageEnabled(pageIdx) {
				sim.t.Logf("TRACE opWriteAllPages success tl=%s page=%d", timelineID, pageIdx)
			}
			sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)
		} else {
			verify := make([]byte, PageSize)
			readN, readErr := v.Read(ctx, verify, pageIdx.ByteOffset())
			if traceLayerEnabled(timelineID) && tracePageEnabled(pageIdx) {
				sim.t.Logf("TRACE opWriteAllPages err tl=%s page=%d err=%v readErr=%v readN=%d verifyEq=%v gotZero=%v",
					timelineID, pageIdx, err, readErr, readN, readErr == nil && bytes.Equal(verify, data), bytes.Equal(verify, zeroPage[:]))
			}
			if readErr == nil && bytes.Equal(verify, data) {
				sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)
			} else {
				return
			}
		}

		if err != nil {
			if fErr := v.Flush(); fErr != nil {
				return
			}
			sim.oracle.RecordFlush(node.id, timelineID)
			return
		}
	}

	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

func (sim *Simulation) opDeleteVolume(ctx context.Context, node *SimNode) {
	// Find volumes not leased by any node and not ancestors of other volumes.
	var candidates []string
	for name := range sim.volumeTimelines {
		if _, held := sim.leases[name]; held {
			continue
		}
		if sim.hasChildren(name) {
			continue
		}
		candidates = append(candidates, name)
	}
	if len(candidates) == 0 {
		return
	}
	sort.Strings(candidates)

	name := candidates[sim.rng.IntN(len(candidates))]
	// Use a temporary manager for the delete (volume is not open on any node).
	dm := sim.newManager(sim.t.TempDir(), node.fs)
	sim.allManagers = append(sim.allManagers, dm)
	if err := DeleteVolume(ctx, dm.Store(), name); err != nil {
		return
	}

	delete(sim.volumeTimelines, name)
	sim.deletedVolumes[name] = true
}

func (sim *Simulation) opCloseVolume(ctx context.Context, node *SimNode) {
	if len(node.volumes) == 0 {
		return
	}

	// Pick a random volume to close.
	idx := sim.rng.IntN(len(node.volumes))
	volName := node.volumes[idx]

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	// Flush before closing so oracle stays consistent.
	timelineID := sim.volumeTimelines[volName]
	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)

	// ReleaseRef → destroy → closeVolume when refcount hits zero.
	v.ReleaseRef()

	// Remove from node's volume list, close the per-volume manager, and release lease.
	node.volumes = append(node.volumes[:idx], node.volumes[idx+1:]...)
	if m, ok := node.managers[volName]; ok {
		m.Close()
		delete(node.managers, volName)
	}
	delete(sim.leases, volName)
}

func (sim *Simulation) opDeepClone(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	// Clone from the most recently cloned volume (if this node owns it),
	// creating deeper ancestor chains (3-5+).
	if sim.lastClonedVolume == "" {
		return
	}
	srcName := sim.lastClonedVolume
	if sim.leases[srcName] != node.id {
		return
	}

	v := sim.getVolume(node, srcName)
	if v == nil {
		return
	}

	cloneName := fmt.Sprintf("%s-deep-%d", srcName, sim.nextVolID)
	sim.nextVolID++

	// Flush parent before cloning.
	if err := v.Flush(); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[srcName]
	sim.oracle.RecordFlush(node.id, parentTL)

	var parentNextSeq uint64
	{
		pv := v
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := sim.checkpointAndClone(ctx, v, cloneName); err != nil {
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		return
	}

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	{
		cv := clone
		sim.t.Logf("[%s] DEEP-CLONE parent=%s (nextSeq=%d) -> child=%s (ancestorSeq=%d)",
			node.id, parentTL[:8], parentNextSeq, ref.LayerID[:8], cv.layer.nextSeq.Load())
	}

	sim.volumeTimelines[cloneName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, parentNextSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true

	sim.lastClonedVolume = cloneName

	// Re-layering: parent now has a new layer ID.
	sim.updateParentAfterRelayer(ctx, srcName, parentTL, parentNextSeq)

	_ = clone
}

func (sim *Simulation) opCloneFromSnapshot(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(1) {
		return
	}
	// Find a frozen volume that isn't leased.
	var snapName string
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for name := range sim.volumeTimelines {
		volNames = append(volNames, name)
	}
	sort.Strings(volNames)
	for _, name := range volNames {
		if _, held := sim.leases[name]; held {
			continue
		}
		ref, err := sim.getVolRef(ctx, name)
		if err != nil {
			continue
		}
		layerStore := sim.store.Store().At("layers/" + ref.LayerID)
		meta, err := layerStore.HeadMeta(ctx, "index.json")
		if err != nil {
			continue
		}
		if meta["frozen_at"] != "" {
			snapName = name
			break
		}
	}
	if snapName == "" {
		return
	}

	// Open the snapshot temporarily.
	snapVol, err := sim.managerFor(node, snapName).OpenVolume(snapName)
	if err != nil {
		return
	}
	sim.leases[snapName] = node.id

	cloneName := fmt.Sprintf("%s-sclone-%d", snapName, sim.nextVolID)
	sim.nextVolID++

	parentTL := sim.volumeTimelines[snapName]
	var parentNextSeq uint64
	{
		pv := snapVol
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := sim.checkpointAndClone(ctx, snapVol, cloneName); err != nil {
		// Close the snapshot.
		snapVol.ReleaseRef()
		delete(sim.leases, snapName)
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		snapVol.ReleaseRef()
		delete(sim.leases, snapName)
		return
	}

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		snapVol.ReleaseRef()
		delete(sim.leases, snapName)
		return
	}

	sim.t.Logf("[%s] CLONE-FROM-SNAP snap=%s -> child=%s", node.id, parentTL[:8], ref.LayerID[:8])

	sim.volumeTimelines[cloneName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, parentNextSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true

	// Close the snapshot volume.
	snapVol.ReleaseRef()
	delete(sim.leases, snapName)

	sim.lastClonedVolume = cloneName
	_ = clone
}

// opCheckpoint creates a checkpoint on an owned volume. Unlike opSnapshot which
// creates a separate named volume, a checkpoint is stored under the volume's own
// key namespace (volumes/{name}/checkpoints/{ts}/index.json).
func (sim *Simulation) opCheckpoint(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	// Flush before checkpoint so unflushed writes are captured.
	if err := v.Flush(); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	// Capture branchSeq before checkpoint.
	var branchSeq uint64
	{
		pv := v
		branchSeq = pv.layer.nextSeq.Load()
	}

	ts, err := v.Checkpoint()
	if err != nil {
		return
	}

	// Read the checkpoint ref to get the child layer ID.
	cpKey := volName + "/checkpoints/" + ts + "/index.json"
	volRefs := sim.store.Store().At("volumes")
	cpRef, _, err := blob.ReadJSON[checkpointRef](ctx, volRefs, cpKey)
	if err != nil {
		return
	}

	sim.timelineIDs = append(sim.timelineIDs, cpRef.LayerID)
	sim.oracle.RecordBranch(cpRef.LayerID, parentTL, branchSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][cpRef.LayerID] = true

	sim.t.Logf("[%s] CHECKPOINT vol=%s parent=%s child=%s ts=%s",
		node.id, volName, parentTL[:8], cpRef.LayerID[:8], ts)

	// Re-layering: parent now has a new layer ID.
	sim.updateParentAfterRelayer(ctx, volName, parentTL, branchSeq)
}

// opCloneFromCheckpoint picks a volume with checkpoints, lists them, picks one,
// and clones from it through the shared object store.
func (sim *Simulation) opCloneFromCheckpoint(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(1) {
		return
	}
	// Find a volume with checkpoints. Try all known volumes in deterministic order.
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for name := range sim.volumeTimelines {
		volNames = append(volNames, name)
	}
	sort.Strings(volNames)

	var srcVolName string
	var checkpoints []CheckpointInfo
	store := sim.store.Store()
	for _, name := range volNames {
		cps, err := ListCheckpoints(ctx, store, name)
		if err != nil || len(cps) == 0 {
			continue
		}
		srcVolName = name
		checkpoints = cps
		break
	}
	if srcVolName == "" {
		return
	}

	// Pick a random checkpoint.
	cp := checkpoints[sim.rng.IntN(len(checkpoints))]

	cloneName := fmt.Sprintf("%s-cpclone-%d", srcVolName, sim.nextVolID)
	sim.nextVolID++

	// Read the checkpoint ref to get the parent layer ID for oracle tracking.
	cpKey := srcVolName + "/checkpoints/" + cp.ID + "/index.json"
	volRefs := store.At("volumes")
	cpRef, _, err := blob.ReadJSON[checkpointRef](ctx, volRefs, cpKey)
	if err != nil {
		return
	}

	if err := Clone(ctx, store, srcVolName, cp.ID, cloneName); err != nil {
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Read the clone's volume ref.
	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	sim.t.Logf("[%s] CLONE-FROM-CP src=%s cp=%s -> child=%s",
		node.id, srcVolName, cp.ID, ref.LayerID[:8])

	sim.volumeTimelines[cloneName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.LayerID, cpRef.LayerID, 0)

	if sim.timelineChildren[cpRef.LayerID] == nil {
		sim.timelineChildren[cpRef.LayerID] = make(map[string]bool)
	}
	sim.timelineChildren[cpRef.LayerID][ref.LayerID] = true

	sim.lastClonedVolume = cloneName
	_ = clone
}

func (sim *Simulation) opVerify(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)
	for pageIdx := PageIdx(0); pageIdx < maxPage; pageIdx++ {
		buf := make([]byte, PageSize)
		_, err := v.Read(ctx, buf, pageIdx.ByteOffset())
		if err != nil {
			return
		}
		if !sim.oracle.VerifyRead(node.id, timelineID, pageIdx, buf) {
			expected := sim.oracle.ExpectedRead(node.id, timelineID, pageIdx)
			sim.t.Logf("MID-SIM VERIFY MISMATCH vol=%s", volName)
			sim.reportMismatch(ctx, v, volName, node.id, timelineID, pageIdx, buf, expected)
		}
	}
}

// opSnapshotIsolation verifies that writes to a parent volume after cloning
// don't bleed into the child. This is a targeted isolation check:
//  1. Clone an owned volume.
//  2. Write new data to the parent.
//  3. Read the same pages from the child and assert they still have
//     the pre-clone values.
func (sim *Simulation) opSnapshotIsolation(ctx context.Context, node *SimNode) {
	if !sim.canAddLineages(2) {
		return
	}
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := sim.getVolume(node, volName)
	if v == nil {
		return
	}

	// Flush parent so state is consistent.
	if err := v.Flush(); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	// Snapshot the parent's current page state (from oracle) for later comparison.
	maxPage := PageIdx(sim.config.DevicePages)
	preClone := make(map[PageIdx][]byte)
	for pg := PageIdx(0); pg < maxPage; pg++ {
		preClone[pg] = sim.oracle.ExpectedRead(node.id, parentTL, pg)
	}

	var parentNextSeq uint64
	{
		pv := v
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	cloneName := fmt.Sprintf("%s-iso-%d", volName, sim.nextVolID)
	sim.nextVolID++
	if err := sim.checkpointAndClone(ctx, v, cloneName); err != nil {
		return
	}
	clone, err := sim.managerFor(node, cloneName).OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Checkpoint internally flushes parent data.
	sim.oracle.RecordFlush(node.id, parentTL)

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	childTL := ref.LayerID
	sim.volumeTimelines[cloneName] = childTL
	sim.timelineIDs = append(sim.timelineIDs, childTL)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(childTL, parentTL, parentNextSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][childTL] = true

	// Re-layering: parent now has a new layer ID. Must update before
	// writing to the parent so oracle tracks writes against the new TL.
	parentTL = sim.updateParentAfterRelayer(ctx, volName, parentTL, parentNextSeq)

	// Write new data to 1-5 random pages on the parent.
	numWrites := sim.rng.IntN(5) + 1
	writtenPages := make(map[PageIdx]struct{})
	for range numWrites {
		pg := PageIdx(sim.rng.Uint64N(uint64(maxPage)))
		data := make([]byte, PageSize)
		sim.fillRandomBytes(data)
		if err := v.Write(data, pg.ByteOffset()); err == nil {
			if traceLayerEnabled(parentTL) && tracePageEnabled(pg) {
				sim.t.Logf("TRACE opCloneIsolationParentWrite record tl=%s page=%d", parentTL, pg)
			}
			sim.oracle.RecordWrite(node.id, parentTL, pg, data)
			writtenPages[pg] = struct{}{}
		}
	}
	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, parentTL)

	// Verify child still sees pre-clone data for the pages we wrote on the parent.
	for pg := range writtenPages {
		buf := make([]byte, PageSize)
		_, err := clone.Read(ctx, buf, pg.ByteOffset())
		if err != nil {
			continue
		}
		if !bytes.Equal(buf, preClone[pg]) {
			sim.t.Logf("SNAPSHOT ISOLATION VIOLATION: parent write to page %d leaked into child", pg)
			sim.t.Logf("  child got:      %x...", buf[:32])
			sim.t.Logf("  expected (pre): %x...", preClone[pg][:32])
			sim.t.FailNow()
		}
	}

	_ = clone
}

func (sim *Simulation) opOpen(ctx context.Context, node *SimNode) {
	// Find an unleased volume and open it on this node.
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for name := range sim.volumeTimelines {
		volNames = append(volNames, name)
	}
	sort.Strings(volNames)
	for _, name := range volNames {
		if _, held := sim.leases[name]; !held {
			sim.acquireVolume(ctx, node, name)
			return
		}
	}
}

func (sim *Simulation) hasChildren(name string) bool {
	tlID := sim.volumeTimelines[name]
	return len(sim.timelineChildren[tlID]) > 0
}

func (sim *Simulation) opCrash(_ context.Context, node *SimNode) {
	node.alive = false
	node.fs.Crash()

	// Release all leases.
	for _, volName := range node.volumes {
		delete(sim.leases, volName)
	}

	// Discard unflushed oracle state.
	sim.oracle.RecordCrash(node.id)

	// Kill the managers' background goroutines most of the time to keep the
	// synctest bubble lean. Leave them running ~5% of the time to exercise
	// the case where a crashed process's goroutines linger (e.g. stuck I/O).
	if sim.rng.Float64() > 0.05 {
		for _, m := range node.managers {
			m.Close()
		}
	}

	node.managers = make(map[string]*Manager)
	node.volumes = nil
}

func (sim *Simulation) recoverNode(ctx context.Context, node *SimNode) {
	node.fs = NewSimLocalFS()
	node.alive = true
	node.volumes = nil
	node.managers = make(map[string]*Manager)

	// Re-acquire up to 3 unleased volumes. Production nodes typically serve
	// multiple volumes; this exercises concurrent timeline opens and shared
	// page cache pressure.
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for volName := range sim.volumeTimelines {
		volNames = append(volNames, volName)
	}
	sort.Strings(volNames)
	acquired := 0
	for _, volName := range volNames {
		if acquired >= 3 {
			break
		}
		if _, held := sim.leases[volName]; !held {
			sim.acquireVolume(ctx, node, volName)
			acquired++
		}
	}

	// Double-crash: 10% chance of crashing again immediately after recovery.
	// Simulates crash-loop scenarios where the process restarts and dies
	// again before doing any useful work.
	if sim.rng.Float64() < 0.10 {
		sim.t.Logf("[%s] DOUBLE-CRASH: crashing again immediately after recovery", node.id)
		sim.opCrash(ctx, node)
	}
}

func (sim *Simulation) setTimelineDebugLog(node *SimNode, name string) {
	v := sim.getVolume(node, name)
	if v == nil {
		return
	}
	actualTL := v.layer.id
	oracleTL := sim.volumeTimelines[name]
	if actualTL != oracleTL {
		sim.t.Fatalf("TIMELINE MISMATCH on open: vol=%s oracle=%s actual=%s", name, oracleTL[:8], actualTL[:8])
	}
}

func (sim *Simulation) createVolume(ctx context.Context, node *SimNode, name string) {
	v, err := sim.managerFor(node, name).NewVolume(CreateParams{Volume: name, Size: uint64(sim.config.DevicePages) * PageSize})
	if err != nil {
		sim.t.Fatalf("create volume %s: %v", name, err)
	}
	_ = v

	ref, err := sim.getVolRef(ctx, name)
	if err != nil {
		sim.t.Fatalf("get vol ref %s: %v", name, err)
	}

	sim.volumeTimelines[name] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.leases[name] = node.id
	node.volumes = append(node.volumes, name)
	sim.setTimelineDebugLog(node, name)
}

func (sim *Simulation) acquireVolume(ctx context.Context, node *SimNode, name string) {
	_, err := sim.managerFor(node, name).OpenVolume(name)
	if err != nil {
		return
	}
	sim.leases[name] = node.id
	node.volumes = append(node.volumes, name)
	sim.setTimelineDebugLog(node, name)
}

func (sim *Simulation) pickOwnedVolume(node *SimNode) string {
	if len(node.volumes) == 0 {
		return ""
	}
	return node.volumes[sim.rng.IntN(len(node.volumes))]
}

// FullScan reopens every known volume on a fresh manager and reads every
// page, verifying each against the oracle. This catches latent corruption
// that random reads during the simulation might miss.
func (sim *Simulation) FullScan() {
	ctx := sim.t.Context()
	sim.store.ClearFaults()

	// Verify ListAllVolumes contains every tracked volume.
	// Note: S3 may also contain partially-created volumes from faulted
	// opCreateVolume calls (ref written but getVolRef failed), so we only
	// check that every tracked volume is present, not the reverse.
	allNames, err := ListAllVolumes(ctx, sim.store.Store())
	if err != nil {
		sim.t.Fatalf("ListAllVolumes: %v", err)
	}
	allNameSet := make(map[string]bool)
	for _, name := range allNames {
		allNameSet[name] = true
	}
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for volName := range sim.volumeTimelines {
		volNames = append(volNames, volName)
	}
	sort.Strings(volNames)
	for _, volName := range volNames {
		if !allNameSet[volName] {
			sim.t.Fatalf("ListAllVolumes missing volume %q", volName)
		}
	}
	// Deleted volumes must NOT appear.
	for name := range sim.deletedVolumes {
		if allNameSet[name] {
			sim.t.Fatalf("ListAllVolumes still contains deleted volume %q", name)
		}
	}

	var scanned int
	for _, volName := range volNames {
		timelineID := sim.volumeTimelines[volName]
		// Verify that the oracle's timeline ID matches what's actually in S3.
		ref, refErr := sim.getVolRef(ctx, volName)
		if refErr != nil {
			sim.t.Logf("full scan: cannot read vol ref for %s: %v", volName, refErr)
			continue
		}
		if ref.LayerID != timelineID {
			sim.t.Fatalf("full scan: vol %s timeline mismatch: oracle=%s s3=%s", volName, timelineID, ref.LayerID)
		}

		// Each volume gets its own manager (one volume per manager).
		vm := sim.newManager(sim.t.TempDir(), NewSimLocalFS())
		sim.allManagers = append(sim.allManagers, vm)

		v, err := vm.OpenVolumeReadOnly(volName)
		if err != nil {
			// Volume may have been created during a fault window and
			// partially written. Skip unloadable volumes.
			continue
		}

		maxPage := PageIdx(sim.config.DevicePages)
		for pageIdx := PageIdx(0); pageIdx < maxPage; pageIdx++ {
			buf := make([]byte, PageSize)
			_, err := v.Read(ctx, buf, pageIdx.ByteOffset())
			if err != nil {
				sim.t.Fatalf("full scan read error: vol=%s page=%d: %v", volName, pageIdx, err)
			}

			// Fresh manager — no node context. Maybe-flushed data from crashed
			// nodes may or may not have survived, so use the crash-tolerant check.
			if !sim.oracle.VerifyReadAfterCrash(timelineID, pageIdx, buf) {
				expected := sim.oracle.ExpectedRead("", timelineID, pageIdx)
				sim.reportMismatch(ctx, v, volName, "", timelineID, pageIdx, buf, expected)
			}
		}
		vm.Close()
		scanned++
	}

	// Structural invariant checks: verify layer reference integrity.
	sim.verifyLayerIntegrity(ctx, volNames)

	// Run GC and verify it doesn't delete any reachable layers.
	// Dry-run first to count orphans, then real run to delete them.
	dryResult, err := GarbageCollect(ctx, sim.store.Store(), true, 0)
	if err != nil {
		sim.t.Fatalf("GC dry-run: %v", err)
	}
	sim.t.Logf("full scan: GC dry-run: %d reachable, %d orphaned layers",
		dryResult.ReachableLayers, dryResult.OrphanedLayers)

	if dryResult.OrphanedLayers > 0 {
		result, err := GarbageCollect(ctx, sim.store.Store(), false, 0)
		if err != nil {
			sim.t.Fatalf("GC: %v", err)
		}
		sim.t.Logf("full scan: GC deleted %d orphaned layers (%d objects, %.1f KB)",
			result.OrphanedLayers, result.DeletedObjects, float64(result.DeletedBytes)/1024)

		// Verify all tracked volumes are still readable after GC.
		for _, volName := range volNames {
			tlID := sim.volumeTimelines[volName]
			layerStore := sim.store.Store().At("layers/" + tlID)
			if _, _, err := layerStore.Get(ctx, "index.json"); err != nil {
				sim.t.Fatalf("GC deleted reachable layer %s for volume %s: %v", tlID[:8], volName, err)
			}
		}

		// Run GC again — should find 0 orphans (idempotent).
		recheck, err := GarbageCollect(ctx, sim.store.Store(), true, 0)
		if err != nil {
			sim.t.Fatalf("GC recheck: %v", err)
		}
		if recheck.OrphanedLayers != 0 {
			sim.t.Fatalf("GC recheck found %d orphans after deletion (expected 0)", recheck.OrphanedLayers)
		}
	}

	sim.t.Logf("full scan: verified %d volumes, %d pages each", scanned, sim.config.DevicePages)
}

// verifyLayerIntegrity checks structural invariants of the S3 state:
//   - Every volume ref points to a layer that has an index.json.
//   - No volume ref points to a nonexistent layer.
//   - FROZEN LAYER INVARIANT: No two active (non-read-only) volumes share
//     the same layer ID. After re-layering, each writable volume has its
//     own unique layer. Shared layers (from clone/snapshot) are only
//     referenced by read-only volumes or as ancestors.
func (sim *Simulation) verifyLayerIntegrity(ctx context.Context, volNames []string) {
	store := sim.store.Store()

	// Collect active (writable) layer IDs to verify uniqueness.
	activeLayerOwners := make(map[string]string) // layerID → volName

	for _, volName := range volNames {
		ref, err := sim.getVolRef(ctx, volName)
		if err != nil {
			continue
		}

		// FROZEN LAYER INVARIANT CHECK: No two writable volumes share a layer.
		// Check layer metadata — Freeze sets frozen_at on the layer's index.json.
		ls := store.At("layers/" + ref.LayerID)
		meta, _ := ls.HeadMeta(ctx, "index.json")
		isFrozen := meta["frozen_at"] != ""
		if !isFrozen {
			if other, exists := activeLayerOwners[ref.LayerID]; exists {
				sim.t.Fatalf("FROZEN LAYER INVARIANT VIOLATION: writable volumes %q and %q share layer %s",
					other, volName, ref.LayerID[:8])
			}
			activeLayerOwners[ref.LayerID] = volName
		}

		// Check index.json exists for this layer.
		layerStore := store.At("layers/" + ref.LayerID)
		idxRC, _, err := layerStore.Get(ctx, "index.json")
		if err != nil {
			sim.t.Fatalf("layer integrity: vol=%s layer=%s missing index.json: %v",
				volName, ref.LayerID[:8], err)
		}

		// Parse the index to verify it's valid JSON.
		var idx layerIndex
		if err := json.NewDecoder(idxRC).Decode(&idx); err != nil {
			idxRC.Close()
			sim.t.Fatalf("layer integrity: vol=%s layer=%s corrupt index.json: %v",
				volName, ref.LayerID[:8], err)
		}
		idxRC.Close()
	}
}

// updateParentAfterRelayer updates the sim's oracle and tracking maps after a
// clone/snapshot re-layers the parent volume. Returns the new parent timeline ID.
//
// INVARIANT: Only frozen (immutable) layers may be referenced by other layers.
// After branch(), the parent calls relayer() to create a new layer, ensuring
// the old layer (now shared with the child) is never written to again.
func (sim *Simulation) updateParentAfterRelayer(ctx context.Context, volName string, oldParentTL string, branchSeq uint64) string {
	ref, err := sim.getVolRef(ctx, volName)
	if err != nil {
		return oldParentTL
	}
	newParentTL := ref.LayerID
	if newParentTL == oldParentTL {
		return oldParentTL // no re-layering (e.g. read-only volume)
	}

	sim.volumeTimelines[volName] = newParentTL
	sim.timelineIDs = append(sim.timelineIDs, newParentTL)
	sim.oracle.RecordBranch(newParentTL, oldParentTL, branchSeq)

	if sim.timelineChildren[oldParentTL] == nil {
		sim.timelineChildren[oldParentTL] = make(map[string]bool)
	}
	sim.timelineChildren[oldParentTL][newParentTL] = true

	sim.t.Logf("RELAYER parent vol=%s old=%s new=%s", volName, oldParentTL[:8], newParentTL[:8])
	return newParentTL
}

func (sim *Simulation) getVolRef(ctx context.Context, name string) (volumeRef, error) {
	volRefs := sim.store.Store().At("volumes")
	ref, _, err := blob.ReadJSON[volumeRef](ctx, volRefs, name+"/index.json")
	return ref, err
}

// TestSimulation runs the deterministic simulation test.
// simEnvInt reads an env var as int, returning def if unset.
func simEnvInt(t *testing.T, key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("invalid %s: %v", key, err)
	}
	return n
}

// simEnvFloat reads an env var as float64, returning def if unset.
func simEnvFloat(t *testing.T, key string, def float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("invalid %s: %v", key, err)
	}
	return n
}

// runSimulation runs a full simulation with the given seed and config, returning
// the oracle event log. Used by both TestSimulation and TestSimulationDeterminism.
func runSimulation(t *testing.T, seed uint64, config SimConfig) []string {
	var events []string
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, seed, config)
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()
		sim.Run()
		sim.FullScan()
		events = sim.oracle.EventLog()
	})
	return events
}

func TestSimulation(t *testing.T) {
	if raceBuild {
		t.Skip("simulation is covered by focused storage tests under -race; skip long synctest run")
	}
	var seed uint64
	if s := os.Getenv("SIM_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("invalid SIM_SEED: %v", err)
		}
	} else {
		var buf [8]byte
		_, _ = crand.Read(buf[:])
		seed = binary.LittleEndian.Uint64(buf[:])
	}

	nodes := simEnvInt(t, "SIM_NODES", 2)
	ops := simEnvInt(t, "SIM_OPS", 40)
	pages := simEnvInt(t, "SIM_PAGES", 64)
	lineages := simEnvInt(t, "SIM_LINEAGES", 8)
	crashRate := simEnvFloat(t, "SIM_CRASH_RATE", 0.005)
	// Dedicated fault/backpressure tests cover the new blocking semantics in
	// detail. Keep the default randomized simulation fault-free so the broad
	// end-to-end invariant check remains fast and stable under -race. Faulty
	// simulation runs are still available by setting SIM_FAULT_RATE explicitly.
	faultRate := simEnvFloat(t, "SIM_FAULT_RATE", 0)

	t.Logf("seed: %d  nodes: %d  ops: %d  pages: %d  lineages: %d  crash: %.2f  faults: %.2f",
		seed, nodes, ops, pages, lineages, crashRate, faultRate)

	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, seed, SimConfig{
			NumNodes:       nodes,
			DevicePages:    pages,
			MaxLineages:    lineages,
			OpsPerNode:     ops,
			CrashRate:      crashRate,
			FlushThreshold: 8 * PageSize,
			S3Faults: FaultConfig{
				PutFailRate:              faultRate,
				GetFailRate:              faultRate,
				ListFailRate:             faultRate / 2,
				DeleteFailRate:           faultRate / 2,
				PhantomPutRate:           0.02,
				LayersJsonPhantomPutRate: 0.03,
			},
		})
		defer sim.useDeterministicLayerIDs()()
		// Defer cleanup so periodic flush goroutines are stopped even if
		// t.FailNow()/t.Fatalf() exits the goroutine via runtime.Goexit().
		// Without this, synctest detects leaked goroutines and panics with
		// "deadlock: main bubble goroutine has exited but blocked goroutines remain".
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()
		sim.Run()
		sim.FullScan()
	})
}

func TestSimulationDeterminism(t *testing.T) {
	if raceBuild {
		t.Skip("simulation determinism check is too expensive under -race")
	}
	var seed uint64
	if s := os.Getenv("SIM_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("invalid SIM_SEED: %v", err)
		}
	} else {
		var buf [8]byte
		_, _ = crand.Read(buf[:])
		seed = binary.LittleEndian.Uint64(buf[:])
	}

	config := SimConfig{
		NumNodes:       1,
		DevicePages:    32,
		MaxLineages:    6,
		OpsPerNode:     20,
		CrashRate:      0.005,
		FlushThreshold: 8 * PageSize,
	}

	t.Logf("determinism check seed: %d", seed)

	events1 := runSimulation(t, seed, config)
	events2 := runSimulation(t, seed, config)

	if len(events1) != len(events2) {
		t.Fatalf("event count mismatch: run1=%d run2=%d", len(events1), len(events2))
	}
	for i := range events1 {
		if events1[i] != events2[i] {
			// Show context around the divergence point.
			start := i - 3
			if start < 0 {
				start = 0
			}
			end := i + 3
			if end > len(events1) {
				end = len(events1)
			}
			for j := start; j < end; j++ {
				marker := "  "
				if j == i {
					marker = ">>"
				}
				t.Logf("%s [%d] run1: %s", marker, j, events1[j])
				t.Logf("%s [%d] run2: %s", marker, j, events2[j])
			}
			t.Fatalf("determinism failure at event %d/%d", i, len(events1))
		}
	}
	t.Logf("determinism OK: %d events identical across 2 runs", len(events1))
}

func TestSimulationSharedLayerReadExercise(t *testing.T) {
	if raceBuild {
		t.Skip("simulation exercise is too expensive under -race")
	}
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 1, SimConfig{
			NumNodes:       2,
			DevicePages:    64,
			MaxLineages:    10,
			OpsPerNode:     1,
			CrashRate:      0,
			FlushThreshold: 8 * PageSize,
		})
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()

		ctx := t.Context()
		sim.createVolume(ctx, sim.nodes[0], "vol-0")
		v := sim.getVolume(sim.nodes[0], "vol-0")
		require.NotNil(t, v)

		timelineID := sim.volumeTimelines["vol-0"]
		for i := 0; i < 20; i++ {
			page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
			require.NoError(t, v.Write(page, uint64(i)*PageSize))
			sim.oracle.RecordWrite(sim.nodes[0].id, timelineID, PageIdx(i), page)
		}
		require.NoError(t, v.Flush())
		sim.oracle.RecordFlush(sim.nodes[0].id, timelineID)

		sim.opSnapshot(ctx, sim.nodes[0])
		snapName := sim.pickReadOnlyVolume(ctx)
		require.NotEmpty(t, snapName)

		sim.opVerifySharedVolume(ctx, sim.nodes[1])
		sim.FullScan()
	})
}

func TestSimulationReproRelayeredSingleBlockPageDisappears(t *testing.T) {
	if raceBuild {
		t.Skip("simulation repro is too expensive under -race")
	}
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 4981481962547272448, SimConfig{
			NumNodes:       3,
			DevicePages:    256,
			MaxLineages:    120,
			OpsPerNode:     2000,
			CrashRate:      0.15,
			FlushThreshold: 8 * PageSize,
			S3Faults: FaultConfig{
				PutFailRate:              0.20,
				GetFailRate:              0.20,
				ListFailRate:             0.10,
				DeleteFailRate:           0.10,
				PhantomPutRate:           0.02,
				LayersJsonPhantomPutRate: 0.03,
			},
		})
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()

		sim.Run()
		sim.FullScan()
	})
}

func TestSimulationLineageCapBlocksCloneAndRelayer(t *testing.T) {
	if raceBuild {
		t.Skip("simulation lineage test is too expensive under -race")
	}
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 1, SimConfig{
			NumNodes:       1,
			DevicePages:    32,
			MaxLineages:    2,
			OpsPerNode:     1,
			CrashRate:      0,
			FlushThreshold: 8 * PageSize,
		})
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()

		ctx := t.Context()
		sim.createVolume(ctx, sim.nodes[0], "vol-0")

		require.Len(t, sim.timelineIDs, 1)
		require.Len(t, sim.volumeTimelines, 1)

		sim.opClone(ctx, sim.nodes[0])

		require.Len(t, sim.timelineIDs, 1)
		require.Len(t, sim.volumeTimelines, 1)
		require.Equal(t, "vol-0", sim.nodes[0].volumes[0])
	})
}

func TestSimulationLineageCapBlocksCheckpoint(t *testing.T) {
	if raceBuild {
		t.Skip("simulation lineage test is too expensive under -race")
	}
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 1, SimConfig{
			NumNodes:       1,
			DevicePages:    32,
			MaxLineages:    2,
			OpsPerNode:     1,
			CrashRate:      0,
			FlushThreshold: 8 * PageSize,
		})
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()

		ctx := t.Context()
		sim.createVolume(ctx, sim.nodes[0], "vol-0")

		require.Len(t, sim.timelineIDs, 1)

		sim.opCheckpoint(ctx, sim.nodes[0])

		require.Len(t, sim.timelineIDs, 1)
		cps, err := ListCheckpoints(ctx, sim.store.Store(), "vol-0")
		require.NoError(t, err)
		require.Empty(t, cps)
	})
}

func TestSimulationLineageCapBlocksCreateVolume(t *testing.T) {
	if raceBuild {
		t.Skip("simulation lineage test is too expensive under -race")
	}
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 1, SimConfig{
			NumNodes:       1,
			DevicePages:    32,
			MaxLineages:    1,
			OpsPerNode:     1,
			CrashRate:      0,
			FlushThreshold: 8 * PageSize,
		})
		defer sim.useDeterministicLayerIDs()()
		defer func() {
			for _, m := range sim.allManagers {
				m.Close()
			}
		}()

		ctx := t.Context()
		sim.createVolume(ctx, sim.nodes[0], "vol-0")

		sim.opCreateVolume(ctx, sim.nodes[0])

		require.Len(t, sim.timelineIDs, 1)
		require.Len(t, sim.volumeTimelines, 1)
	})
}
