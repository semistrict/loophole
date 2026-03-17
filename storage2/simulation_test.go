package storage2

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/require"
)

// SimConfig controls the simulation parameters.
type SimConfig struct {
	NumNodes       int
	DevicePages    int // number of 4KB pages per volume
	MaxTimelines   int
	OpsPerNode     int
	CrashRate      float64 // probability of crash per tick
	S3Faults       FaultConfig
	FlushThreshold int64
	FlushInterval  time.Duration // periodic auto-flush interval; 0 = default (500ms under synctest)
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
	// Monotonic timeline ID counter for deterministic manager id generation.
	nextTLID int
	// Deleted volume names (removed from volumeTimelines).
	deletedVolumes map[string]bool
	// Parent timeline ID → set of child timeline IDs.
	timelineChildren map[string]map[string]bool
	// Last cloned volume name for deep clone chains.
	lastClonedVolume string
	// All managers ever created (for cleanup, including crashed ones).
	allManagers []*Manager
}

// SimNode represents a single simulated instance.
type SimNode struct {
	id      string
	fs      *SimLocalFS
	manager *Manager
	volumes []string // volume names this node has open
	alive   bool
}

func NewSimulation(t *testing.T, seed uint64, config SimConfig) *Simulation {
	rng := mrand.New(mrand.NewPCG(seed, 0))

	store := NewSimObjectStore(rng, config.S3Faults)

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
	}

	// Create nodes.
	for i := range config.NumNodes {
		node := &SimNode{
			id:    fmt.Sprintf("node-%d", i),
			fs:    NewSimLocalFS(),
			alive: true,
		}
		node.manager = sim.newManager(t.TempDir(), node.fs)
		sim.allManagers = append(sim.allManagers, node.manager)
		sim.nodes = append(sim.nodes, node)
	}

	return sim
}

func (sim *Simulation) nextLayerID() string {
	id := fmt.Sprintf("%08x-simtl-%08x", sim.nextTLID, sim.nextTLID)
	sim.nextTLID++
	return id
}

func (sim *Simulation) newManager(cacheDir string, fs localFS) *Manager {
	flushInterval := sim.config.FlushInterval
	if flushInterval == 0 {
		// The main loop sleeps 0-5ms per tick. A 500ms timer fires every
		// ~200 ticks rather than every tick, keeping synctest scheduling
		// cost O(nodes) per tick instead of O(volumes). Still gives ~200+
		// flush fires per volume over a 5000-op run — plenty of coverage
		// for flush/write race conditions.
		flushInterval = 500 * time.Millisecond
	}
	dc, err := NewPageCache(filepath.Join(cacheDir, "diskcache"))
	if err != nil {
		sim.t.Fatalf("create disk cache: %v", err)
	}
	sim.t.Cleanup(func() { _ = dc.Close() })
	m := NewVolumeManager(
		sim.store.Store(),
		cacheDir,
		Config{
			FlushThreshold: sim.config.FlushThreshold,
			FlushInterval:  flushInterval,
		},
		fs,
		dc,
	)
	m.idGen = sim.nextLayerID
	return m
}

func (sim *Simulation) fillRandomBytes(dst []byte) {
	for i := range dst {
		dst[i] = byte(sim.rng.IntN(256))
	}
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
			v := node.manager.GetVolume(volName)
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
		{2, sim.opFreeze},
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

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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

		// Always record: for full-page writes, data enters the memLayer
		// before auto-flush runs. If Write returns an error it's from
		// auto-flush, not from the put — the data is in a frozen layer
		// and will be persisted by the next successful Flush.
		sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)
		wrote = true

		if err != nil {
			break
		}
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

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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

	// Always record: writePage puts the full-page result into the memLayer
	// before auto-flush runs. The data persists even if Write returns an
	// error from auto-flush. We use the actual base page (read above), not
	// the oracle's expected value, so the expected result is correct.
	sim.oracle.RecordWrite(node.id, timelineID, pageIdx, expected)

	if err != nil {
		return
	}
	if err := v.Flush(); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

// reportMismatch dumps both oracle history and timeline layer state for a
// mismatched page, then fatals. This is the single diagnostic entry point
// for all mismatch types (inline reads and full scan).
func (sim *Simulation) reportMismatch(ctx context.Context, vol loophole.Volume, volName, nodeID, timelineID string, pageIdx PageIdx, got, expected []byte) {
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
	if v, ok := vol.(*volume); ok {
		sim.t.Log(v.layer.DebugPage(ctx, pageIdx))
	}

	sim.t.FailNow()
}

func (sim *Simulation) opRead(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
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

	v := node.manager.GetVolume(volName)
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
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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
	if pv, ok := v.(*volume); ok {
		branchSeq = pv.layer.nextSeq.Load()
	}

	snapName := fmt.Sprintf("%s-snap-%d", volName, sim.nextVolID)
	sim.nextVolID++

	err := snapshotVolume(sim.t, v, snapName)
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
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
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
	if pv, ok := v.(*volume); ok {
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := v.Clone(cloneName); err != nil {
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Read the clone's volume ref.
	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	// Log the clone's ancestorSeq for diagnostics.
	if cv, ok := clone.(*volume); ok {
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
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
		return
	}

	cloneName := fmt.Sprintf("%s-clone-%d", volName, sim.nextVolID)
	sim.nextVolID++

	parentTL := sim.volumeTimelines[volName]

	// Capture branchSeq before clone.
	var parentNextSeq uint64
	if pv, ok := v.(*volume); ok {
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	// No pre-flush! Clone's internal freezeMemLayer + flushFrozenLayers
	// persists unflushed data, so the child sees everything.
	if err := v.Clone(cloneName); err != nil {
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Clone's internal flush persisted parent data — record it in oracle.
	sim.oracle.RecordFlush(node.id, parentTL)

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	if cv, ok := clone.(*volume); ok {
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

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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

	v := node.manager.GetVolume(volName)
	if v == nil {
		return
	}

	if _, ok := v.(*volume); !ok {
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
	volName := sim.pickReadOnlyVolume(ctx)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil {
		var err error
		v, err = node.manager.OpenVolume(volName)
		if err != nil {
			return
		}
		defer func() { _ = v.ReleaseRef() }()
	}

	if _, ok := v.(*frozenVolume); !ok {
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
			sim.t.Logf("POST-SHARED-READ MISMATCH vol=%s", volName)
			sim.reportMismatch(ctx, v, volName, node.id, timelineID, pageIdx, buf, expected)
		}
	}
}

func (sim *Simulation) opCreateVolume(ctx context.Context, node *SimNode) {
	// Limit total volumes to keep the simulation tractable.
	if len(sim.volumeTimelines) >= sim.config.MaxTimelines {
		return
	}
	name := fmt.Sprintf("vol-%d", sim.nextVolID)
	sim.nextVolID++

	v, err := node.manager.NewVolume(loophole.CreateParams{Volume: name, Size: uint64(sim.config.DevicePages) * PageSize})
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

func (sim *Simulation) opFreeze(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
		return
	}

	timelineID := sim.volumeTimelines[volName]

	if err := v.Freeze(); err != nil {
		return
	}
	// Freeze flushes internally, so record the flush in oracle.
	sim.oracle.RecordFlush(node.id, timelineID)
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

	dst := node.manager.GetVolume(dstName)
	src := node.manager.GetVolume(srcName)
	if dst == nil || src == nil || dst.ReadOnly() {
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

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
		return
	}

	timelineID := sim.volumeTimelines[volName]
	maxPage := PageIdx(sim.config.DevicePages)

	for pageIdx := PageIdx(0); pageIdx < maxPage; pageIdx++ {
		data := make([]byte, PageSize)
		sim.fillRandomBytes(data)

		err := v.Write(data, pageIdx.ByteOffset())
		sim.oracle.RecordWrite(node.id, timelineID, pageIdx, data)

		if err != nil {
			// Auto-flush error — data is in frozen layer, try to flush.
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
	if err := node.manager.DeleteVolume(ctx, name); err != nil {
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

	v := node.manager.GetVolume(volName)
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

	// Remove from node's volume list and release lease.
	node.volumes = append(node.volumes[:idx], node.volumes[idx+1:]...)
	delete(sim.leases, volName)
}

func (sim *Simulation) opDeepClone(ctx context.Context, node *SimNode) {
	// Clone from the most recently cloned volume (if this node owns it),
	// creating deeper ancestor chains (3-5+).
	if sim.lastClonedVolume == "" {
		return
	}
	srcName := sim.lastClonedVolume
	if sim.leases[srcName] != node.id {
		return
	}

	v := node.manager.GetVolume(srcName)
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
	if pv, ok := v.(*volume); ok {
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := v.Clone(cloneName); err != nil {
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
	if err != nil {
		return
	}

	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	if cv, ok := clone.(*volume); ok {
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
	snapVol, err := node.manager.OpenVolume(snapName)
	if err != nil {
		return
	}
	sim.leases[snapName] = node.id

	cloneName := fmt.Sprintf("%s-sclone-%d", snapName, sim.nextVolID)
	sim.nextVolID++

	parentTL := sim.volumeTimelines[snapName]
	var parentNextSeq uint64
	if pv, ok := snapVol.(*volume); ok {
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	if err := snapVol.Clone(cloneName); err != nil {
		// Close the snapshot.
		snapVol.ReleaseRef()
		delete(sim.leases, snapName)
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
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
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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
	if pv, ok := v.(*volume); ok {
		branchSeq = pv.layer.nextSeq.Load()
	}

	ts, err := v.Checkpoint()
	if err != nil {
		return
	}

	// Read the checkpoint ref to get the child layer ID.
	cpKey := volName + "/checkpoints/" + ts + "/index.json"
	volRefs := sim.store.Store().At("volumes")
	cpRef, _, err := loophole.ReadJSON[checkpointRef](ctx, volRefs, cpKey)
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
// and clones from it using Manager.CloneFromCheckpoint.
func (sim *Simulation) opCloneFromCheckpoint(ctx context.Context, node *SimNode) {
	// Find a volume with checkpoints. Try all known volumes in deterministic order.
	volNames := make([]string, 0, len(sim.volumeTimelines))
	for name := range sim.volumeTimelines {
		volNames = append(volNames, name)
	}
	sort.Strings(volNames)

	var srcVolName string
	var checkpoints []loophole.CheckpointInfo
	for _, name := range volNames {
		cps, err := node.manager.ListCheckpoints(ctx, name)
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
	volRefs := sim.store.Store().At("volumes")
	cpRef, _, err := loophole.ReadJSON[checkpointRef](ctx, volRefs, cpKey)
	if err != nil {
		return
	}

	if err := node.manager.CloneFromCheckpoint(ctx, srcVolName, cp.ID, cloneName); err != nil {
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
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

	v := node.manager.GetVolume(volName)
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
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil || v.ReadOnly() {
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
	if pv, ok := v.(*volume); ok {
		parentNextSeq = pv.layer.nextSeq.Load()
	}

	cloneName := fmt.Sprintf("%s-iso-%d", volName, sim.nextVolID)
	sim.nextVolID++
	if err := v.Clone(cloneName); err != nil {
		return
	}
	clone, err := node.manager.OpenVolume(cloneName)
	if err != nil {
		return
	}

	// Clone internally flushes parent data.
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

	// Kill the manager's background goroutines most of the time to keep the
	// synctest bubble lean. Leave them running ~5% of the time to exercise
	// the case where a crashed process's goroutines linger (e.g. stuck I/O).
	if sim.rng.Float64() > 0.05 {
		node.manager.Close(context.Background())
	}

	node.manager = nil
	node.volumes = nil
}

func (sim *Simulation) recoverNode(ctx context.Context, node *SimNode) {
	node.fs = NewSimLocalFS()
	node.alive = true
	node.volumes = nil

	node.manager = sim.newManager(sim.t.TempDir(), node.fs)
	sim.allManagers = append(sim.allManagers, node.manager)

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
	v := node.manager.GetVolume(name)
	if v == nil {
		return
	}
	vol, ok := v.(*volume)
	if !ok {
		return
	}
	actualTL := vol.layer.id
	oracleTL := sim.volumeTimelines[name]
	if actualTL != oracleTL {
		sim.t.Fatalf("TIMELINE MISMATCH on open: vol=%s oracle=%s actual=%s", name, oracleTL[:8], actualTL[:8])
	}
}

func (sim *Simulation) createVolume(ctx context.Context, node *SimNode, name string) {
	v, err := node.manager.NewVolume(loophole.CreateParams{Volume: name, Size: uint64(sim.config.DevicePages) * PageSize})
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
	_, err := node.manager.OpenVolume(name)
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

	m := sim.newManager(sim.t.TempDir(), NewSimLocalFS())
	sim.allManagers = append(sim.allManagers, m)

	// Verify ListAllVolumes contains every tracked volume.
	// Note: S3 may also contain partially-created volumes from faulted
	// opCreateVolume calls (ref written but getVolRef failed), so we only
	// check that every tracked volume is present, not the reverse.
	allNames, err := m.ListAllVolumes(ctx)
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

		v, err := m.OpenVolume(volName)
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
		scanned++
	}

	// Verify Volumes() matches the number of successfully opened volumes.
	openNames := m.Volumes()
	if len(openNames) != scanned {
		sim.t.Fatalf("Volumes() returned %d names, expected %d", len(openNames), scanned)
	}

	m.Close(ctx)

	// Structural invariant checks: verify layer reference integrity.
	sim.verifyLayerIntegrity(ctx, volNames)

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
	ref, _, err := loophole.ReadJSON[volumeRef](ctx, volRefs, name+"/index.json")
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
		defer func() {
			for _, m := range sim.allManagers {
				m.Close(context.Background())
			}
		}()
		sim.Run()
		sim.FullScan()
		events = sim.oracle.EventLog()
	})
	return events
}

func TestSimulation(t *testing.T) {
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

	nodes := simEnvInt(t, "SIM_NODES", 3)
	ops := simEnvInt(t, "SIM_OPS", 300)
	pages := simEnvInt(t, "SIM_PAGES", 256)
	timelines := simEnvInt(t, "SIM_TIMELINES", 20)
	crashRate := simEnvFloat(t, "SIM_CRASH_RATE", 0.02)
	faultRate := simEnvFloat(t, "SIM_FAULT_RATE", 0.02)

	t.Logf("seed: %d  nodes: %d  ops: %d  pages: %d  timelines: %d  crash: %.2f  faults: %.2f",
		seed, nodes, ops, pages, timelines, crashRate, faultRate)

	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, seed, SimConfig{
			NumNodes:       nodes,
			DevicePages:    pages,
			MaxTimelines:   timelines,
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
		// Defer cleanup so periodic flush goroutines are stopped even if
		// t.FailNow()/t.Fatalf() exits the goroutine via runtime.Goexit().
		// Without this, synctest detects leaked goroutines and panics with
		// "deadlock: main bubble goroutine has exited but blocked goroutines remain".
		defer func() {
			for _, m := range sim.allManagers {
				m.Close(context.Background())
			}
		}()
		sim.Run()
		sim.FullScan()
	})
}

func TestSimulationDeterminism(t *testing.T) {
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
		NumNodes:       3,
		DevicePages:    64,
		MaxTimelines:   15,
		OpsPerNode:     100,
		CrashRate:      0.02,
		FlushThreshold: 8 * PageSize,
		S3Faults: FaultConfig{
			PutFailRate:    0.02,
			GetFailRate:    0.02,
			ListFailRate:   0.01,
			DeleteFailRate: 0.01,
			PhantomPutRate: 0.02,
		},
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
	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, 1, SimConfig{
			NumNodes:       2,
			DevicePages:    64,
			MaxTimelines:   10,
			OpsPerNode:     1,
			CrashRate:      0,
			FlushThreshold: 8 * PageSize,
		})
		defer func() {
			for _, m := range sim.allManagers {
				m.Close(context.Background())
			}
		}()

		ctx := t.Context()
		sim.createVolume(ctx, sim.nodes[0], "vol-0")
		v := sim.nodes[0].manager.GetVolume("vol-0")
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
