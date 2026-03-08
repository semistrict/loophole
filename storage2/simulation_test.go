package storage2

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/semistrict/loophole"
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
	FlushInterval  time.Duration // periodic auto-flush interval; 0 = default (5ms under synctest)
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

func (sim *Simulation) newManager(cacheDir string, fs LocalFS) *Manager {
	flushInterval := sim.config.FlushInterval
	if flushInterval == 0 {
		flushInterval = 5 * time.Millisecond // small interval so periodic flush fires under synctest
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
			FlushThreshold:  sim.config.FlushThreshold,
			MaxFrozenTables: 2,
			FlushInterval:   flushInterval,
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
		// This creates a race between periodic auto-flush and explicit
		// operations — a real production scenario.
		time.Sleep(1 * time.Millisecond)

		if tick%50 == 0 {
			sim.t.Logf("tick %d: %d volumes, %d timelines", tick, len(sim.volumeTimelines), len(sim.timelineIDs))
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
		{5, sim.opCompact},
		{3, sim.opCreateVolume},
		{2, sim.opFreeze},
		{2, sim.opCopyFrom},
		{2, sim.opDeepClone},
		{2, sim.opCloneFromSnapshot},
		{3, sim.opVerify},
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

	err := v.Snapshot(snapName)
	if err != nil {
		return
	}

	// The snapshot is a child timeline. We need to figure out its ID.
	// Read the volume ref from the store.
	ref, err := sim.getVolRef(ctx, snapName)
	if err != nil {
		return
	}

	sim.volumeTimelines[snapName] = ref.LayerID
	sim.timelineIDs = append(sim.timelineIDs, ref.LayerID)
	sim.oracle.RecordBranch(ref.LayerID, parentTL, branchSeq)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.LayerID] = true
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

	clone, err := v.Clone(cloneName)
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
	clone, err := v.Clone(cloneName)
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

func (sim *Simulation) opCompact(ctx context.Context, node *SimNode) {
	volName := sim.pickOwnedVolume(node)
	if volName == "" {
		return
	}

	v := node.manager.GetVolume(volName)
	if v == nil {
		return
	}

	vol, ok := v.(*volume)
	if !ok {
		return
	}

	// Flush first so oracle is consistent.
	if err := v.Flush(); err != nil {
		return
	}
	timelineID := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, timelineID)

	_ = vol.layer.CompactL0(ctx)

	// Post-compaction verification: read all pages and check against oracle.
	// Compaction rewrites layers; this catches data loss or corruption.
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
			sim.t.Logf("POST-COMPACT MISMATCH vol=%s", volName)
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

	v, err := node.manager.NewVolume(ctx, name, uint64(sim.config.DevicePages)*PageSize, "")
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

	clone, err := v.Clone(cloneName)
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
	_ = clone
}

func (sim *Simulation) opCloneFromSnapshot(ctx context.Context, node *SimNode) {
	// Find a read-only (snapshot) volume that isn't leased.
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
		// Check if it's a snapshot by reading the ref.
		ref, err := sim.getVolRef(ctx, name)
		if err != nil {
			continue
		}
		if ref.ReadOnly {
			snapName = name
			break
		}
	}
	if snapName == "" {
		return
	}

	// Open the snapshot temporarily.
	snapVol, err := node.manager.OpenVolume(ctx, snapName)
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

	clone, err := snapVol.Clone(cloneName)
	if err != nil {
		// Close the snapshot.
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

	// Abandon the manager — a real crash doesn't get a clean Close.
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
	vol.layer.debugLog = func(msg string) {
		sim.t.Logf("[%s/%s] %s", node.id, name, msg)
	}
}

func (sim *Simulation) createVolume(ctx context.Context, node *SimNode, name string) {
	v, err := node.manager.NewVolume(ctx, name, uint64(sim.config.DevicePages)*PageSize, "")
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
	_, err := node.manager.OpenVolume(ctx, name)
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

		v, err := m.OpenVolume(ctx, volName)
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

	sim.t.Logf("full scan: verified %d volumes, %d pages each", scanned, sim.config.DevicePages)
}

func (sim *Simulation) getVolRef(ctx context.Context, name string) (volumeRef, error) {
	volRefs := sim.store.Store().At("volumes")
	ref, _, err := loophole.ReadJSON[volumeRef](ctx, volRefs, name)
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
