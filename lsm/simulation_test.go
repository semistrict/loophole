package lsm

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"testing"
	"testing/synctest"

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
	// Deleted volume names (removed from volumeTimelines).
	deletedVolumes map[string]bool
	// Parent timeline ID → set of child timeline IDs.
	timelineChildren map[string]map[string]bool
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
	}

	// Create nodes.
	for i := range config.NumNodes {
		node := &SimNode{
			id:    fmt.Sprintf("node-%d", i),
			fs:    NewSimLocalFS(),
			alive: true,
		}
		node.manager = NewManager(
			store.Store(),
			t.TempDir(),
			Config{
				FlushThreshold:  config.FlushThreshold,
				MaxFrozenLayers: 2,
				PageCacheBytes:  PageSize, // small for simulation
			},
			nil,
			node.fs,
			RealClock{}, // time.Now() works fine under synctest
		)
		sim.allManagers = append(sim.allManagers, node.manager)
		sim.nodes = append(sim.nodes, node)
	}

	return sim
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
			if err := v.Flush(ctx); err == nil {
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
		{5, sim.opCompact},
		{3, sim.opCreateVolume},
		{2, sim.opFreeze},
		{2, sim.opCopyFrom},
		{2, sim.opDeleteVolume},
		{2, sim.opCloseVolume},
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
	maxPage := uint64(sim.config.DevicePages)

	wrote := false
	for range numPages {
		pageAddr := sim.rng.Uint64N(maxPage)
		data := make([]byte, PageSize)
		rand.Read(data)

		offset := pageAddr * PageSize
		err := v.Write(ctx, data, offset)

		// Always record: for full-page writes, data enters the memLayer
		// before auto-flush runs. If Write returns an error it's from
		// auto-flush, not from the put — the data is in a frozen layer
		// and will be persisted by the next successful Flush.
		sim.oracle.RecordWrite(node.id, timelineID, pageAddr, data)
		wrote = true

		if err != nil {
			break
		}
	}

	if !wrote {
		return
	}

	if err := v.Flush(ctx); err != nil {
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
	maxPage := uint64(sim.config.DevicePages)
	pageAddr := sim.rng.Uint64N(maxPage)

	// Pick a random sub-page offset and length (1-2048 bytes).
	writeLen := sim.rng.IntN(2048) + 1
	maxOff := PageSize - writeLen
	writeOff := sim.rng.IntN(maxOff + 1)

	data := make([]byte, writeLen)
	rand.Read(data)

	// Compute the expected full-page result by applying the partial write
	// to the oracle's current page state. This avoids needing a read-back
	// from the volume (which could fail due to S3 faults).
	expected := sim.oracle.ExpectedRead(node.id, timelineID, pageAddr)
	copy(expected[writeOff:writeOff+writeLen], data)

	offset := pageAddr*PageSize + uint64(writeOff)
	if err := v.Write(ctx, data, offset); err != nil {
		return
	}

	sim.oracle.RecordWrite(node.id, timelineID, pageAddr, expected)
	if err := v.Flush(ctx); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)
}

// reportMismatch dumps both oracle history and timeline layer state for a
// mismatched page, then fatals. This is the single diagnostic entry point
// for all mismatch types (inline reads and full scan).
func (sim *Simulation) reportMismatch(ctx context.Context, vol loophole.Volume, volName, nodeID, timelineID string, pageAddr uint64, got, expected []byte) {
	isZero := func(b []byte) bool {
		for _, v := range b {
			if v != 0 {
				return false
			}
		}
		return true
	}

	sim.t.Logf("=== MISMATCH: vol=%s tl=%s page=%d gotZero=%v expZero=%v ===",
		volName, timelineID, pageAddr, isZero(got), isZero(expected))
	sim.t.Logf("got:      %x...", got[:32])
	sim.t.Logf("expected: %x...", expected[:32])

	// Oracle history: full lifecycle of this page.
	sim.t.Log(sim.oracle.PageHistory(timelineID, pageAddr))

	// Timeline layer inspector: what each layer sees.
	if v, ok := vol.(*volume); ok {
		sim.t.Log(v.timeline.DebugPage(ctx, pageAddr, math.MaxUint64))
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

	maxPage := uint64(sim.config.DevicePages)
	pageAddr := sim.rng.Uint64N(maxPage)

	buf := make([]byte, PageSize)
	_, err := v.Read(ctx, buf, pageAddr*PageSize)
	if err != nil {
		// S3 faults can cause read failures. Expected.
		return
	}

	timelineID := sim.volumeTimelines[volName]
	if !sim.oracle.VerifyRead(node.id, timelineID, pageAddr, buf) {
		expected := sim.oracle.ExpectedRead(node.id, timelineID, pageAddr)
		sim.reportMismatch(ctx, v, volName, node.id, timelineID, pageAddr, buf, expected)
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

	err := v.Flush(ctx)
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
	if err := v.Flush(ctx); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	snapName := fmt.Sprintf("%s-snap-%d", volName, sim.nextVolID)
	sim.nextVolID++

	err := v.Snapshot(ctx, snapName)
	if err != nil {
		return
	}

	// The snapshot is a child timeline. We need to figure out its ID.
	// Read the volume ref from the store.
	ref, err := sim.getVolRef(ctx, snapName)
	if err != nil {
		return
	}

	sim.volumeTimelines[snapName] = ref.TimelineID
	sim.timelineIDs = append(sim.timelineIDs, ref.TimelineID)
	sim.oracle.RecordBranch(ref.TimelineID, parentTL, 0)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.TimelineID] = true
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
	if err := v.Flush(ctx); err != nil {
		return
	}
	parentTL := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, parentTL)

	clone, err := v.Clone(ctx, cloneName)
	if err != nil {
		return
	}

	// Read the clone's volume ref.
	ref, err := sim.getVolRef(ctx, cloneName)
	if err != nil {
		return
	}

	sim.volumeTimelines[cloneName] = ref.TimelineID
	sim.timelineIDs = append(sim.timelineIDs, ref.TimelineID)
	sim.leases[cloneName] = node.id
	node.volumes = append(node.volumes, cloneName)
	sim.oracle.RecordBranch(ref.TimelineID, parentTL, 0)

	if sim.timelineChildren[parentTL] == nil {
		sim.timelineChildren[parentTL] = make(map[string]bool)
	}
	sim.timelineChildren[parentTL][ref.TimelineID] = true

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
	maxPage := uint64(sim.config.DevicePages)

	// Punch 1-5 contiguous pages.
	startPage := sim.rng.Uint64N(maxPage)
	numPages := uint64(sim.rng.IntN(5) + 1)
	if startPage+numPages > maxPage {
		numPages = maxPage - startPage
	}

	offset := startPage * PageSize
	length := numPages * PageSize
	if err := v.PunchHole(ctx, offset, length); err != nil {
		return
	}

	// Record in oracle, then flush.
	for pg := startPage; pg < startPage+numPages; pg++ {
		sim.oracle.RecordPunchHole(node.id, timelineID, pg)
	}
	if err := v.Flush(ctx); err != nil {
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
	if err := v.Flush(ctx); err != nil {
		return
	}
	timelineID := sim.volumeTimelines[volName]
	sim.oracle.RecordFlush(node.id, timelineID)

	vol.timeline.Compact(ctx)
}

func (sim *Simulation) opCreateVolume(ctx context.Context, node *SimNode) {
	// Limit total volumes to keep the simulation tractable.
	if len(sim.volumeTimelines) >= sim.config.MaxTimelines {
		return
	}
	name := fmt.Sprintf("vol-%d", sim.nextVolID)
	sim.nextVolID++

	v, err := node.manager.NewVolume(ctx, name, uint64(sim.config.DevicePages)*PageSize)
	if err != nil {
		return // S3 faults can cause creation failures
	}
	_ = v

	ref, err := sim.getVolRef(ctx, name)
	if err != nil {
		return
	}

	sim.volumeTimelines[name] = ref.TimelineID
	sim.timelineIDs = append(sim.timelineIDs, ref.TimelineID)
	sim.leases[name] = node.id
	node.volumes = append(node.volumes, name)
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

	if err := v.Freeze(ctx); err != nil {
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

	maxPage := uint64(sim.config.DevicePages)
	numPages := uint64(sim.rng.IntN(10) + 1)
	if numPages > maxPage {
		numPages = maxPage
	}

	srcStart := sim.rng.Uint64N(maxPage - numPages + 1)
	dstStart := sim.rng.Uint64N(maxPage - numPages + 1)

	srcOff := srcStart * PageSize
	dstOff := dstStart * PageSize
	length := numPages * PageSize

	srcTL := sim.volumeTimelines[srcName]
	dstTL := sim.volumeTimelines[dstName]

	copied, err := dst.CopyFrom(ctx, src, srcOff, dstOff, length)

	// Record in oracle: pages that were actually copied.
	pagesCopied := copied / PageSize
	for pg := uint64(0); pg < pagesCopied; pg++ {
		expected := sim.oracle.ExpectedRead(node.id, srcTL, srcStart+pg)
		sim.oracle.RecordWrite(node.id, dstTL, dstStart+pg, expected)
	}

	if err != nil {
		return
	}

	if err := dst.Flush(ctx); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, dstTL)
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
	if err := v.Flush(ctx); err != nil {
		return
	}
	sim.oracle.RecordFlush(node.id, timelineID)

	// ReleaseRef → destroy → closeVolume when refcount hits zero.
	v.ReleaseRef(ctx)

	// Remove from node's volume list and release lease.
	node.volumes = append(node.volumes[:idx], node.volumes[idx+1:]...)
	delete(sim.leases, volName)
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

	node.manager = NewManager(
		sim.store.Store(),
		sim.t.TempDir(),
		Config{
			FlushThreshold:  sim.config.FlushThreshold,
			MaxFrozenLayers: 2,
			PageCacheBytes:  PageSize,
		},
		nil,
		node.fs,
		RealClock{},
	)
	sim.allManagers = append(sim.allManagers, node.manager)

	// Re-acquire a volume if available.
	for volName := range sim.volumeTimelines {
		if _, held := sim.leases[volName]; !held {
			sim.acquireVolume(ctx, node, volName)
			break
		}
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
	actualTL := vol.timeline.id
	oracleTL := sim.volumeTimelines[name]
	if actualTL != oracleTL {
		sim.t.Fatalf("TIMELINE MISMATCH on open: vol=%s oracle=%s actual=%s", name, oracleTL[:8], actualTL[:8])
	}
	vol.timeline.debugLog = func(msg string) {
		sim.t.Logf("[%s/%s] %s", node.id, name, msg)
	}
}

func (sim *Simulation) createVolume(ctx context.Context, node *SimNode, name string) {
	v, err := node.manager.NewVolume(ctx, name, uint64(sim.config.DevicePages)*PageSize)
	if err != nil {
		sim.t.Fatalf("create volume %s: %v", name, err)
	}
	_ = v

	ref, err := sim.getVolRef(ctx, name)
	if err != nil {
		sim.t.Fatalf("get vol ref %s: %v", name, err)
	}

	sim.volumeTimelines[name] = ref.TimelineID
	sim.timelineIDs = append(sim.timelineIDs, ref.TimelineID)
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

	m := NewManager(
		sim.store.Store(),
		sim.t.TempDir(),
		Config{
			FlushThreshold:  sim.config.FlushThreshold,
			MaxFrozenLayers: 2,
			PageCacheBytes:  16 * PageSize,
		},
		nil,
		NewSimLocalFS(),
		RealClock{},
	)
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
	for volName := range sim.volumeTimelines {
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
	for volName, timelineID := range sim.volumeTimelines {
		// Verify that the oracle's timeline ID matches what's actually in S3.
		ref, refErr := sim.getVolRef(ctx, volName)
		if refErr != nil {
			sim.t.Logf("full scan: cannot read vol ref for %s: %v", volName, refErr)
			continue
		}
		if ref.TimelineID != timelineID {
			sim.t.Fatalf("full scan: vol %s timeline mismatch: oracle=%s s3=%s", volName, timelineID, ref.TimelineID)
		}

		v, err := m.OpenVolume(ctx, volName)
		if err != nil {
			// Volume may have been created during a fault window and
			// partially written. Skip unloadable volumes.
			continue
		}

		maxPage := uint64(sim.config.DevicePages)
		for pageAddr := uint64(0); pageAddr < maxPage; pageAddr++ {
			buf := make([]byte, PageSize)
			_, err := v.Read(ctx, buf, pageAddr*PageSize)
			if err != nil {
				sim.t.Fatalf("full scan read error: vol=%s page=%d: %v", volName, pageAddr, err)
			}

			// Fresh manager — no node context. Maybe-flushed data from crashed
			// nodes may or may not have survived, so use the crash-tolerant check.
			if !sim.oracle.VerifyReadAfterCrash(timelineID, pageAddr, buf) {
				expected := sim.oracle.ExpectedRead("", timelineID, pageAddr)
				sim.reportMismatch(ctx, v, volName, "", timelineID, pageAddr, buf, expected)
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
		rand.Read(buf[:])
		seed = binary.LittleEndian.Uint64(buf[:])
	}
	t.Logf("seed: %d", seed)

	synctest.Test(t, func(t *testing.T) {
		sim := NewSimulation(t, seed, SimConfig{
			NumNodes:       3,
			DevicePages:    256, // 1MB device (small for speed)
			MaxTimelines:   20,
			OpsPerNode:     300,
			CrashRate:      0.02,
			FlushThreshold: 8 * PageSize, // triggers auto-flush after ~8 pages
			S3Faults: FaultConfig{
				PutFailRate: 0.02,
				GetFailRate: 0.02,
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
