# LSM-based storage layer for loophole

## Problem

The current block-based storage uploads one S3 object per dirty block (1-4MB).
Scattered small writes (ext4 metadata, inode tables, bitmaps, journal) dirty
many blocks, causing:

- High S3 PUT count (cost-dominant)
- Massive write amplification (4KB write → 4MB upload)
- Freeze/snapshot blocks on uploading all dirty blocks

## Proposal

Replace the per-block storage with an LSM-inspired approach adapted from Neon's
pageserver. The block device is treated as a flat array of 4KB pages. Writes are
appended to an in-memory layer and periodically flushed as batch objects to S3.
Branching and snapshots become instant metadata operations.

## Data model

```
Page address = device byte offset / 4096
Seq          = monotonically increasing write counter (uint64)
Value        = 4096 bytes (always a full page, never a diff)
```

The storage for each timeline consists of layers. Each layer covers a rectangle
in (page address, seq) space. Two types:

**Delta layer** — a sparse set of (page addr, seq, 4KB data) entries. Contains
only pages that were actually written during the seq range it covers. Stored as
a single S3 object with an index for point lookups.

**Image layer** — a dense snapshot of a contiguous page range at a specific seq.
Every page in the range has a value (or is implicitly zero). Produced by
compaction, never on the write path.

### Simplification vs Neon

Neon stores WAL records (diffs) and must replay them against a base image to
reconstruct a page. This requires a "will_init" flag, multi-record
reconstruction, and a WAL redo process.

Our block device always stores full 4KB page images. Reading a page means
finding the newest entry — no reconstruction, no replay. This eliminates Neon's
most complex code path.

## Timeline and branching

A **timeline** (terminology from Neon) is the storage backing a single volume —
an ordered set of layers plus an optional ancestor pointer. Each volume has one
active timeline. Snapshots and clones create new timelines that reference their
parent as an ancestor, forming a tree:

```go
type Timeline struct {
    id          string
    ancestor    *Timeline  // nil for root timelines
    ancestorSeq uint64     // read from ancestor only at seq <= this value
    layers      *LayerMap  // tracks all delta and image layers
    memLayer    *MemLayer  // active in-memory write buffer
    nextSeq     uint64     // monotonic write counter
}
```

**Snapshot**: a read-only point-in-time copy of the block device. The result is
a new timeline that sees the exact contents of the parent at the moment of the
snapshot — all subsequent writes to the parent are invisible to it.

Steps:

1. Record `branchSeq = timeline.nextSeq`
2. Freeze the parent's current memLayer (mark read-only, O(1))
3. Start a new empty memLayer for the parent — writes resume immediately
4. Create a child timeline: `ancestor = parent, ancestorSeq = branchSeq`
5. Write the child's `meta.json` to S3

The child timeline has zero layers of its own. All reads go through the
ancestor pointer into the parent's existing layers (frozen memLayers, deltas,
images). No data is copied or uploaded synchronously — the frozen memLayer is
flushed to S3 in the background like any other frozen memLayer.

In the current design, snapshot must upload every dirty block before completing
(blocking). Here, the snapshot returns as soon as `meta.json` is written. The
parent's writes are unblocked at step 3.

**Clone**: same as snapshot but the child timeline is writable. The child's
`nextSeq` starts at 0 — each timeline has its own independent seq space. The
parent is frozen at the branch point (or continues writing to a continuation
timeline, same as today's `freezeAndContinue`).

**Lease requirement**: creating a child of any timeline (frozen or active)
requires holding the parent's lease. This serializes child creation with GC
on the parent — without it, a GC running on a frozen timeline could delete a
layer while a concurrent child creation establishes an `ancestorSeq` that
depends on it. Lease acquisition is one S3 conditional write.

**Reading across branches**: search the child's layers first. If the page isn't
found, search the ancestor's layers restricted to `seq <= ancestorSeq`. Recurse
up the ancestor chain. This replaces `refBlockIndex` entirely. Grandchildren
(and further descendants) do not add constraints on an ancestor's GC — a
grandchild's access to timeline A is bounded by its parent B's `ancestorSeq`
on A, which is fixed at branch time.

## Write path

```
Write(offset uint64, data []byte):
    for each 4KB page overlapping [offset, offset+len):
        seq := timeline.nextSeq++
        pageAddr := pageStart / 4096
        pageData := extract or pad the 4KB page

        timeline.memLayer.Put(pageAddr, seq, pageData)
        pageCache.Put(pageAddr, pageData)    // write-through to local cache
```

The in-memory layer is backed by an append-only local file (like Neon's
EphemeralFile). Metadata is in memory:

```go
type MemLayer struct {
    file     *os.File                         // append-only data file
    index    map[uint64]memEntry              // pageAddr → latest entry
    startSeq uint64
    size     uint64                           // bytes written to file
}

type memEntry struct {
    seq       uint64
    offset    uint64  // offset in file where the 4KB page data starts
    tombstone bool    // true = page was punched (PunchHole), read as zeros
}
```

Writes are fast: append 4KB to the file, update the in-memory map. The index
stores only the latest entry per pageAddr — if a page is written multiple times
within one memLayer's lifetime, intermediate versions are discarded. This is
safe because branching freezes the memLayer, so no branch point can fall inside
an active memLayer's seq range.

When `memLayer.size` exceeds a threshold (e.g. 256MB), freeze it and start a
new one. The frozen in-memory layer is then flushed to S3 as a delta layer.

## Flush (in-memory layer → delta layer on S3)

```
1. Freeze the current memLayer (set endSeq, reject new writes)
2. Start a new memLayer for incoming writes
3. Sort entries by pageAddr
4. Serialize: header + per-page zstd-compressed values + uncompressed index
5. Upload as single S3 object: deltas/<startSeq>-<endSeq>
6. Update the in-memory layer map (add delta, remove frozen memLayer)
7. Delete the local ephemeral file
```

Multiple frozen memLayers may upload concurrently, but the in-memory layer map
update in step 6 is serialized — a delta layer is added to the layer map only
after all older frozen memLayers have been added. This prevents seq gaps in
the layer map. `layers.json` is written periodically as a checkpoint, not on
every flush.

One S3 PUT per flush, regardless of how many pages were written. 300 scattered
4KB metadata writes = one ~1.2MB object, compressed to ~100-200KB.

### Delta layer S3 format

Pages are compressed individually so that S3 range reads can fetch a single page
without downloading the entire layer.

```
Header (fixed size, uncompressed):
    magic:       [4]byte "LDLT"
    version:     uint16
    startSeq:    uint64
    endSeq:      uint64
    pageRange:   [2]uint64   // min and max pageAddr
    numEntries:  uint32
    indexOffset: uint64       // byte offset where index starts

Values section (variable):
    For each entry, a zstd-compressed 4KB page (variable length).
    Entries ordered by (pageAddr, seq).

Index section (uncompressed):
    Sorted array of (pageAddr uint64, seq uint64, valueOffset uint64, valueLen uint32, crc32 uint32).
    Enables binary search for point lookups. CRC32 covers the compressed page
    data and is checked on every read to detect corruption from partial S3
    range reads.
    For small delta layers (< 10K entries), a flat sorted array is sufficient.
    For larger ones, a B-tree index (like Neon's disk_btree) can be used.
```

The header and index are uncompressed.

### Layer index caching

On first access to a layer, download the header + index (not the values) and
cache to local disk as `layers/<timelineID>/deltas/<name>.index`. The index
for a 64K-entry delta layer is ~2MB. Since layers are immutable once written
to S3, cached indexes never need invalidation — they are deleted only when GC
deletes the layer itself.

With cached indexes, reading a page from a delta layer is:

- **Index cached locally**: local disk read + binary search. If page not found,
  zero S3 calls. If found, one S3 range GET for the compressed page data.
- **Index not cached (first access)**: 2 S3 range GETs (header + index), cached
  to disk. Then one more S3 GET if the page is found.

Without index caching, every layer probe costs 2 S3 GETs just to discover the
page isn't there. On a read that traverses an ancestor chain with many delta
layers, this compounds to dozens of S3 round trips for a single 4KB page.

For bulk reads, fetch the entire layer in one GET and decompress pages as
needed.

## Read path

```
Read(offset uint64, buf []byte):
    for each 4KB page overlapping [offset, offset+len):
        pageAddr := pageStart / 4096
        data := ReadPage(timeline, pageAddr, MaxUint64)
        copy data into buf

ReadPage(timeline, pageAddr, maxSeq) → []byte:
    // 1. Local page cache (fast path, only for unbounded reads)
    if maxSeq == MaxUint64:
        if page := pageCache.Get(timeline.id, pageAddr); page != nil:
            return page

    // 2. In-memory layer (skip if maxSeq bounds us before it)
    if entry := timeline.memLayer.Get(pageAddr); entry != nil && entry.seq <= maxSeq:
        return entry.data

    // 3. Frozen in-memory layers (newest first)
    for each frozen memLayer (newest → oldest):
        if entry := frozen.Get(pageAddr); entry != nil && entry.seq <= maxSeq:
            return entry.data

    // 4. Delta layers (newest first, skip layers with startSeq > maxSeq)
    for each delta layer where startSeq <= maxSeq (newest → oldest):
        if entry := delta.Get(pageAddr, maxSeq); entry != nil:
            pageCache.Put(timeline.id, pageAddr, entry.data)
            return entry.data

    // 5. Image layers (skip layers with seq > maxSeq)
    for each image layer covering pageAddr where seq <= maxSeq:
        if entry := image.Get(pageAddr); entry != nil:
            pageCache.Put(timeline.id, pageAddr, entry.data)
            return entry.data

    // 6. Ancestor timeline (bounded by ancestorSeq)
    if timeline.ancestor != nil:
        return ReadPage(timeline.ancestor, pageAddr, timeline.ancestorSeq)

    // 7. Page never written
    return zeroPage
```

The `maxSeq` parameter threads through every layer search. For direct reads
(not through an ancestor), `maxSeq = MaxUint64` imposes no filtering. For
ancestor reads, `maxSeq = ancestorSeq` ensures the child only sees pages
written before the branch point. Branching freezes the parent's memLayer, so
all entries at `seq <= ancestorSeq` are in frozen or flushed layers, never in
the ancestor's active memLayer.

Since every entry is a full 4KB page (not a diff), the first match is the final
answer. No reconstruction needed. This is simpler than Neon's read path.

### Layer map

The layer map is the data structure that makes the read path efficient. Instead
of linearly scanning all layers, it indexes them by (page range, seq range).

Neon uses a persistent BST indexed by page address, with versions at each LSN,
allowing O(log n) lookup of which layer covers a given (page, seq) pair.

A simpler approach for our case: since we have far fewer layers than Neon
(block device writes are coarser than per-row Postgres WAL), a sorted list of
layers with binary search on seq ranges is likely sufficient. If needed,
an interval tree or R-tree can be added later.

```go
type LayerMap struct {
    // Sorted by startSeq descending (newest first) for search.
    deltas []DeltaLayerMeta
    images []ImageLayerMeta

    // Open and frozen in-memory layers.
    memLayer       *MemLayer
    frozenMemLayers []*MemLayer
}

type DeltaLayerMeta struct {
    startSeq  uint64
    endSeq    uint64
    pageRange [2]uint64   // min, max pageAddr
    s3Key     string
}

type ImageLayerMeta struct {
    seq       uint64
    pageRange [2]uint64
    s3Key     string
}
```

## Compaction

Background process that never blocks reads or writes. Two operations:

### Delta merge

Multiple small delta layers → one larger delta layer covering the combined seq
range. Reduces layer count, improving read performance on cache miss.

```
1. Collect the set of all branch points: the ancestorSeq of every child
   timeline that references this timeline as an ancestor.
2. Select N oldest delta layers
3. Merge-sort their entries by (pageAddr, seq)
4. Deduplicate: for each pageAddr, keep the latest seq entry overall AND the
   latest seq entry at or below each branch point. For example, if branch
   points are {300, 500} and a page has entries at seq 200, 400, 600, keep
   seq 200 (latest <= 300), seq 400 (latest <= 500), and seq 600 (latest
   overall). Tombstones count as entries and must be retained — discarding
   a tombstone would expose a stale value in an older image layer.
5. Write merged delta layer to S3
6. Update layer map: remove old deltas, add merged delta
7. Delete old delta layer objects from S3 (after layer map is updated)
```

### Image layer creation

For a page range, materialize the value of every page at a pinned seq by
searching existing layers.

```
1. Pin imageSeq = timeline.nextSeq (snapshot of the current write counter)
2. Choose a page range (e.g. 0..65536, covering 256MB)
3. For each pageAddr in range:
     Read the value at seq <= imageSeq by searching layers
4. Write as image layer to S3: images/<imageSeq>__<pageStart>-<pageEnd>
5. Update layer map
6. Delta layers covering this page range with ALL entries at seq < imageSeq
   (and not needed by any branch point) can now be garbage collected
```

Pinning the seq before reading ensures that concurrent writes at seq > imageSeq
do not affect the image contents, and that GC only removes deltas that the
image fully supersedes.

Image layers make reads faster (one lookup instead of scanning deltas) and
enable garbage collection of old deltas.

### Compaction triggers

- **Layer count**: when delta layer count exceeds a threshold (e.g. 20), merge.
- **Total delta size**: when cumulative delta layer bytes exceed a threshold.
- **Image staleness**: when no image layer exists for a page range within the
  last N seq numbers, create one.

Compaction is purely an optimization. The system is correct without it — reads
just get slower as more deltas accumulate.

## Local disk budget

All local disk usage is bounded by a configurable budget. Data on local disk
falls into two categories:

**Pinned** (cannot be evicted — not yet on S3):
- Active memLayer ephemeral file: up to `flushThreshold` (e.g. 256MB)
- Frozen memLayer ephemeral files awaiting upload: up to
  `maxFrozenLayers × flushThreshold`

**Evictable** (recoverable from S3 layers):
- Page cache: individual 4KB pages, LRU eviction
- Delta/image layer cache: recently downloaded layers for read misses, LRU

Total local disk = pinned budget + evictable cache budget. Both are bounded by
configuration. When the evictable cache is full, the oldest entry is evicted
before inserting a new one.

### Write backpressure

The number of frozen memLayers is capped (e.g. `maxFrozenLayers = 2`). When
the active memLayer hits the flush threshold and needs to freeze, but all
frozen slots are occupied (uploads in progress), writes block until a flush
completes and frees a slot.

```
Write(offset, data):
    ...
    if memLayer.size >= flushThreshold:
        while len(frozenMemLayers) >= maxFrozenLayers:
            wait on flushComplete
        freeze current memLayer, start new one
```

This bounds pinned local disk to `(maxFrozenLayers + 1) × flushThreshold` and
bounds memory to the same (the in-memory indexes are small relative to the
ephemeral files). Under normal conditions, flushes complete before the next
freeze and writes never block.

### Page cache

Fixed-size LRU on local disk, replacing `openBlocks`. Stores individual 4KB
pages keyed by (timeline, pageAddr).

```go
type PageCache struct {
    dir      string
    maxBytes uint64
    // In-memory index: (timelineID, pageAddr) → offset in cache file
    index    map[pageCacheKey]uint64
    lru      *list.List
}
```

- Writes update the cache (write-through), so subsequent reads are local.
- Any cache entry can be evicted — the data is always recoverable from layers.
- Bounded by configuration. No unbounded growth like the current `openBlocks`.

For bulk sequential reads (e.g. reading a large file), the page cache can
optionally use larger readahead units — fetch a range of pages from a layer in
one operation.

### Local cache directory layout

```
<cache_dir>/
    mem/
        <startSeq>.ephemeral                    # active memLayer append-only data file
        <startSeq>.ephemeral                    # frozen memLayer(s) awaiting upload
    pages.cache                                 # page cache: fixed-size file of 4KB slots
    layers/
        <timelineID>/
            deltas/<startSeq>-<endSeq>.index    # layer index (header + index section)
            deltas/<startSeq>-<endSeq>.data     # full layer data (optional, for bulk reads)
            images/<seq>__<pStart>-<pEnd>.index  # image layer index
            images/<seq>__<pStart>-<pEnd>.data   # full image layer (optional)
```

**`mem/`**: pinned, cannot be evicted. One file per memLayer (active + frozen).
Deleted after successful flush to S3.

**`pages.cache`**: a fixed-size file divided into 4KB slots. The in-memory
index maps `(timelineID, pageAddr)` to a slot number. LRU eviction reuses
slots. Size bounded by `PageCache.maxBytes`.

**`layers/*.index`**: layer indexes cached on disk. Downloaded on first access
to a layer (2 S3 GETs: header + index). Immutable — never invalidated, evictable
under disk pressure (re-downloaded on next access). Small (~2MB per 64K-entry
layer). Cleaned up when GC deletes the layer: the lease holder deletes local
cached files when it removes a layer from the layer map. A non-lease-holder node
with a stale cached index will get a 404 on the next S3 range read, at which
point it deletes the stale cache entry and refreshes its layer map from S3.

**`layers/*.data`**: full layer data cached for read performance. LRU eviction
deletes files when the cache budget is exceeded. Any cached layer can be
re-downloaded from S3 on eviction.

## Garbage collection

Once an image layer exists at seq S for a page range, delta layers covering
the same page range with all entries at seq < S can be deleted.

**Branch-aware GC**: a delta layer can only be deleted if no child's
`ancestorSeq` falls within that layer's seq range for any page in its range.

The parent's `meta.json` stores all branch points with associated child IDs:

```json
{
    "branch_points": [
        {"child": "timeline-abc", "seq": 100},
        {"child": "timeline-def", "seq": 500}
    ]
}
```

Updated by the lease holder when creating or deleting a child. GC reads this
list to determine which seq values are pinned. When a child timeline is
deleted, its entry is removed from `branch_points`, potentially allowing GC to
reclaim layers that were previously pinned.

## S3 layout

```
timelines/<id>/
    meta.json                                              # timeline metadata
    layers.json                                            # layer map (list of all layers)
    deltas/<startSeq>-<endSeq>                             # delta layer
    images/<seq>__<pageStart>-<pageEnd>                     # image layer
```

`meta.json`:
```json
{
    "ancestor": "<parent timeline id>",
    "ancestor_seq": 12345,
    "branch_points": [
        {"child": "timeline-abc", "seq": 100},
        {"child": "timeline-def", "seq": 500}
    ],
    "created_at": "2025-01-15T10:00:00Z"
}
```

`layers.json` is a cache for fast startup — the authoritative layer set can
always be reconstructed by listing `deltas/*` and `images/*` and parsing the
seq ranges and page ranges from the key names. This means:

- If the process crashes after uploading a delta layer but before updating
  `layers.json`, the orphaned layer is discovered on next startup via S3 list.
- If `layers.json` is stale or missing, reconstruct it from the S3 listing.
- `next_seq` is recovered as `max(endSeq across all deltas) + 1`.

Update protocol for `layers.json` (write-new-first):

1. Upload new layer object(s) to S3
2. Write updated `layers.json` (new layers included, old layers still listed)
3. Delete old layer objects from S3 (if compaction/GC)

A reader that sees a stale `layers.json` may miss a recently uploaded delta
layer, but will find it on the next refresh. A reader that sees an updated
`layers.json` referencing a not-yet-deleted old layer will simply read it
(harmless). The only failure mode is a crash between step 2 and 3, which
leaves old layer objects in S3 — cleaned up on next compaction or startup.

```json
{
    "next_seq": 50000,
    "deltas": [
        {"start_seq": 100, "end_seq": 500, "pages": [0, 65535], "key": "deltas/0000000000000100-0000000000000500", "size": 1048576}
    ],
    "images": [
        {"seq": 500, "pages": [0, 65535], "key": "images/0000000000000500__0000000000000000-000000000000FFFF", "size": 4194304}
    ]
}
```

## Comparison with current design

| Aspect | Current | LSM |
|---|---|---|
| S3 PUTs for 300 scattered 4KB writes | 300 (one per block) | 1 (one delta layer) |
| Upload volume for above | 300 × 1MB = 300MB | ~100KB compressed |
| Snapshot/freeze | Upload all dirty blocks, blocking | Instant: freeze memLayer + write meta.json (flush is async) |
| Clone | Copy refBlockIndex, create child layer | Pointer to ancestor + seq |
| Read path (warm) | pread on local block file | Page cache hit |
| Read path (cold) | Download full block from S3 | 3 small S3 range reads (header, index, page) |
| Local disk usage | Unbounded (openBlocks) | Bounded (pinned memLayers + LRU cache) |
| Branching model | refBlockIndex + layer chain | Ancestor pointer + seq |

## Mapping to current design

The LSM model replaces the core storage abstraction. The key interfaces change:

**Layer → Timeline**: instead of `Layer.Read/Write`, the timeline owns the layer
stack and provides Read/Write. The `Volume` struct holds a `*Timeline` instead
of a `*Layer`.

**VolumeManager → TimelineManager**: manages timelines instead of layers.
Snapshot and clone create new timelines instead of freezing layers and creating
children.

**BlockCache → PageCache**: fixed-size LRU of 4KB pages instead of immutable
block file cache.

**Flusher → delta layer writer**: instead of uploading individual blocks, the
flusher serializes frozen in-memory layers into delta layer objects.

The FUSE/NBD frontend and the Volume API (Read/Write/PunchHole/Snapshot/Clone)
remain the same from the caller's perspective. The daemon, client, and
container storage interfaces are unaffected.

## PunchHole

PunchHole (FUSE fallocate with punch mode) zeroes a range of the block device.
Instead of writing 4KB of zeros for each page, a tombstone is recorded:

```
PunchHole(offset, length):
    for each 4KB page fully covered by [offset, offset+length):
        seq := timeline.nextSeq++
        pageAddr := pageStart / 4096
        timeline.memLayer.PutTombstone(pageAddr, seq)
        pageCache.Put(timeline.id, pageAddr, zeroPage)
```

**Representation**: in the memLayer, a tombstone is a `memEntry` with
`tombstone = true` and no data written to the ephemeral file. In the delta
layer on S3, a tombstone has `valueLen = 0` in the index entry — no compressed
page data is stored. This avoids wasting 4KB per punched page.

**Read path**: when the first match for a page is a tombstone, return zeros
immediately. Do not continue searching older layers — the tombstone explicitly
overwrites any previous value.

**Delta merge**: tombstones are retained. Discarding a tombstone would expose a
stale non-zero value in an older image or delta layer.

**Image layer creation**: when a tombstone is the latest entry for a page, the
image layer records the page as zero (implicitly, by omitting it — unrecorded
pages in an image layer are zero).

**GC**: once an image layer at seq S supersedes the tombstone (S > tombstone's
seq), the tombstone can be garbage collected — the image layer already records
the page as zero.

## CopyFrom (CoW)

CoW between timelines that share an ancestor is free — both read from the same
ancestor layers, no data copy needed. Cross-timeline CopyFrom between unrelated
timelines goes through the normal write path (read pages from source, write to
destination).

## Durability and crash recovery

**Durability guarantee**: writes are durable only after an explicit Flush (ext4
fsync → our Flush handler). Flush freezes the current memLayer and uploads it
as a delta layer to S3. Returns when the S3 PUT completes. Same guarantee as
today, one S3 PUT instead of N.

**Writes between flushes**: acknowledged immediately but only stored in the
local memLayer ephemeral file. If the process crashes, these writes are lost.
This matches the current behavior — the block device provides write-back
semantics, not write-through.

**Crash recovery on startup**:

1. Delete `mem/*.ephemeral` — unflushed writes are lost, in-memory index is
   gone (same as current block-based design)
2. Delete `pages.cache` — may contain pages from unflushed writes that aren't
   in S3 (keeping them would return data that was supposed to be lost)
3. Keep `layers/` — immutable copies of S3 objects, always valid, avoids
   re-downloading on startup
4. Reconstruct layer map from S3 listing (`deltas/*`, `images/*`)
5. Load `meta.json` for ancestor/branch info
6. Start with an empty memLayer and empty page cache (fills on demand from
   reads)

## Deterministic simulation testing

The LSM engine is tested using Go's `testing/synctest` for deterministic
execution with simulated time. All I/O is injected through interfaces, allowing
fault injection and crash simulation in fully reproducible tests.

### Injected interfaces

The timeline engine depends on three interfaces, all injected at construction:

```go
// S3-compatible object store.
type ObjectStore interface {
    Get(key string) ([]byte, error)
    GetRange(key string, offset, length int64) ([]byte, error)
    Put(key string, data []byte) error
    List(prefix string) ([]string, error)
    Delete(key string) error
}

// Local filesystem for ephemeral files and caches.
type LocalFS interface {
    Create(path string) (File, error)
    Open(path string) (File, error)
    Remove(path string) error
    ReadDir(path string) ([]string, error)
}

// Clock for timers and timeouts (satisfied by synctest's fake clock).
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}
```

Production uses real S3/disk/time. Tests inject simulation implementations.

### SimObjectStore

In-memory S3 simulation with configurable fault injection:

```go
type SimObjectStore struct {
    objects  map[string][]byte
    mu       sync.Mutex

    // Fault injection, checked on every operation.
    faults   FaultConfig
}

type FaultConfig struct {
    // Per-operation failure probability (0.0 - 1.0).
    putFailRate    float64
    getFailRate    float64
    // Per-operation latency (advanced via synctest's fake clock).
    putLatency     time.Duration
    getLatency     time.Duration
    // Simulate partial writes: Put succeeds in S3 but returns error to caller.
    // The object exists but the caller thinks it failed.
    phantomPutRate float64
}
```

Latency is implemented via `time.Sleep` — under synctest, this advances fake
time instantly when all goroutines are blocked, so tests run fast while still
exercising concurrent timing behavior.

Faults are seeded from a deterministic PRNG so tests are reproducible. The seed
is logged on failure for replay.

### SimLocalFS

In-memory filesystem that supports crash simulation:

```go
type SimLocalFS struct {
    files    map[string][]byte
    mu       sync.Mutex
}
```

**Crash simulation**: `SimLocalFS.Crash()` discards all files in `mem/` and
`pages.cache` (simulating the crash recovery procedure). Files in `layers/`
survive. After crash, the timeline engine is reconstructed from
`SimObjectStore` state — same as real crash recovery.

### Simulation test

The simulation test runs many concurrent simulated nodes against a shared
`SimObjectStore`, each with its own `SimLocalFS`. Nodes independently perform
random operations (writes, reads, flushes, snapshots, clones, compaction, GC)
and randomly crash and recover. All behavior is driven by a deterministic PRNG
seeded from a single seed, logged on failure for exact replay.

```go
func TestSimulation(t *testing.T) {
    seed := time.Now().UnixNano()
    t.Logf("seed: %d", seed)

    synctest.Run(func() {
        sim := NewSimulation(seed, SimConfig{
            NumNodes:       5,
            DevicePages:    16384,       // 64MB device
            MaxTimelines:   20,
            OpsPerNode:     1000,
            CrashRate:      0.02,        // 2% chance of crash per tick
            S3Faults:       FaultConfig{ ... },
        })
        sim.Run()
    })
}
```

**Simulation structure**:

```go
type Simulation struct {
    rng       *rand.Rand           // deterministic PRNG from seed
    store     *SimObjectStore      // shared S3, all nodes see the same objects
    nodes     []*SimNode           // independent instances
    oracle    *Oracle              // tracks expected state for verification
}

type SimNode struct {
    id        string
    fs        *SimLocalFS          // node-local filesystem, lost on crash
    timelines map[string]*Timeline // active timelines on this node
    leases    map[string]bool      // timelines this node holds leases for
    alive     bool                 // false after crash, until recover
}
```

**Operation selection**: each tick, each alive node picks a random operation
weighted by the PRNG:

| Operation | Weight | Description |
|---|---|---|
| Write | 40% | Write 1-100 random pages to a random owned timeline |
| Read | 25% | Read random pages, verify against oracle |
| Flush | 10% | Flush a random owned timeline |
| Snapshot | 8% | Snapshot a random owned timeline |
| Clone | 5% | Clone a timeline, acquire lease on the new timeline |
| Compaction | 5% | Run delta merge or image creation on a random timeline |
| GC | 3% | Run GC on a random timeline |
| Crash | 2% | Crash this node (lose local state) |
| Recover | 2% | Recover a crashed node from S3 |

**Oracle**: the oracle is a simple reference model that tracks what each
timeline should contain at each point. It records every flushed write (page
address → data) per timeline, respecting ancestor chains. After every read, the
simulation verifies the result matches the oracle. The oracle does not track
unflushed writes — after a crash, it reflects the last flushed state.

```go
type Oracle struct {
    // Per-timeline: pageAddr → expected data (after last flush).
    // Unflushed writes are tracked separately and discarded on crash.
    flushed   map[string]map[uint64][]byte   // timelineID → pageAddr → data
    unflushed map[string]map[uint64][]byte   // timelineID → pageAddr → data
    ancestors map[string]AncestorRef         // timelineID → (parentID, seq)
}

// Expected read result: search timeline's flushed pages, then ancestor chain.
// If the node hasn't crashed since writing, also check unflushed pages.
func (o *Oracle) ExpectedRead(timelineID string, pageAddr uint64, alive bool) []byte
```

**Crash and recovery**: when a node crashes, its `SimLocalFS` discards
ephemeral files and page cache. Its in-memory timeline state is destroyed. The
oracle discards that node's unflushed writes. On recovery, the node
reconstructs timelines from `SimObjectStore` and the oracle verifies all
flushed data is still readable.

**Lease coordination**: nodes acquire and release leases through the
`SimObjectStore` (conditional writes on a lease key). Only one node holds a
lease for a given timeline at a time. Operations that require the lease
(write, flush, snapshot, clone, compaction, GC) are skipped if the node
doesn't hold the lease. Crashed nodes release all leases.

**Invariant checks** (verified continuously):

- Every read matches the oracle's expected value
- Snapshot reads never see post-branch writes
- After crash and recovery, all flushed data is intact
- No two alive nodes hold the same lease
- Branch points in `meta.json` match existing child timelines
- No seq gaps in any timeline's layer map
- GC never deletes a layer needed by a branch point

**Reproducing failures**: on assertion failure, the test logs the seed. Rerun
with that seed to reproduce the exact same sequence of operations, timing, and
faults:

```
go test -run TestSimulation -seed 1709312847562
```
