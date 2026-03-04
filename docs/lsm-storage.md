# LSM-based storage layer for loophole

## Problem

The current block-based storage uploads one S3 object per dirty block (1-4MB).
Scattered small writes (ext4 metadata, inode tables, bitmaps, journal) dirty
many blocks, causing:

- High S3 PUT count (cost-dominant)
- Massive write amplification (64KB write → 4MB upload)
- Freeze/snapshot blocks on uploading all dirty blocks

## Proposal

Replace the per-block storage with an LSM-inspired approach adapted from Neon's
pageserver. The block device is treated as a flat array of 64KB pages. Writes are
appended to an in-memory layer and periodically flushed as batch objects to S3.
Branching and snapshots become instant metadata operations.

## Data model

```
Page address = device byte offset / 65536
Seq          = monotonically increasing write counter (uint64)
Value        = 65536 bytes (always a full page, never a diff)
```

The storage for each timeline consists of layers. Each layer covers a rectangle
in (page address, seq) space. Two types:

**Delta layer** — a sparse set of (page addr, seq, 64KB data) entries. Contains
only pages that were actually written during the seq range it covers. Stored as
a single S3 object with an index for point lookups.

**Image layer** — a dense snapshot of a contiguous page range at a specific seq.
Every page in the range has a value (or is implicitly zero). Produced by
compaction, never on the write path.

### Simplification vs Neon

Neon stores WAL records (diffs) and must replay them against a base image to
reconstruct a page. This requires a "will_init" flag, multi-record
reconstruction, and a WAL redo process.

Our block device always stores full 64KB page images. Reading a page means
finding the newest entry — no reconstruction, no replay. This eliminates Neon's
most complex code path.

## Timeline and branching

A **timeline** (terminology from Neon) is the storage backing a single volume —
an ordered set of layers plus an optional ancestor pointer. Each volume has one
active timeline. Snapshots and clones create new timelines that reference their
parent as an ancestor, forming a tree:

A Timeline holds an ID, an optional ancestor pointer (nil for root timelines),
an `ancestorSeq` (read from ancestor only at seq <= this value), a LayerMap
(tracks all delta and image layers), the active MemLayer write buffer, and a
monotonic `nextSeq` write counter. See `lsm/timeline.go`.

**Snapshot**: a read-only point-in-time copy of the block device. The result is
a new timeline that sees the exact contents of the parent at the moment of the
snapshot — all subsequent writes to the parent are invisible to it.

Steps:

1. Record `branchSeq = timeline.nextSeq`
2. Freeze the parent's current memLayer (mark read-only, O(1))
3. Start a new empty memLayer for the parent
4. Flush all frozen memLayers to S3 as delta layers (synchronous)
5. Create a child timeline: `ancestor = parent, ancestorSeq = branchSeq`
6. Write the child's `meta.json` to S3

The flush in step 4 is synchronous because the child opens the parent as an
ancestor from S3. If frozen layers were not uploaded first, the child would
be unable to see that data. After the flush completes, the child timeline has
zero layers of its own — all reads go through the ancestor pointer into the
parent's layers on S3.

**Clone**: same as snapshot but the child timeline is writable. The child's
`nextSeq` starts at 0 — each timeline has its own independent seq space. The
parent is frozen at the branch point (or continues writing to a continuation
timeline, same as today's `freezeAndContinue`).

**Lease requirement**: creating a child of any timeline (frozen or active)
requires holding the parent's lease. This serializes child creation with GC
on the parent — without it, a GC running on a frozen timeline could delete a
layer while a concurrent child creation establishes an `ancestorSeq` that
depends on it. Lease acquisition is a read-modify-CAS-write cycle on `meta.json`
(via `ModifyJSON`).

**Reading across branches**: search the child's layers first. If the page isn't
found, search the ancestor's layers restricted to `seq <= ancestorSeq`. Recurse
up the ancestor chain. This replaces `refBlockIndex` entirely. Grandchildren
(and further descendants) do not add constraints on an ancestor's GC — a
grandchild's access to timeline A is bounded by its parent B's `ancestorSeq`
on A, which is fixed at branch time.

## Write path

```
Write(offset uint64, data []byte):
    for each 64KB page overlapping [offset, offset+len):
        seq := timeline.nextSeq++
        pageAddr := pageStart / 65536
        pageData := extract or pad the 64KB page

        timeline.memLayer.Put(pageAddr, seq, pageData)
```

The in-memory layer (see `lsm/memlayer.go`) is backed by a fixed-size
memory-mapped file. Page data lives in the mmap region (off-heap, zero GC
pressure). An in-memory index maps page addresses to fixed-size slots within
the mmap. A bump allocator (`nextSlot`) assigns slots; if a page already exists,
its slot is reused (in-place overwrite, no size growth).

Writes are a `copy()` into the mmap region — no syscalls. This eliminates the
append-only amplification where repeated writes to the same page each appended
another 64KB copy. Reads return a direct slice into the mmap region — zero
allocation, zero syscalls.

The index stores only the latest entry per pageAddr — if a page is written
multiple times within one memLayer's lifetime, intermediate versions are
discarded. This is safe because branching freezes the memLayer, so no branch
point can fall inside an active memLayer's seq range.

Sub-page writes (e.g. the kernel sending 4KB writes for a 64KB page) require
a read-modify-write cycle. A striped lock array `pageLocks [256]sync.Mutex`
serializes concurrent writes to the same page, preventing lost updates where
two writers each read the old page, apply their chunk, and the last writer
silently drops the other's data. The lock is indexed by `pageAddr % 256`.

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
7. Cleanup: munmap + close + delete the backing file
```

Multiple frozen memLayers may upload concurrently, but the in-memory layer map
update in step 6 is serialized — a delta layer is added to the layer map only
after all older frozen memLayers have been added. This prevents seq gaps in
the layer map. `layers.json` is written after every successful delta layer
upload.

One S3 PUT per flush, regardless of how many pages were written. 300 scattered
64KB metadata writes = one ~1.2MB object, compressed to ~100-200KB.

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
    For each entry, a zstd-compressed 64KB page (variable length).
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

On first access to a layer, the full blob is downloaded and cached to local
disk at `<cacheDir>/<key>` (e.g. `<cacheDir>/deltas/0000...0100-0000...0500`).
Since layers are immutable once written to S3, cached files never need
invalidation — they are deleted only when GC deletes the layer itself.

With a cached layer, reading a page is a local disk read + binary search on
the index section. If the page isn't found, zero S3 calls. Without caching,
every layer probe costs S3 GETs. On a read that traverses an ancestor chain
with many delta layers, this compounds to dozens of S3 round trips for a
single 64KB page.

## Read path

```
Read(offset uint64, buf []byte):
    for each 64KB page overlapping [offset, offset+len):
        pageAddr := pageStart / 65536
        data := ReadPage(timeline, pageAddr, MaxUint64)
        copy data into buf

ReadPage(timeline, pageAddr, beforeSeq) → []byte:
    // 1. Local page cache (fast path, only for unbounded reads)
    if beforeSeq == MaxUint64:
        if page := pageCache.Get(timeline.id, pageAddr); page != nil:
            return page

    // 2. In-memory layer (skip if beforeSeq bounds us before it)
    if entry := timeline.memLayer.Get(pageAddr); entry != nil && entry.seq < beforeSeq:
        return entry.data

    // 3. Frozen in-memory layers (newest first)
    for each frozen memLayer (newest → oldest):
        if entry := frozen.Get(pageAddr); entry != nil && entry.seq < beforeSeq:
            return entry.data

    // 4. Delta layers (newest first, skip layers with startSeq >= beforeSeq)
    for each delta layer where startSeq < beforeSeq (newest → oldest):
        if entry := delta.Get(pageAddr, beforeSeq); entry != nil:
            pageCache.Put(timeline.id, pageAddr, entry.data)
            return entry.data

    // 5. Image layers (skip layers with seq >= beforeSeq)
    for each image layer covering pageAddr where seq < beforeSeq:
        if entry := image.Get(pageAddr); entry != nil:
            pageCache.Put(timeline.id, pageAddr, entry.data)
            return entry.data

    // 6. Ancestor timeline (bounded by ancestorSeq)
    if timeline.ancestor != nil:
        return ReadPage(timeline.ancestor, pageAddr, timeline.ancestorSeq)

    // 7. Page never written
    return zeroPage
```

The `beforeSeq` parameter threads through every layer search as an exclusive
upper bound. For direct reads (not through an ancestor), `beforeSeq = MaxUint64`
imposes no filtering. For ancestor reads, `beforeSeq = ancestorSeq` ensures
the child only sees pages written before the branch point. Branching freezes
the parent's memLayer, so all entries at `seq < ancestorSeq` are in frozen or
flushed layers, never in the ancestor's active memLayer.

Since every entry is a full 64KB page (not a diff), the first match is the final
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

The LayerMap (see `lsm/layer.go`) stores delta layers sorted by startSeq
ascending. The read path iterates in reverse for newest-first traversal.
Each delta has a (startSeq, endSeq, pageRange, s3Key); each image has a
(seq, pageRange, s3Key).

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
4. Write as image layer to S3: images/<imageSeq>
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
- Active memLayer mmap file: up to `flushThreshold` (e.g. 256MB)
- Frozen memLayer mmap files awaiting upload: up to
  `maxFrozenLayers × flushThreshold`

**Evictable** (recoverable from S3 layers):
- Page cache: individual 64KB pages, random eviction
- Delta/image layer cache: recently downloaded layers for read misses, random eviction

Total local disk = pinned budget + evictable cache budget. Both are bounded by
configuration. When the evictable cache is full, a random entry is evicted
before inserting a new one.

### Write backpressure

The number of frozen memLayers is capped (e.g. `maxFrozenLayers = 2`). When
the frozen layer count reaches the cap, the write path flushes them inline
before proceeding:

```
Write(offset, data):
    ...
    // Backpressure: if frozen layers are at capacity, flush them now.
    if len(frozenMemLayers) >= maxFrozenLayers:
        flushFrozenLayers()

    if memLayer.size >= flushThreshold:
        freeze current memLayer, start new one
```

This bounds pinned local disk to `(maxFrozenLayers + 1) × flushThreshold` and
bounds memory to the same (the in-memory indexes are small relative to the
ephemeral files). Under normal conditions, flushes complete before the next
freeze and writes never block.

### Page cache

Fixed-size on local disk, replacing `openBlocks`. Stores individual 64KB
pages keyed by (timeline, pageAddr).

The PageCache (see `lsm/pagecache.go`) is a fixed-size mmap'd file divided
into PageSize slots. An in-memory index maps `(timelineID, pageAddr)` to slot
numbers. Random eviction reuses slots when full.

- Populated on reads from delta/image layers (not on writes). Writes go only
  to memLayer. `flushFrozenLayers` invalidates cache entries for flushed pages.
- Any cache entry can be evicted — the data is always recoverable from layers.
- Bounded by configuration. No unbounded growth like the current `openBlocks`.

For bulk sequential reads (e.g. reading a large file), the page cache can
optionally use larger readahead units — fetch a range of pages from a layer in
one operation.

### Local cache directory layout

```
<dataDir>/
    mem/
        <startSeq>-<rand>.ephemeral             # active memLayer mmap backing file
        <startSeq>-<rand>.ephemeral             # frozen memLayer(s) awaiting upload
    pages.cache                                 # page cache: fixed-size file of 64KB slots
<cacheDir>/
    deltas/<startSeq>-<endSeq>                  # cached full delta layer blob
    images/<seq>                                # cached full image layer blob
```

**`mem/`**: pinned, cannot be evicted. One mmap backing file per memLayer
(active + frozen). Each file is `maxPages × PageSize` bytes, pre-allocated via
`Truncate`. Cleaned up (munmap + close + remove) after successful flush to S3.

**`pages.cache`**: a fixed-size file divided into 64KB slots. The in-memory
index maps `(timelineID, pageAddr)` to a slot number. Random eviction reuses
slots. Size bounded by `PageCache.maxBytes`.

**`<cacheDir>/`**: full layer blobs cached on disk at `<cacheDir>/<key>`.
Immutable — never invalidated, evictable under disk pressure (re-downloaded on
next access). Cleaned up when GC deletes the layer. A non-lease-holder node
with a stale cached file will get a 404 on the next S3 read, at which point
it deletes the stale cache entry and refreshes its layer map from S3. Random
eviction when the in-memory cache exceeds `MaxLayerCacheEntries`.

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
    images/<seq>                                            # image layer
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
        {"seq": 500, "pages": [0, 65535], "key": "images/00000000000001f4", "size": 4194304}
    ]
}
```

## Comparison with current design

| Aspect | Current | LSM |
|---|---|---|
| S3 PUTs for 300 scattered 64KB writes | 300 (one per block) | 1 (one delta layer) |
| Upload volume for above | 300 × 1MB = 300MB | ~100KB compressed |
| Snapshot/freeze | Upload all dirty blocks, blocking | Freeze memLayer + flush frozen layers + write meta.json |
| Clone | Copy refBlockIndex, create child layer | Pointer to ancestor + seq |
| Read path (warm) | pread on local block file | Page cache hit |
| Read path (cold) | Download full block from S3 | 3 small S3 range reads (header, index, page) |
| Local disk usage | Unbounded (openBlocks) | Bounded (pinned memLayers + page cache) |
| Branching model | refBlockIndex + layer chain | Ancestor pointer + seq |

## Mapping to current design

The LSM model replaces the core storage abstraction. The key interfaces change:

**Layer → Timeline**: instead of `Layer.Read/Write`, the timeline owns the layer
stack and provides Read/Write. The `Volume` struct holds a `*Timeline` instead
of a `*Layer`.

**VolumeManager → TimelineManager**: manages timelines instead of layers.
Snapshot and clone create new timelines instead of freezing layers and creating
children.

**BlockCache → PageCache**: fixed-size cache of 64KB pages (random eviction)
instead of immutable block file cache.

**Flusher → delta layer writer**: instead of uploading individual blocks, the
flusher serializes frozen in-memory layers into delta layer objects.

The FUSE/NBD frontend and the Volume API (Read/Write/PunchHole/Snapshot/Clone)
remain the same from the caller's perspective. The daemon, client, and
container storage interfaces are unaffected.

## PunchHole

PunchHole (FUSE fallocate with punch mode) zeroes a range of the block device.
Partial pages at the start and end of the range are handled by zero-filling
via `tl.Write` (read-modify-write under the per-page lock). Only fully-covered
interior pages get tombstones, avoiding a full 64KB write:

```
PunchHole(offset, length):
    // Partial page at start: zero-fill via tl.Write
    if offset is not page-aligned:
        tl.Write(zeros for remainder of start page)

    // Partial page at end: zero-fill via tl.Write
    if end is not page-aligned:
        tl.Write(zeros for beginning of end page)

    // Tombstone fully-covered interior pages
    for each 64KB page fully covered by [offset, offset+length):
        seq := timeline.nextSeq++
        pageAddr := pageStart / 65536
        timeline.memLayer.PutTombstone(pageAddr, seq)
        pageCache.Delete(timeline.id, pageAddr)
```

**Representation**: in the memLayer, a tombstone is a `memEntry` with
`tombstone = true` and no data written to the mmap. In the delta
layer on S3, a tombstone has `valueLen = 0` in the index entry — no compressed
page data is stored. This avoids wasting 64KB per punched page.

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

## ZeroRange

ZeroRange (FUSE fallocate with zero mode) delegates to PunchHole. Both
operations produce the same result: the affected range reads back as zeros.

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
local memLayer mmap. If the process crashes, these writes are lost.
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

The timeline engine depends on two interfaces, both injected at construction:

- **ObjectStore** (`object_store.go`): S3-compatible object store with
  Get/GetRange/Put/List/Delete operations.
- **LocalFS** (`lsm/localfs.go`): local filesystem for delta/image layer cache
  files (Remove, MkdirAll, ReadFile, WriteFile). MemLayer uses real OS files +
  mmap directly, not through this interface.

Production uses real S3 and OS filesystem. Tests inject simulation
implementations.

### SimObjectStore

In-memory S3 simulation with configurable fault injection (see
`lsm/simstore_test.go`). Supports per-operation failure probabilities, latency
injection, and phantom puts (S3 succeeds but caller sees an error).

Latency is implemented via `time.Sleep` — under synctest, this advances fake
time instantly when all goroutines are blocked, so tests run fast while still
exercising concurrent timing behavior.

Faults are seeded from a deterministic PRNG so tests are reproducible. The seed
is logged on failure for replay.

### SimLocalFS

In-memory filesystem that supports crash simulation (see `lsm/simfs_test.go`).
Only used for delta/image layer cache files — MemLayer uses real OS files + mmap
via `t.TempDir()`.

`Crash()` discards only files matching `/mem/` in the path or ending with
`.ephemeral` — these are the memLayer backing files. Delta/image layer caches
are retained (they are immutable copies of S3 data). After crash, the timeline
engine is reconstructed from `SimObjectStore` state — same as real crash
recovery. MemLayer mmap files are real OS files in `t.TempDir()` and become
orphans cleaned up by test temp dir cleanup.

### Simulation test

The simulation test runs many concurrent simulated nodes against a shared
`SimObjectStore`, each with its own `SimLocalFS`. Nodes independently perform
random operations (writes, reads, flushes, snapshots, clones, compaction, GC)
and randomly crash and recover. All behavior is driven by a deterministic PRNG
seeded from a single seed, logged on failure for exact replay.

The test (see `lsm/simulation_test.go`) runs inside `synctest.Run` with a
configurable number of nodes, device size, operation count, crash rate, and S3
fault parameters. A Simulation holds a deterministic PRNG, a shared
SimObjectStore, a list of SimNodes, and an Oracle for verification. Each SimNode
has its own SimLocalFS (lost on crash), a set of active timelines, and lease
tracking.

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

**Oracle** (see `lsm/oracle_test.go`): a simple reference model that tracks
what each timeline should contain at each point. It records every flushed write
(page address → data) per timeline, respecting ancestor chains. After every
read, the simulation verifies the result matches the oracle. Unflushed writes
are tracked separately and discarded on crash.

**Crash and recovery**: when a node crashes, its `SimLocalFS` discards files
matching `/mem/` or `.ephemeral`. Its in-memory timeline state is destroyed.
The oracle discards that node's unflushed writes. On recovery, the node
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
