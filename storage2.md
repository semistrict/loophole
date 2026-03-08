# Storage v2: Tiered LSM with Immutable Layers

## Overview

Replace the current ancestor-pointer-with-beforeSeq design with a simpler model:
- Layers are **immutable once frozen**. Snapshots freeze the current layer and move the volume to a new child.
- Each layer's index is **self-contained** — it references all data files it needs (including inherited ones from ancestors). No recursive ancestor traversal.
- Data files are organized into three levels: **L0** (4KB pages), **L1** (sparse 4MB blocks), **L2** (dense 4MB blocks). Compaction merges upward: L0 → L1 → L2.
- A single `index.json` per layer stores all level metadata atomically.

## Compression and page access

All data files on S3 use the same format: each 4KB page is **independently zstd-compressed** within the file. A file-level header maps page addresses to `(byte_offset, compressed_length, CRC32)`. To read a single page, use S3 `GetRange` to fetch just that page's compressed bytes — no need to download the whole file. The decompressed 4KB page is cached in the page cache.

**L1 and L2 blocks** include a **zstd shared dictionary** in the file header, trained from the pages in the block. Each page is still independently decompressible — you just need the dictionary. The header (including dictionary) is fetched and cached on first access to a block; subsequent page reads use cached dictionary + `GetRange`.

## Data file levels

### L0 — Page-granularity flushes

- Each L0 file contains individual 4KB page writes.
- Flushed from the memtable when it reaches the threshold.
- `index.json` stores a **page index** for each L0 file: the set of page addresses it contains, plus which of those are **tombstones**. This avoids downloading L0 blobs just to check if a page exists.
- Tombstones represent punched holes — they shadow inherited L1/L2 data and return zeros on read.
- Many small files accumulate here between compactions.

### L1 — Sparse 4MB blocks

- Each L1 block is aligned to a 4MB region (1,024 pages) but only contains the pages that have changed — a sparse overlay.
- Created by merging L0 pages into 4MB-aligned blocks.
- When an L1 block exceeds **25% occupancy** (>256 pages), it is merged into the corresponding L2 block.

### L2 — Dense 4MB blocks

- Each L2 block covers a 4MB-aligned region and contains all pages that have data.
- Created by merging an L1 block into an existing L2 block (or promoted directly from a >50% full L1 block).
- Covers the full address space of the volume.

L1 and L2 share the same block address space (`pageAddr / 1024`). For any given block address, there may be an L1 entry, an L2 entry, or both (L1 overlays L2).

## S3 layout

```
layers/{layer_id}/
  meta.json
  index.json                # single index covering L0, L1, L2
  l0/
    {seq_start}-{seq_end}   # L0 data blobs
  l1/
    {block_addr}            # L1 data blobs (sparse 4MB blocks)
  l2/
    {block_addr}            # L2 data blobs (dense 4MB blocks)
```

## In-memory structure

```go
type Layer struct {
    id    string
    store loophole.ObjectStore

    mu       sync.RWMutex
    memtable *Memtable

    frozenMu     sync.RWMutex
    frozenTables []*Memtable

    // From per-level index files — self-contained, includes inherited entries.
    l0 []L0Entry            // page index per file, ordered newest-first
    l1 *BlockRangeMap       // block address → layer_id (sparse 4MB blocks)
    l2 *BlockRangeMap       // block address → layer_id (dense 4MB blocks)

    nextSeq atomic.Uint64
}
```

L1 and L2 use a range map: contiguous block addresses from the same layer are stored as a single range entry. A fully-written 1TB volume compacted in one pass is **one range entry** for all of L2 instead of 256K per-block entries.

L0 stays a slice because reads must check each file's page index.

## index.json format

```json
{
  "next_seq": 42,
  "l0": [
    {
      "key": "layers/abc123/l0/0000000038-000000003f",
      "pages": [0, 5, 17, 1024],
      "tombstones": [42, 43],
      "size": 16384
    }
  ],
  "l1": [
    {"start": 0, "end": 100, "layer": "abc123"},
    {"start": 100, "end": 200, "layer": "def456"}
  ],
  "l2": [
    {"start": 0, "end": 256000, "layer": "abc123"}
  ]
}
```

- **L0**: list of flush files with page indexes. Bounded by the 100K page entry limit.
- **L1/L2**: ranges mapping contiguous block addresses `[start, end)` to the layer ID that owns the blobs. Blob key for block address N in layer L is `layers/{L}/l1/{N}` or `layers/{L}/l2/{N}`.

A fully-written 1TB volume after one compaction pass: **one range entry** in L2.

## Read path

Single layer, no recursion:

```
memtable → frozen memtables → L0 → L1 → L2 → zeros
```

1. **Memtable** — in-memory writes not yet flushed.
2. **Frozen memtables** — frozen but not yet uploaded to L0 (newest first).
3. **L0** — check each file's page index; skip files that don't contain the target page. If a matching file has a **tombstone** for this page, return zeros (don't fall through to L1/L2). Otherwise download and extract from matching files (newest first).
4. **L1** — lookup `l1.Find(pageAddr / 1024)` → get layer ID. Download the block's file header to check if the target page exists; if yes, `GetRange` the compressed page, decompress, cache in page cache.
5. **L2** — lookup `l2.Find(pageAddr / 1024)` → get layer ID. Same as L1.
6. **Zeros** — page never written or not present in any block.

No `beforeSeq`. No ancestor pointer. No recursive `readPage`.

## Write path

Unchanged: writes go into the memtable, which is flushed to an L0 file when it reaches the threshold.

## Snapshot

1. Flush the active layer's memtable to L0.
2. Freeze the layer (mark immutable, stop periodic flush).
3. Create a new child layer.
4. **Copy the parent's `index.json` into the child** — every entry is preserved, including inherited ones from the parent's ancestors.
5. Update the volume ref to point to the child layer.
6. The parent layer is now immutable forever.

Cost: O(size of index files) — copying metadata, not data. The actual S3 blobs are shared.

## Clone

Same as snapshot, but the clone gets its own volume ref pointing to a new child layer. The original volume also moves to a new child (both start with the same inherited entries).

## Compaction

Compaction merges data files upward within a single layer.

### L0 → L1

- Trigger: when total L0 page entries exceeds ~10K.
- Group L0 pages by 4MB block address (`pageAddr / 1024`).
- For each block: read existing L1 block (if any), overlay L0 pages, write new L1 block.
- **Tombstones**: a tombstoned page writes zeros into the L1 block, masking any inherited L2 data.
- Remove compacted L0 entries from `index.json`, add/update ranges in `index.json`.
- L0 blob deletion deferred to GC (other layers may reference them).

### L1 → L2

- Trigger: per-block, when an L1 block exceeds **25% occupancy** (>256 out of 1024 pages).
- Read existing L2 block (if any), overlay L1 pages, write new L2 block.
- Remove the L1 entry from `index.json`, add/update range in `index.json`.
- L1 blob deletion deferred to GC.

### Hard limits

- L0 page entries: max 100K (compaction should trigger well before this, at ~10K).
- L1/L2: no practical limit on block count (range-based representation keeps index small).

### No branch point blocking

Compaction updates only the compacting layer's own index files. Children have their own copies pointing to original blobs. No coordination needed.

## Garbage collection

A data file blob on S3 can be deleted when no layer's index references it.

### Mark-and-sweep

1. List all layers.
2. Read each layer's index files, collect all referenced blob keys into a set.
3. List all blob objects in S3.
4. Delete any blob not in the referenced set.

Cost: O(layers + blobs). Run periodically or on-demand.

### Practical shortcut

A blob created by layer L can only be referenced by L and L's descendants. If L has no descendants, its blobs can be deleted immediately during compaction without a full sweep.

## Page cache

- Cache individual 4KB pages extracted from L0/L1/L2 downloads.
- Cache key: `(layer_id, page_addr)` — but only for frozen layers.
- Frozen layer data is immutable, so cached pages never go stale. No invalidation needed.
- Active layer reads hit memtable/frozen memtables first (in-memory), then fall through to inherited entries from frozen ancestors (cacheable).

## What this eliminates from the current design

- `ancestor *timeline` pointer and recursive `readPage`
- `beforeSeq` filtering
- `BranchPoint` tracking in `meta.json`
- Branch points blocking compaction
- Timeline cache (`timelineCache` on Manager) and its staleness bugs
- `Superseded` layer concept
- Page cache invalidation complexity
- The `readPage` vs `readPageWith` split (only one read path needed)

## Migration

Clean break — new format only, no migration. No users yet.
