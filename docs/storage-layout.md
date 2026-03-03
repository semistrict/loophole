# S3 Storage Layout

Loophole stores all persistent state in a single S3 bucket under an optional
prefix. The layout is flat and uses only three top-level directories plus one
root file.

```
s3://bucket/prefix/
  state.json
  leases/
    <token>.json
  layers/
    <layer-id>/
      state.json
      0000000000000000
      0000000000000001
      ...
  volumes/
    <volume-name>
```

## `state.json` -- System State

Written once during `FormatSystem()` with `PutIfNotExists`. Contains the
block size used by every volume in this store.

```json
{
  "block_size": 4194304
}
```

Block size is typically 4 MB. All blocks in all layers share this size.

## `leases/<token>.json` -- Process Leases

One file per running loophole process. The token is a UUID generated on
startup.

```json
{
  "expires": "2026-02-26T15:45:30Z"
}
```

- Created atomically (`PutIfNotExists`) on first mount.
- Renewed every 30 seconds via CAS (ETag-conditional PUT). Duration is 60s.
- Deleted on clean shutdown.
- A missing file means clean shutdown; an expired file means the process
  crashed. Used to enforce single-writer-per-layer.

## `volumes/<volume-name>` -- Volume References

One file per named volume. This is an indirection: the volume name points at
whichever layer is currently active for that volume.

```json
{
  "layer_id": "a1b2c3d4-...",
  "size": 107374182400
}
```

- `layer_id` -- UUID of the mutable layer currently backing this volume.
- `size` -- volume size in bytes (0 means default 100 GB). Immutable after
  creation.
- Created with `PutIfNotExists` to prevent duplicates.
- Updated via CAS when snapshotting (the old layer is frozen, a new child
  layer is created, and `layer_id` is swapped to the child).

## `layers/<layer-id>/` -- Layer Data

Each layer is identified by a UUID. A layer contains a `state.json` plus zero
or more block objects.

### `layers/<layer-id>/state.json`

```json
{
  "ref_layers": ["parent-uuid", "grandparent-uuid"],
  "ref_block_index": {
    "0000000000000001": 0,
    "0000000000000005": 0
  },
  "frozen_at": "",
  "lease_token": "process-uuid",
  "children": ["child-uuid"]
}
```

| Field             | Description |
|-------------------|-------------|
| `ref_layers`      | Ordered list of ancestor layer IDs (parent first). |
| `ref_block_index` | Maps block index (hex string) to an index into `ref_layers`, indicating which ancestor owns that block. Compact: stores an int instead of a full UUID per block. |
| `frozen_at`       | RFC 3339 timestamp if the layer is frozen (immutable). Empty string if mutable. |
| `lease_token`     | UUID of the process holding the write lease. Only meaningful for mutable layers. |
| `children`        | Direct child layer IDs created from this layer. |

All updates to layer state use CAS (ETag-conditional PUT) with up to 5
retries on conflict.

### `layers/<layer-id>/<block-index>` -- Blocks

Block indices are **16-character zero-padded hex strings** derived from byte
offset: `block_index = offset / block_size`.

Examples: `0000000000000000`, `000000000000000a`, `ffffffffffffffff`.

There are three kinds of block objects:

| Kind      | S3 Object | Meaning |
|-----------|-----------|---------|
| Data      | Raw bytes, exactly `block_size` long | Written block data |
| Tombstone | Zero-length object | Explicitly zeroed block (e.g. from hole-punch) |
| Absent    | No object exists | Block was never written; reads return zeros (or fall through to ancestors) |

Blocks are stored as **raw binary with no compression or content-addressing**.

## Read Path

When reading block *B* from a layer:

1. Check the layer's own blocks. If *B* exists (data or tombstone), return it.
2. Check `ref_block_index`. If *B* maps to ancestor *i*, read from
   `ref_layers[i]`. That ancestor is guaranteed frozen, so the block is
   immutable.
3. If not found anywhere, return zeros.

## Write Path and Flushing

Writes go to a local mutable cache first, tracked in a dirty set. The flusher
uploads dirty blocks to S3 concurrently (up to 20 parallel uploads by
default).

- **Backpressure**: writes block when dirty count hits `MaxDirtyBlocks`
  (default 100, ~400 MB at 4 MB block size). Early flush starts at 50%.
- **Zero optimization**: all-zero blocks are converted to tombstones (empty
  objects) instead of uploading 4 MB of zeros.
- On partial upload failure, blocks are re-marked dirty for retry.

## Snapshots and Clones

**Snapshot** of volume *V*:

1. Flush all dirty blocks for the current layer.
2. Freeze the current layer (set `frozen_at`).
3. Create a new child layer with `ref_layers` pointing to the frozen parent
   (and transitively to all ancestors).
4. Update the volume ref to point at the new child layer.

**Clone** from a frozen layer:

1. Create a new layer with `ref_layers` pointing to the source layer (and its
   ancestors).
2. Populate `ref_block_index` from the source's own blocks plus its
   `ref_block_index`.
3. Create a new volume ref pointing at the clone layer.

Both operations are metadata-only -- no block data is copied.

## Copy-on-Write Semantics

- Blocks owned by **frozen** layers can be referenced by pointer (entry in
  `ref_block_index`). No data copy needed.
- Blocks owned by **mutable** layers must be data-copied if referenced,
  because the source could still change. In practice this doesn't happen
  because snapshots freeze the source first.

## Local Disk Cache

Not stored in S3, but relevant to understanding the full picture:

- **Immutable cache** (`<cache-dir>/immutable/<layer-id>/<block-index>`):
  shared, persistent, deduplicated via `singleflight`.
- **Mutable cache** (`<cache-dir>/mutable/<uuid>/<layer-id>/<block-index>`):
  per-process, wiped on startup. After a layer is frozen, its blocks are
  adopted into the immutable cache.

## Instance Identity

An instance is identified by `bucket + prefix + endpoint`. A 12-character hex
hash of these three values is used to name local cache directories and Unix
sockets, allowing multiple instances on the same machine without collisions.

Configuration:
- S3 URL: `s3://bucket/prefix` or `s3://bucket`
- `S3_ENDPOINT` -- custom S3 endpoint (for MinIO, R2, etc.)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` -- credentials
