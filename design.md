# Loophole Design

Loophole is a block-level virtual disk backed by S3 with instant copy-on-write snapshots and clones.

## Glossary

**Page** — A 4 KB chunk of data (4096 bytes). The smallest unit of storage. Pages are addressed by index and stored in compressed batches (L0 files, L1/L2 blocks) in S3.

**Layer** — An S3 prefix (`layers/<uuid>/`) containing an `index.json` and data files organized into three levels (L0, L1, L2). A layer is either *mutable* (accepts writes via a memtable) or *frozen* (immutable, created by a snapshot). Each layer's `index.json` is self-contained — it references all data files it needs, including inherited ones from parent layers. No recursive ancestor traversal.

**Volume** — A named, user-facing handle (like `my_files`). A volume points to a current layer via a volume ref in S3 (`volumes/<name>`). When a volume is snapshotted, a child layer is created and the parent is re-layered (swapped to a new layer). The name stays the same but the underlying layer changes.

**Volume Ref** — A small JSON object in S3 (`volumes/<name>`) that maps a volume name to its current layer ID, size, type, lease token, and write lease sequence number.

**Freeze** — The act of making a volume immutable. Flushes the memtable to S3, optionally compacts L0→L1, then sets `frozen_at` in the `index.json` object metadata. No further writes are accepted.

**Memtable** — An in-memory write buffer backed by an mmap'd file. Writes go to the memtable first. When the memtable reaches a size threshold (default 256 MB), it is frozen and flushed to S3 as an L0 file. Multiple frozen memtables may be queued for upload.

**Page Cache** — A fixed-size mmap'd file providing page-level caching of data read from S3 (L0/L1/L2 files). Keyed by (layer ID, page index). Not used for memtable data (which is already in memory).

## Storage Levels

Data flows through three levels on S3, with compaction merging upward:

- **L0** — Page-granularity flush files. Each L0 file contains independently zstd-compressed pages from a single memtable flush. Small, scattered writes (ext4 metadata, journal) are batched into a single S3 PUT.
- **L1** — Sparse 4 MB blocks. Each block covers a contiguous range of 1024 pages but only contains the pages that were written. Created by L0→L1 compaction when the L0 page count exceeds a threshold (default 10,000 pages).
- **L2** — Dense 4 MB blocks. When an L1 block reaches 25% occupancy (256+ pages), it is promoted to L2 by merging with the existing L2 block for that address range.

Read path: memtable → frozen memtables → page cache → L0 → L1 → L2 → zeros.

See `storage2.md` for the full storage design.

## S3 Layout

```
s3://bucket/[prefix/]
├── layers/
│   ├── <layer-uuid-A>/
│   │   ├── index.json                                  # layerIndex (L0/L1/L2 metadata)
│   │   ├── l0/<nextSeq>-<writeLeaseSeq>               # L0 flush files
│   │   ├── l1/<writeLeaseSeq>-<blockAddr>              # L1 sparse blocks
│   │   └── l2/<writeLeaseSeq>-<blockAddr>              # L2 dense blocks
│   └── ...
├── volumes/
│   ├── myvolume                # volumeRef JSON
│   └── myclone                 # volumeRef JSON
└── leases/
    └── <token>.json            # LeaseManager heartbeat/RPC file
```

### index.json

```json
{
  "next_seq": 42,
  "l0": [
    {"key": "layers/<uuid>/l0/...", "pages": [0, 5, 12], "size": 8192}
  ],
  "l1": [
    {"start": 0, "end": 10, "layer": "<uuid>", "write_lease_seq": 1}
  ],
  "l2": [
    {"start": 0, "end": 5, "layer": "<uuid>", "write_lease_seq": 1}
  ]
}
```

- `next_seq`: monotonic counter for unique L0 file naming.
- `l0`: list of L0 flush files with their page indices.
- `l1`/`l2`: block range maps — contiguous ranges of block addresses mapped to the layer and write lease sequence that owns the blob.
- `frozen_at`: stored as object metadata (not in the JSON body). Present means immutable.

### Volume Ref

```json
{
  "layer_id": "<uuid>",
  "size": 8589934592,
  "type": "ext4",
  "lease_token": "<daemon-lease-token>",
  "write_lease_seq": 3,
  "parent": "parent-volume-name",
  "labels": {"key": "value"}
}
```

## Invariants

1. **Only frozen layers are shared between volumes.** Snapshot creates a child layer by copying `index.json`. The parent is then re-layered (swapped to a new layer ID) so the old layer is never written to again. Both child and new parent inherit the same immutable L0/L1/L2 objects.

2. **A frozen layer is never written to again.** Once `frozen_at` metadata is set, the layer accepts no further writes. The only mutation is reading it.

3. **Each layer's index.json is self-contained.** It references all L0/L1/L2 data it needs. L1/L2 block ranges may reference blobs under other layer prefixes (inherited from a parent), but the index itself contains the complete picture.

4. **At most one process holds a volume's write lease.** Enforced by CAS on the volume ref's `lease_token`. A process must hold the lease before opening the layer for writing.

5. **A mutable volume always points to a mutable layer.** Snapshot re-layers the parent to a new mutable layer. A read-only volume points to a frozen layer.

## Filesystem Layer

Loophole exposes a full POSIX filesystem to the user. The stack:

```
  user files (read/write/mkdir/...)
        │
     ext4 (journaled filesystem)
        │
     loop device (/dev/loopN)
        │
     FUSE block device (/mnt/loophole/<volume>/device)
        │
     Layer (memtable → L0/L1/L2 → S3)
```

### Mount

1. Open (or create) the volume via VolumeManager. This loads the layer from S3.
2. FUSE exports a regular file (`<fuse-mount>/<volume>/device`) that represents the raw block device. The file's size equals the configured volume size (see [Volume Size](#volume-size)). Reads and writes to this file go to the layer. The FUSE mount is internal (e.g. `/mnt/loophole` or `~/.loophole/fuse/`).
3. Attach a loop device: `losetup --direct-io=on --find --show <fuse-mount>/<volume>/device`.
4. If this is a new volume, format it: `mkfs.ext4 /dev/loopN`.
5. Mount the filesystem: `mount /dev/loopN <user-mountpoint>`.

The user interacts with `<user-mountpoint>` as a normal directory.

### Unmount

1. `umount <user-mountpoint>`
2. `losetup --detach /dev/loopN`
3. Flush the layer (freeze memtable, upload frozen memtables to S3 as L0 files).
4. Close the volume (release volume lease in S3).

### Avoiding Double Caching

Without care, the same block data ends up cached twice in the kernel page cache:

1. **ext4 file pages** — cached under ext4 inodes. When a user reads a file, the kernel caches those pages keyed by (ext4 inode, offset). This is the normal page cache behavior for any filesystem.
2. **FUSE backing file pages** — cached under the FUSE `device` inode. When the loop device reads from the FUSE file, the kernel caches those pages keyed by (FUSE inode, offset).

These are different inodes on different filesystems, so the kernel treats them as separate entries in the same page cache. The result: every block of user data lives in RAM twice.

We eliminate the redundant copy by setting the `direct_io` flag in our FUSE open handler for the `device` file. This tells the kernel not to cache the FUSE file's contents in the page cache — reads and writes pass straight through to our FUSE handler. We also use `losetup --direct-io=on` so the loop device opens the backing file with O_DIRECT, reinforcing the bypass.

With both set, the caching layers are:

```
  page cache             ← ext4 file pages in RAM (wanted, managed by kernel)
  (no FUSE file pages)
  memtable + page cache  ← pages in mmap'd memory (wanted, managed by us)
  S3                     ← durable storage
```

The page cache gives users fast repeated reads of their files. Our memtable and page cache avoid repeated S3 fetches. There is no redundant RAM copy in between.

### Future: NBD

The FUSE + loop device layers can be replaced with NBD (Network Block Device), which provides a real kernel block device directly. NBD has no backing file, so there is no double-caching problem. The rest of the stack (ext4, fsfreeze) stays the same. The layer interface is transport-agnostic.

## Volume Size

Volume size is stored in the volume ref and determines:
- The size of the FUSE device file (or NBD export size).
- The upper bound for ext4 formatting and mount.
- The bounds check for writes at the transport layer (FUSE/NBD).

Volume size is specified via the `--size` option at creation time (default 8 GB). The FUSE and NBD backends enforce bounds.

## Filesystem-Level Snapshot

A snapshot creates a frozen, point-in-time copy of the filesystem that can be independently mounted (read-only) or cloned (read-write). No data is copied — only metadata (`index.json`).

1. **fsfreeze -f \<user-mountpoint\>** — tells ext4 to flush its journal and stop accepting new writes.
2. **Acquire exclusive volume lock** (volume.mu write lock) — drains in-flight writes.
3. **Flush** — freeze the memtable and upload all frozen memtables to S3 as L0 files.
4. **Branch** — copy `index.json` to a new child layer. Then re-layer the parent: create a second new layer for the parent volume so the snapshotted layer is never written to again.
5. **Create snapshot volume ref** — write a read-only volume ref pointing to the child layer.
6. **Release exclusive volume lock** — new writes go to the parent's new layer.
7. **fsthaw -f \<user-mountpoint\>** — resume the filesystem.

From the user's perspective, the filesystem briefly pauses (typically < 1 second), then continues as if nothing happened.

## Filesystem-Level Clone

A clone creates an independent writable copy of a volume.

**From a read-only (frozen) volume:**

The layer is already frozen — no fsfreeze needed.

1. Copy the frozen layer's `index.json` to a new child layer.
2. Create a volume ref for the clone pointing to the child.
3. Mount the clone.

**From a mutable volume:**

1. **fsfreeze** the source filesystem.
2. **Acquire exclusive volume lock** on the source.
3. Flush + branch (creates child layer + re-layers parent).
4. Create a volume ref for the clone pointing to the child layer.
5. **Release exclusive volume lock** on the source.
6. **fsthaw** the source filesystem.
7. Mount the clone.

In both cases, no page data is copied. The clone starts with an inherited `index.json` pointing to the parent's L0/L1/L2 objects and reads everything from them. Writes go to the clone's own L0 files.

## Block Device Semantics

Loophole is a block device, not a filesystem. Like a real disk:

- **Writes can be reordered.** Memtable flushes may send pages to S3 in any order. The filesystem above (ext4) uses its journal to handle crash recovery — it does not rely on the block device preserving write order.
- **Sector-level atomicity only.** A write within a single sector (512 bytes) is atomic. Larger writes are not — a reader may see a partially-written page. This matches real disk behavior.
- **No durability without fsync.** Between fsyncs, writes live only in the memtable mmap. They may or may not have been flushed to S3. The filesystem is responsible for calling fsync when it needs durability.

The filesystem is responsible for its own consistency. ext4's journal ensures that metadata operations are atomic and recoverable regardless of block device write ordering.

## Leases

Volumes are protected by a lease system to prevent two daemons from writing to the same volume simultaneously. Frozen/read-only volumes need no lease.

The lease token is stored directly in the volume ref (`lease_token` field). A separate `LeaseManager` maintains a heartbeat file in S3 (`leases/<token>.json`) with an RPC inbox for cross-daemon communication.

**Acquire**: CAS on the volume ref. Checks that the existing `lease_token` is absent or that the holder's heartbeat has expired. On success, writes our daemon's token. Also increments `write_lease_seq` (used in L1/L2 blob key naming for uniqueness).

**Break-lease**: A daemon that wants to take over a volume sends an RPC message (via the lease file's inbox) to the current holder, requesting it to flush and release. If the holder doesn't respond, the lease can be force-cleared.

**Release**: On clean unmount, CAS clears the `lease_token` from the volume ref.

## Concurrency

- **Volume RWMutex**: shared (read) lock held during normal I/O, exclusive (write) lock held during snapshot/clone. The exclusive lock drains in-flight writes before the layer swap.
- **Memtable lock**: read lock for reads and writes to the active memtable, write lock for freezing (swapping to a new memtable).
- **Flush mutex**: serializes frozen memtable uploads to S3.
- **Compact mutex**: serializes L0→L1 compaction.
- **Per-page locks**: serialize partial-page read-modify-write cycles (256 sharded mutexes).

## Daemon and RPC

Loophole is a single binary that acts as both the daemon and the CLI.

### Daemon

The daemon starts automatically when any command needs it. It creates an internal FUSE mountpoint and listens on a Unix domain socket for RPC. It manages multiple volumes, each appearing as a separate device file under the internal FUSE mount. All volumes share a single page cache and VolumeManager.

Config lives in `~/.loophole/config.toml`. Each `[profiles.<name>]` section defines an S3/R2 backend. Use `-p <profile>` to select one.

### RPC Socket

The daemon listens on an HTTP server over a Unix domain socket at `~/.loophole/<profile>.sock`.

### CLI Workflows

**Create and use a new volume:**

```
$ loophole -p r2 create --size 16GB myfiles
$ loophole -p r2 mount myfiles /mnt/myfiles
$ echo "hello" > /mnt/myfiles/greeting.txt
```

**Snapshot before a risky change:**

```
$ loophole -p r2 snapshot /mnt/myfiles before-migration
$ run-migration.sh /mnt/myfiles
# if migration fails:
$ loophole -p r2 unmount /mnt/myfiles
$ loophole -p r2 clone before-migration myfiles-restored
$ loophole -p r2 mount myfiles-restored /mnt/myfiles
```

**Freeze a volume (compact + mark immutable):**

```
$ loophole -p r2 freeze myfiles
```

## Key Modules

| File | Purpose |
|---|---|
| `storage2/manager.go` | Creates/opens volumes, manages volume refs and leases |
| `storage2/volume.go` | Named volume with snapshot/clone/freeze, layer swap (re-layer) |
| `storage2/layer.go` | Core I/O: read/write/flush, memtable management, L0→L1 compaction |
| `storage2/index.go` | Layer index structures, block range maps |
| `storage2/memtable.go` | In-memory write buffer backed by mmap'd files |
| `storage2/pagecache.go` | Page-level disk cache (mmap'd file) |
| `storage2/frozen_volume.go` | Read-only volume backed by a frozen layer |
| `storage2/l0.go` | L0 file format: build and parse compressed page batches |
| `storage2/block.go` | L1/L2 block format: build and parse compressed block files |
| `interfaces.go` | Volume and VolumeManager interfaces |
| `object_store.go` | ObjectStore interface (S3 abstraction), ModifyJSON CAS helper |
| `daemon/daemon.go` | HTTP/UDS server, RPC handlers, volume lifecycle orchestration |
| `cmd/loophole/` | CLI argument parsing, daemon startup |
