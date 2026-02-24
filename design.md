# Loophole Design

Loophole is a block-level virtual disk backed by S3 with instant copy-on-write snapshots and clones.

## Glossary

**Block** — A fixed-size chunk of data (default 4 MB). The smallest unit of storage. Blocks are addressed by index (0, 1, 2, ...) and stored as individual S3 objects with zero-padded hex names (`0000000000000000`, `0000000000000001`, ...).

**Layer** — An S3 prefix (`layers/<uuid>/`) containing a `state.json` and zero or more block objects. A layer is either *mutable* (accepts writes) or *frozen* (immutable, created by a snapshot). Each layer has a parent chain of frozen ancestors. Reads check local cache first, then fall back to the block ownership map to find which ancestor in S3 holds the data. Writes always go to the mutable layer's local cache.

**Volume** — A named, user-facing handle (like `my_files`). A volume points to a current layer via a volume ref in S3 (`volumes/<name>`). When a volume is snapshotted, its current layer is frozen and it moves to a new child layer. The name stays the same but the underlying layer changes.

**Volume Ref** — A small JSON object in S3 (`volumes/<name>`) that maps a volume name to the UUID of its current layer. Updated atomically with CAS on snapshot/clone. May carry a `snapshot_pending` field during a two-phase snapshot commit (see [Crash safety](#crash-safety)).

**Tombstone** — A zero-byte S3 object at a block key. Indicates that the block has been explicitly zeroed. Tombstones override ancestor data: if a parent has block 5 and the child writes a tombstone for block 5, reads return zeros.

**Block Ownership Map** — A flat `map[uint64]string` (block index → layer UUID) built at mount time by listing this layer and all ancestor layers in parallel. Built from root to leaf so closer ancestors override farther ones. Tombstones delete entries. Rebuilt from scratch on every mount, so it always reflects the true state of S3 at load time. Immutable for the lifetime of a single mount — blocks uploaded during this session are not added to the map (those are tracked by the local index instead). Used as the fallback for reads that miss the local cache and the local index.

**Disk Cache** — A local directory of block data files, one file per block. All state is in memory — dirty set, local index, zero-blocks set. On crash, the cache is lost and blocks are re-fetched from S3 on demand. No persistence layer.

**Freeze** — The act of making a layer immutable. Flushes all dirty blocks to S3, then writes `frozen_at` to `state.json`. No further writes are accepted.

**Ancestor Chain** — The `ancestors` field in `state.json`: an ordered list `[parent, grandparent, ..., root]`. The full chain is stored in every layer so loading is O(1) state reads + parallel block listings — no chain walking needed.

## S3 Layout

```
s3://bucket/[prefix/]
├── layers/
│   ├── <layer-uuid-A>/
│   │   ├── state.json          # LayerState
│   │   ├── 0000000000000000    # block 0 (4 MB)
│   │   ├── 0000000000000001    # block 1
│   │   └── 000000000000000f    # block 15 (tombstone if size == 0)
│   ├── <layer-uuid-B>/
│   │   ├── state.json
│   │   └── ...
│   └── ...
└── volumes/
    ├── myvolume                # {"layer_id": "<layer-uuid-B>"}
    └── myclone                 # {"layer_id": "<layer-uuid-C>"}
```

### state.json

```json
{
  "ancestors": ["<parent-uuid>", "<grandparent-uuid>"],
  "block_size": 4194304,
  "frozen_at": "2025-06-01T12:00:00Z",
  "lease": {
    "token": "host1:12345:a1b2c3d4",
    "expires": "2025-06-01T12:01:00Z"
  },
  "children": ["<child-uuid-1>", "<child-uuid-2>"]
}
```

- `ancestors`: full parent chain, empty for root layers.
- `frozen_at`: present means immutable. Absent means writable.
- `lease`: timed exclusive write lease with a unique token per acquisition. Only present on mutable layers — frozen layers need no lease since they never change. See [Leases](#leases).
- `children`: updated each time a child layer is created from this one (continuation after snapshot, clone, etc.).

## Invariants

1. **A layer with children is always frozen.** `CreateChild` requires the parent to be frozen first. This ensures children inherit a stable, immutable base — no writes land on a layer after a child starts reading from it.

2. **A frozen layer is never written to again.** Once `frozen_at` is set, the layer accepts no further block writes, tombstones, or deletes. The only mutations are appending to `children` and lease changes (which are metadata-only).

3. **The ancestor chain is append-only and immutable.** A layer's `ancestors` list is written once at creation and never modified. Every layer in the chain is frozen.

4. **At most one process holds a layer's write lease.** Enforced by CAS on `state.json`. A process must hold the lease before writing blocks.

5. **At most one process holds a volume's lease.** Enforced by CAS on the volume ref. A process must hold the volume lease before opening the layer for writing or performing snapshot/clone.

6. **Volume lease is acquired before layer lease.** Prevents deadlocks when multiple processes compete for overlapping volumes and layers.

7. **A mutable volume always points to a mutable layer.** Snapshot freezes the current layer and swaps the volume to a new mutable child in a single operation. A read-only volume points to a frozen layer.

## Local Cache Layout

```
<cache-dir>/
├── <layer-uuid-A>/
│   ├── 0000000000000000           # cached block data
│   └── 0000000000000003
└── <layer-uuid-B>/
    └── 0000000000000000
```

Block files on disk are the same size as the block (sparse files for partially-written blocks). All metadata is in memory:

- **dirty set** — block indices that have been written locally but not yet uploaded to S3.
- **local index** — block indices that exist in S3 for this layer (populated at load time from S3 listing, updated after successful uploads/deletes).
- **zero-blocks set** — block indices explicitly zeroed (pending tombstone/delete on next flush).

The cache has a configurable size limit. When full, clean (non-dirty) blocks are evicted LRU-first to make room for new fetches and writes. Dirty blocks are never evicted — they must be uploaded first.

On startup, the cache directory starts empty. Blocks are fetched from S3 on demand and cached locally. On crash, unflushed dirty blocks are lost — the filesystem journal handles recovery.

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
     Layer (read/write/flush → S3)
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
3. Flush the layer (upload remaining dirty blocks to S3).
4. Close the volume (release layer and volume leases in S3).

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
  our disk cache          ← S3 blocks on local SSD (wanted, managed by us)
  S3                      ← durable storage
```

The page cache gives users fast repeated reads of their files. Our disk cache on SSD avoids repeated S3 fetches. There is no redundant RAM copy in between.

### Future: NBD

The FUSE + loop device layers can be replaced with NBD (Network Block Device), which provides a real kernel block device directly. NBD has no backing file, so there is no double-caching problem. The rest of the stack (ext4, fsfreeze) stays the same. The layer interface is transport-agnostic.

## Volume Size

Volume size is a mount-time concern, not a property of the layer or S3 state. Since S3 storage is practically unlimited, there is no reason to restrict it at the layer level — the layer simply stores whatever blocks are written to it.

The volume size determines:
- The size of the FUSE device file (or NBD export size).
- The upper bound for ext4 formatting and mount.
- The bounds check for writes at the transport layer (FUSE/NBD).

Volume size is specified via the `--size` mount option (default 1 TB, accepts human-readable sizes like `100G`, `2T`). The FUSE and NBD backends enforce bounds: reads and writes beyond the volume size are rejected at the transport layer. The layer itself performs no bounds checking — it accepts writes at any block index.

When mounting an existing volume, the volume size must be at least as large as the ext4 filesystem on it. Increasing the size is safe (the filesystem can be grown with `resize2fs`). Decreasing it below the filesystem size will cause mount failures.

## Filesystem-Level Snapshot

A snapshot creates a frozen, point-in-time copy of the filesystem that can be independently mounted (read-only) or cloned (read-write). No data is copied — only metadata.

1. **fsfreeze -f \<user-mountpoint\>** — tells ext4 to flush its journal, complete all pending writes, and stop accepting new ones. After this returns, no new writes reach the block device. The filesystem is in a clean, consistent state.
2. **Acquire exclusive volume lock** — blocks any new writes at the layer level. Writes already past fsfreeze (e.g., in-flight in the FUSE layer) drain before the lock is acquired. This is defense in depth: fsfreeze stops the filesystem, the volume lock stops the block device.
3. **Flush** — upload all remaining dirty blocks from the layer to S3. No new writes can arrive during upload.
4. **Write snapshot intent** — CAS the volume ref to add a `snapshot_pending` field with the continuation layer ID (generated now). This is the prepare phase. If we crash after this point, recovery knows a snapshot was in progress.
5. **Freeze the layer** — write `frozen_at` to `state.json`. The layer is now immutable.
6. **Create a child layer** — write a new `state.json` with `ancestors = [frozen_layer, ...frozen_layer.ancestors]` using the continuation ID from step 4. No block data is copied.
7. **Commit** — CAS the volume ref: set `layer_id` to the continuation, remove `snapshot_pending`. This is the commit point.
8. **Swap** — load a new Layer for the child (read state, list blocks, build ownership map). Point the volume at the new layer.
9. **Release exclusive volume lock** — new writes now go to the child layer.
10. **fsthaw -f \<user-mountpoint\>** — resume the filesystem.

From the user's perspective, the filesystem briefly pauses (typically < 1 second for the fsfreeze window), then continues as if nothing happened. The frozen layer in S3 is a complete, mountable ext4 image.

### Crash safety

If we crash before step 4 (snapshot intent): the layer remains mutable. No snapshot was created. On restart, the filesystem replays its journal against the layer.

If we crash after step 4 but before step 7 (commit): the volume ref has `snapshot_pending` set. On restart, recovery checks if the layer was frozen (step 5 completed). If frozen, it completes the snapshot by creating the child and updating the volume ref. If not frozen, it clears `snapshot_pending` and the layer remains mutable.

If we crash after step 7: the snapshot is fully committed. The volume ref points to the continuation layer. The frozen layer is complete and consistent (all blocks were uploaded in step 3 before freezing in step 5).

Write ordering within the upload doesn't matter — the filesystem was quiesced by fsfreeze before uploads began, so there are no in-flight writes to reorder.

## Filesystem-Level Clone

A clone creates an independent writable copy of a volume. It works differently depending on whether the source is read-only or mutable.

**From a read-only (frozen) volume:**

The layer is already frozen — no fsfreeze needed.

1. Create a child layer from the frozen layer.
2. Create a volume ref for the clone pointing to the child.
3. Mount the clone (loop device + ext4 as usual).

**From a mutable volume:**

1. **fsfreeze** the source filesystem.
2. **Acquire exclusive volume lock** on the source.
3. Snapshot the source (flush + freeze + create continuation child).
4. Create a second child from the frozen layer for the clone.
5. **Release exclusive volume lock** on the source (now writing to its continuation).
6. **fsthaw** the source filesystem.
7. Create a volume ref for the clone.
8. Mount the clone.

In both cases, no block data is copied. The clone starts with zero blocks of its own and reads everything from ancestors. Writes go to the clone's own layer.

## Layer Operations

### Read (byte offset + length → data)

1. Split the byte range into per-block slices.
2. For each block:
   a. If the block is in the zero-blocks set → return zeros.
   b. If the block is in the local disk cache → pread from the cache file.
   c. If the block is in the local index → fetch from this layer's S3 prefix (block was uploaded this session but evicted from cache).
   d. Look up the block ownership map for the S3 owner. If no owner → return zeros (never written).
   e. Fetch from the owning layer's S3 prefix. Spawn a background full-block fetch to warm the cache.

### Write (byte offset + data)

1. Reject if frozen or closed.
2. Split into per-block slices.
4. For each block:
   a. If the block was in the zero-blocks set, clear it.
   b. Ensure a cache file exists (fetch from S3 if partial write into an existing block, or create sparse).
   c. pwrite data into the cache file at the correct offset.
   d. Add block index to the dirty set.

Full-block writes of all zeros are detected and converted to zero-block operations (tombstone/delete) instead of uploading a block-sized file of zeros.

### Flush (fsync)

Triggered by fsync/fdatasync from the filesystem. Waits for the uploader to finish uploading all currently dirty blocks. Multiple concurrent flush calls coalesce via a generation counter.

### Upload Cycle (background)

Runs periodically and on flush. Uploads dirty blocks to S3, reading directly from cache files. No per-block locking — writes may continue concurrently during upload.

1. **Verify lease** — renew the layer lease via CAS before doing any work. If renewal fails (token mismatch), another process has taken over — abort the cycle and shut down.
2. **Swap dirty sets** — atomically swap the current dirty set and zero-blocks set for new empty ones. Writes that arrive during the upload go into the new sets. The upload works exclusively from the swapped-out sets.
3. Upload dirty blocks to S3 concurrently (PutFile from cache files).
4. For zero-blocks: write tombstones or delete from S3 as appropriate.
5. Update the local index.

The atomic swap ensures no dirty marks are lost: a write that lands during upload dirties the block in the *new* set, which will be picked up by the next cycle. The upload reads from cache files that may be concurrently written — the uploaded content may be stale or partially updated. This is fine and matches real block device behavior: without fsync, there is no guarantee that a write has reached durable storage. The concurrent write re-dirties the block (in the new set) and it will be uploaded again.

### Freeze

1. Upload all dirty blocks (flush).
2. Write `frozen_at` to `state.json` via CAS.
3. Set the in-memory frozen flag. Reject all future writes.

### Zero Block

Marks a block as explicitly zeroed without uploading a block-sized file of zeros:
1. Add block index to the in-memory zero-blocks set.
2. Remove the block from the local cache (delete file, remove from dirty set).
3. On next upload cycle:
   - If an ancestor has the block → write a tombstone (empty S3 object).
   - If only the local layer has it → delete the S3 object.
   - If nobody has it → nothing to do.

## Block Device Semantics

Loophole is a block device, not a filesystem. Like a real disk:

- **Writes can be reordered.** The uploader may send blocks to S3 in any order. The filesystem above (ext4) uses its journal to handle crash recovery — it does not rely on the block device preserving write order.
- **Sector-level atomicity only.** A write within a single sector (512 bytes) is atomic. Larger writes are not — a reader may see a partially-written block. This matches real disk behavior.
- **No durability without fsync.** Between fsyncs, writes live only in the local cache. They may or may not have been uploaded to S3. The filesystem is responsible for calling fsync when it needs durability.

The filesystem is responsible for its own consistency. ext4's journal ensures that metadata operations are atomic and recoverable regardless of block device write ordering.

## Leases

Mutable layers and volumes are protected by timed leases to prevent two processes from writing to the same data simultaneously. Frozen layers and read-only volumes need no leases — they are immutable and can be read by any number of processes concurrently. Leases are stored in S3 and maintained with CAS.

### Layer Lease

A mutable layer's `state.json` carries a lease:

```json
{
  "lease": {
    "token": "host1:12345:a1b2c3d4",
    "expires": "2025-06-01T12:01:00Z"
  }
}
```

**Token**: A new lease token is generated for every acquisition — `hostname:pid:uuid`, where the UUID is fresh each time. Even if the same process re-acquires a lease it previously held, it gets a new token. The token is used for all CAS operations (renew, release) to detect stale holders.

**Acquire**: CAS read-modify-write on `state.json`. Succeeds only if `lease` is absent or `expires` is in the past. On success, writes a fresh token and an expiry (now + lease duration, 60 seconds).

**Renew**: A background goroutine periodically extends `expires` via CAS (every 30 seconds with a 60-second lease). The CAS checks that `token` still matches — if it doesn't, another process stole the lease after it expired and the holder must immediately stop writing and close the layer.

**Release**: On clean unmount, CAS removes the `lease` field, checking that `token` matches.

**Crash recovery**: If the holder crashes, it stops renewing. After `expires` passes, any other process can acquire the lease with a new token. Dirty blocks from the crashed session are lost — the filesystem journal handles recovery on the next mount.

### Volume Lease

The volume ref carries its own lease:

```json
{
  "layer_id": "<uuid>",
  "lease": {
    "token": "host1:12345:a1b2c3d4",
    "expires": "2025-06-01T12:01:00Z"
  }
}
```

The volume lease protects volume-level operations: snapshot, clone, and the volume ref update that accompanies them. Without it, two processes could both read the same volume ref, both try to freeze the same layer, and race on the CAS — one would fail with a confusing error.

Read-only volumes (pointing to a frozen layer) need no lease — any process can mount them for reading without coordination.

Acquire, renew, release, and crash recovery work identically to layer leases — each acquisition generates a fresh token. A process must hold the volume lease before opening the volume's layer for writing.

### Fencing

Leases are *advisory* — S3 has no way to reject a PutObject from a process whose lease expired. Safety relies on two mechanisms:

1. **Lease check before each upload cycle.** The uploader renews the layer lease via CAS at the start of every cycle (before uploading any blocks). If renewal fails, the process knows it lost the lease and shuts down without uploading. This ensures a stale process does not upload blocks after another process has taken over, as long as it hasn't been paused for longer than one full lease period (60 seconds).

2. **Renewal loop.** The background goroutine renews the lease every 30 seconds. If renewal fails (token mismatch), the process stops writing immediately.

There is a residual risk: if a process is paused for longer than the full lease duration (60 seconds) — e.g., a long GC pause or VM suspend — it may not detect the lease loss before its next upload cycle starts. During this window a stale upload could overwrite blocks written by the new lease holder. In practice this is bounded by the lease duration and requires an unusual pause. The new lease holder's filesystem will journal-replay on mount, which may re-issue overwritten writes. The layer lease CAS on `state.json` remains the true serialization point for freeze/snapshot — even if two processes write blocks, only one can freeze the layer.

## Concurrency

- **Upload/download semaphores**: bound concurrent S3 operations (default 20 uploads, 200 downloads).
- **Volume mutex (RWMutex)**: shared (read) lock held during normal I/O, exclusive (write) lock held during snapshot/clone. The exclusive lock drains in-flight writes before the layer swap begins. This is the block-level complement to fsfreeze — together they ensure no writes land on the frozen layer.
- **Leases (CAS)**: timed leases on volume refs and layer `state.json` prevent two VolumeManager instances from writing to the same volume or layer simultaneously. See [Leases](#leases).

No per-block locks are needed. Like a real block device, concurrent writes to the same block result in last-writer-wins. The filesystem above is responsible for its own synchronization.

## Daemon and RPC

Loophole is a single binary that acts as both the daemon and the CLI.

### Daemon

`loophole start` starts the FUSE server in the foreground (like JuiceFS). It creates an internal FUSE mountpoint at `/mnt/loophole` (if writable) or `~/.loophole/fuse/`. The FUSE mountpoint is an implementation detail — users don't interact with it directly. The daemon also listens on a Unix domain socket for RPC. It can manage multiple volumes, each appearing as a separate device file under the internal FUSE mount. All volumes share a single disk cache and a single VolumeManager.

For NBD mode, `loophole nbd` starts an NBD server on a single port. Multiple volumes are served as named exports — the client specifies the volume name during the NBD handshake.

`loophole mount` auto-starts the daemon if no existing socket is found.

### RPC Socket

The daemon listens on an HTTP server over a Unix domain socket. The default path is derived from a hash of the S3 base URL: `/run/loophole/<hash>.sock` (or `~/.loophole/<hash>.sock` if `/run` is not writable). This means each S3 base gets its own daemon — different bases cannot share cache or volumes, so they must be separate instances. The path can be overridden with `--sock <path>`.

Operations that cannot be expressed as FUSE operations (mounting a new volume, snapshot, clone, unmount) go through this RPC interface.

### CLI

The CLI has two levels: high-level commands that manage the full filesystem stack (loop device + ext4 + fsfreeze), and low-level `device` commands that operate on the raw block device files only.

**High-level (filesystem):**

```
loophole start s3://bucket/prefix              # start daemon only (foreground)
loophole start s3://bucket/prefix -d           # start daemon only (background)

loophole mount s3://bucket/prefix myvolume /mnt/myfiles    # mount volume's ext4 at /mnt/myfiles
loophole unmount /mnt/myvolume                               # umount ext4, losetup detach, flush, close
loophole snapshot /mnt/myvolume snapname                     # fsfreeze + snapshot + fsthaw
loophole clone /mnt/myvolume clonename /mnt/myclone          # clone + mount at /mnt/myclone
loophole clone s3://bucket/prefix snapname clonename /mnt/myclone   # clone from unmounted frozen snapshot
loophole status s3://bucket/prefix                           # list mounted volumes, cache stats
```

`loophole mount` takes an S3 URL (root of the loophole system), a volume name, and a mountpoint for the ext4 filesystem. The S3 URL is required on every command — it is hashed to find the correct daemon socket. If no daemon is running for that S3 base, `mount` auto-starts one in the background, waits for the socket, then issues the mount RPC. This means `loophole mount` is the typical single command to go from nothing to a mounted filesystem.

`loophole start` is available for starting the daemon without mounting anything.

**Low-level (block device):**

```
loophole device start s3://bucket/prefix                     # start daemon only (foreground)
loophole device mount s3://bucket/prefix myvolume            # open volume, create device file
loophole device unmount s3://bucket/prefix myvolume          # flush, close
loophole device snapshot s3://bucket/prefix myvolume snap    # flush + freeze + create child
loophole device clone s3://bucket/prefix myvolume clone      # flush + freeze + clone
```

The low-level commands are for users who want to manage the loop device and filesystem themselves, or use NBD. The high-level commands perform the same layer operations in-process and add the ext4/loop/fsfreeze orchestration on top — they do not shell out to the low-level CLI.

Commands that create or connect to a daemon take the S3 URL as their first argument. The URL is hashed to derive the socket path. When a volume is mounted, the daemon creates a symlink at `~/.loophole/mounts/<hash-of-mountpoint>.sock` pointing to the daemon socket. This allows post-mount commands (snapshot, clone, unmount) to accept a mountpoint instead of the S3 URL — the mountpoint is hashed to find the symlink, which resolves to the correct socket. `--sock <path>` overrides all of this.

All commands except `start` and `mount` (which may auto-start) are RPC calls to an already-running daemon.

### CLI Workflows

**Create and use a new volume:**

```
$ loophole mount s3://mybucket/loophole myfiles /mnt/myfiles
# daemon auto-starts, creates volume in S3, formats ext4, mounts at /mnt/myfiles
$ echo "hello" > /mnt/myfiles/greeting.txt
```

**Snapshot before a risky change:**

```
$ loophole snapshot /mnt/myfiles before-migration
$ run-migration.sh /mnt/myfiles
# if migration fails:
$ loophole unmount /mnt/myfiles
$ loophole clone s3://mybucket/loophole before-migration myfiles-restored /mnt/myfiles
$ ls /mnt/myfiles/   # back to pre-migration state
```

**Clone a production dataset for testing:**

```
$ loophole snapshot /mnt/prod-db weekly-snap
$ loophole clone /mnt/prod-db weekly-snap test-db /mnt/test-db
# test-db is an independent writable copy — no data was copied
$ psql -h /mnt/test-db ...
```

**Mount an existing volume on a new machine:**

```
$ loophole mount s3://mybucket/loophole myfiles /mnt/myfiles
# volume ref found in S3, opens existing layer, mounts ext4
# all data is fetched from S3 on demand
```

**Multiple volumes, shared cache:**

```
$ loophole mount s3://mybucket/loophole vol-a /mnt/vol-a
$ loophole mount s3://mybucket/loophole vol-b /mnt/vol-b
$ loophole status s3://mybucket/loophole
# both volumes share the same daemon, internal FUSE mount, and disk cache
# (same S3 URL hashes to same socket — connects to existing daemon)
```

**Low-level: raw block device for custom use:**

```
$ S3=s3://mybucket/loophole
$ loophole device start $S3
$ loophole device mount $S3 myvolume
# device file created under internal FUSE mount
$ losetup --find --show ~/.loophole/fuse/myvolume/device
/dev/loop0
$ mkfs.xfs /dev/loop0   # use any filesystem, not just ext4
$ mount /dev/loop0 /mnt/custom
```

### RPC Endpoints

All endpoints are HTTP JSON. Errors are returned as non-2xx status codes with a JSON body.

**High-level (filesystem):**

| Method | Path | Description |
|---|---|---|
| POST | `/mount` | Open volume + losetup + mkfs (if new) + mount ext4 |
| POST | `/unmount` | umount ext4 + losetup detach + flush + close |
| POST | `/snapshot` | fsfreeze + snapshot + fsthaw |
| POST | `/clone` | fsfreeze + snapshot + clone + fsthaw (or clone-only if source is frozen) |

**Low-level (block device):**

| Method | Path | Description |
|---|---|---|
| POST | `/device/mount` | Open volume, create device file |
| POST | `/device/unmount` | Flush, close, remove device file |
| POST | `/device/snapshot` | Flush + freeze + create child |
| POST | `/device/clone` | Flush + freeze + clone |

**Status:**

| Method | Path | Description |
|---|---|---|
| GET | `/volumes` | List mounted volumes with status |
| GET | `/status` | Daemon health, cache stats, lease info |

### Lifecycle

1. `loophole mount s3://mybucket/lh myfiles /mnt/myfiles` — hashes the S3 URL, checks for socket, none found, so auto-starts daemon in background (internal FUSE mount + socket), then issues mount RPC.
2. Daemon opens volume from S3, creates device file under internal FUSE mount, runs `losetup` + `mkfs.ext4` (if new) + `mount` at `/mnt/myfiles`.
3. User works with files in `/mnt/myfiles`.
4. `loophole snapshot /mnt/myfiles snap1` — finds socket via mountpoint symlink, daemon does fsfreeze/snapshot/fsthaw via RPC.
5. `loophole unmount /mnt/myfiles` — daemon unmounts `/mnt/myfiles`, detaches loop, flushes, closes, removes mountpoint symlink. If no volumes remain mounted, the daemon exits automatically.
6. Ctrl-C or SIGTERM to daemon — unmounts all volumes, flushes, releases leases, exits.

## Key Modules

| File | Purpose |
|---|---|
| `volume_manager.go` | Creates/opens volumes, deduplicates layer instances, manages volume refs |
| `volume.go` | Named volume with snapshot/clone operations, protects layer swaps |
| `layer.go` | Core I/O: read/write/flush/freeze, block ownership map, S3 state management |
| `object_store.go` | ObjectStore interface (S3 abstraction), ModifyJSON CAS helper |
| `cache.go` | DiskCache: file-based block cache with LRU eviction |
| `uploader.go` | Background goroutine: uploads dirty blocks and executes zero ops |
| `daemon.go` | HTTP/UDS server, RPC handlers, volume lifecycle orchestration |
| `cli.go` | CLI argument parsing, RPC client, daemon startup |
