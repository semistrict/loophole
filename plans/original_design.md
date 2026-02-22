# loophole design

A FUSE filesystem that exposes a single large virtual file backed by S3, with
support for instant copy-on-write clones that share unchanged data.

---

## S3 layout

```
s3://<bucket>/<prefix>/stores/<store-id>/state.json
s3://<bucket>/<prefix>/stores/<store-id>/<block-idx-hex>
```

- `prefix` is an operator-controlled namespace (e.g. `myproject`). Resolved
  internally as `<prefix>/stores` (or just `stores` if empty).
- Block index is zero-padded 16-char hex (`%016x`).
- Blocks are simply overwritten on update — no hash in the name.
- Only one mount of a mutable store is supported at a time.

### state.json

```json
{
  "parent_id": "<store-id or null>",
  "block_size": 4194304,
  "volume_size": 1099511627776,
  "children": []
}
```

- `children: []` → store is **mutable**
- `children: [...]` → store is **frozen** (no further writes)

---

## Stores, snapshots, clones

Everything is a **store**. There is no separate snapshot or clone type.

**Snapshot** (freeze current state, continue writing):
1. Acquire `write_lock.write()` — blocks until all in-flight writes complete
2. Flush all dirty blocks to S3 (drain upload queue, wait for completion;
   return error to caller if any upload fails permanently)
3. Create child store B with `parent_id = A`
4. Append B to `A.children` → A is now frozen
5. Release `write_lock.write()`
6. Writer switches to B

**Clone** (fork to two independent writers):
1. Acquire `write_lock.write()` — blocks until all in-flight writes complete
2. Flush all dirty blocks to S3 (drain upload queue, wait for completion;
   return error to caller if any upload fails permanently)
3. Create child stores B and C both with `parent_id = A`
4. `A.children = [B, C]` → A is now frozen
5. Release `write_lock.write()`
6. Original writer uses B; clone uses C

No data is ever copied. Blocks stay exactly where they were written.

---

## Block index

At mount time, LIST `stores/<store-id>/` for the mounted store and each
ancestor store, building an in-memory index:

```
BlockIndex: HashSet<u64>   // which block indices exist in this store in S3
```

One `BlockIndex` per store in the ancestor chain. This is an S3-existence
index only — it does not track what is locally cached.

---

## Block read path

On `read(offset, len)`:

```
block_idx            = offset / block_size
offset_within_block  = offset % block_size

store_id = first store in chain (local → ancestors) whose index contains block_idx
         ?? None  →  return zeros

key = stores/<store_id>/<block_idx_hex>

cache hit?  (open(<cache_dir>/<store_id>/<block_idx_hex>) succeeds)
  → pread(fd, buf, offset_within_block, len)   ← fd kept open across pread;
  → close fd                                      safe against concurrent eviction unlink
      [OS page cache may serve from RAM]

cache miss?  (open returns ENOENT)
  → HTTP Range GET from S3: Range: bytes=<offset_within_block>-<offset_within_block+len-1>
      [small allocation, only the bytes needed]
  → if (store_id, block_idx) ∈ inflight_downloads → skip (already being fetched)
    if download_slots.try_acquire() fails → skip (at capacity, not queued)
    insert (store_id, block_idx) into inflight_downloads
    spawn background task:
      acquire block_locks[block_idx]
      stream full GET from S3 → <block_idx_hex>.pending   [lock held during stream]
      rename .pending → <block_idx_hex>                   [atomic]
      release block_locks[block_idx]
      remove (store_id, block_idx) from inflight_downloads
      release download_slots permit
      [writes to this block wait until fetch completes]
```

---

## Block write path

Writes are **write-back**: data is committed to the local disk cache
immediately, then uploaded to S3 asynchronously. FUSE sees a fast local write.

### Locking primitives

```rust
// in Store
write_lock: tokio::sync::RwLock<()>,                    // guards all writes to this store
block_locks: DashMap<u64, Arc<tokio::sync::Mutex<()>>>, // guards individual block upload/fetch
upload_slots: tokio::sync::Semaphore,                   // limits concurrent S3 uploads (default: 20)
download_slots: tokio::sync::Semaphore,                 // limits concurrent background S3 reads (default: 200)
inflight_downloads: DashSet<(StoreId, u64)>,            // deduplicates in-flight background fetches
```

Normal writes take `write_lock.read()` (shared — many concurrent writes allowed).
Snapshot/clone takes `write_lock.write()` (exclusive — waits for all in-flight
writes to finish, then blocks new ones while flushing and freezing).

### Write sequence

```
block_idx            = offset / block_size
offset_within_block  = offset % block_size

acquire write_lock.read()                                ← blocks if snapshot/clone in progress
acquire block_locks[block_idx]                           ← waits if uploader or fetch holds it

ensure <cache_dir>/<store_id>/<block_idx_hex> exists:
  already in cache?   → nothing to do
  not in cache?
    ancestor has it?  → stream full block from S3 → <block_idx_hex>.pending, rename into place
    new block?        → fallocate <block_idx_hex> to block_size (sparse zeros)

open(<block_idx_hex>.dirty, O_CREAT|O_EXCL)              ← create dirty marker BEFORE pwrite
dirty_is_new = succeeded?

pwrite(<cache_dir>/<store_id>/<block_idx_hex>, data, offset_within_block)
fdatasync(<block_idx_hex>)                               ← ensure data survives power failure

release block_locks[block_idx]

if dirty_is_new:
  upload_tx.send(block_idx)   ← only enqueue if we created the marker; already-dirty
                                 blocks are already queued, uploader picks up latest data

release write_lock.read()     ← must be held through upload_tx.send so snapshot flush
                                 cannot miss a block that was pwritten but not yet queued

reply.written() to FUSE
```

### Background uploader

```
recv block_idx from upload_tx
acquire block_locks[block_idx]
cp --reflink=auto <block_idx_hex> → <block_idx_hex>.uploading   ← reflink if supported, else copy
delete <block_idx_hex>.dirty                                     ← remove marker only after copy is complete
release block_locks[block_idx]                                   ← writes to this block can resume

acquire upload_slots permit                                      ← blocks if max concurrent uploads reached
stream <block_idx_hex>.uploading → S3 put_object                ← no lock held during upload
on success:
  delete <block_idx_hex>.uploading
  update local_index (add block_idx to this store's set)
  release upload_slots permit
on failure:
  touch <block_idx_hex>.dirty                                    ← re-mark dirty
  delete <block_idx_hex>.uploading
  release upload_slots permit
  re-enqueue to upload_tx with exponential backoff               ← retry, do not silently drop
```

If a write arrives while the upload is in flight:
- Lock is free → write proceeds immediately to `<block_idx_hex>`
- `O_CREAT|O_EXCL` on `.dirty` succeeds (marker was deleted by uploader) → re-enqueues
- The in-flight upload continues from its `.uploading` snapshot — no conflict

### Startup recovery

Scan cache dir for:
- `*.dirty` files → verify data file exists; if missing, remove orphaned marker and skip;
  otherwise enqueue block_idx for upload
- `*.uploading` files → delete them (may be a partial copy); if `*.dirty` does not exist,
  recreate it so the normal upload path re-copies from the data file and re-uploads

---

## Disk cache

```
<cache_dir>/
  <store-id>/
    <block_idx_hex>          ← block data (readable regardless of dirty state)
    <block_idx_hex>.dirty    ← empty marker: block not yet uploaded to S3
    <block_idx_hex>.uploading ← copy of block currently being streamed to S3
    <block_idx_hex>.pending  ← partial background fetch in progress (deleted on crash recovery)
```

Each store in the ancestor chain gets its own subdirectory. Blocks fetched
from an ancestor are cached under that ancestor's store ID.

- OS page cache transparently keeps hot blocks in RAM. No manual memory cache.
- All block states are readable via `pread` on the data file.

### Cache lookup

`cache_read(store_id, block_idx, offset_within_block, len) -> Option<Vec<u8>>`
- `open(<block_idx_hex>)` — returns `None` if `ENOENT`
- `pread(fd, buf, offset_within_block, len)` → return exact bytes
- Close fd after read
- The open fd survives a concurrent eviction `unlink` on Linux (fd remains valid)

### Cache eviction

Tracked via `AtomicU64 used_bytes` (initialized by scanning dir at startup,
maintained incrementally on insert/delete).

`evict_if_needed()` called after each cache insert:
- If `max_bytes == 0`: unlimited, skip
- If `used_bytes <= max_bytes`: skip
- Scan dir, collect `(mtime, path, size)` for **clean** files only
  (skip any block that has a `.dirty` or `.uploading` sibling — never evict these)
- Sort by mtime ascending (oldest first)
- Delete oldest until `used_bytes <= max_bytes * 0.9`

### Ownership

`DiskCache` is plain-embedded in `Store`. Background tasks receive only what
they need at spawn time: `PathBuf` (cache dir), `Client`, `bucket`, `prefix`,
`store_id`.

```rust
pub struct Store {
    cache: DiskCache,
    upload_tx: mpsc::Sender<u64>,
    write_lock: tokio::sync::RwLock<()>,
    block_locks: DashMap<u64, Arc<tokio::sync::Mutex<()>>>,
    upload_slots: Arc<tokio::sync::Semaphore>,
    download_slots: Arc<tokio::sync::Semaphore>,
    inflight_downloads: Arc<DashSet<(String, u64)>>,
    ...
}
```

---

## CLI

```
loophole [OPTIONS] --bucket <BUCKET> --store <STORE> --cache-dir <DIR> <MOUNTPOINT>

Options:
  --bucket         S3 bucket name
  --prefix         Global key prefix [default: ""]
  --store          Store ID to mount
  --cache-dir      Local directory for caching blocks (required)
  --cache-size     Max cache size, e.g. 10G [default: 0 = unlimited]
  --max-uploads    Max concurrent S3 block uploads [default: 20]
  --max-downloads  Max concurrent background S3 block fetches [default: 200]
                   If limit is reached, background fetch is skipped (not queued)
```

---

## Module layout

```
src/
  main.rs     CLI parsing, init logging, load store, mount FUSE
  store.rs    Store struct, block index, read_block, write_block, snapshot, clone
  cache.rs    DiskCache, pread/pwrite, eviction
  fs.rs       FUSE Filesystem impl (lookup, getattr, read, write)
```

---

## Key properties

| Property | Value |
|---|---|
| S3 object size | `block_size` (default 4 MB) |
| Cache granularity | one file per block (= S3 object size) |
| Read allocation | only the bytes requested (range read or pread) |
| Write allocation | only the bytes written (pwrite to cache file) |
| Full-block heap alloc | never |
| Write durability | after `fdatasync` of cache file (power-failure safe) |
| Clone cost | two `state.json` writes + one read (zero data copied) |
| Concurrent mounts | one mutable mount per store |
