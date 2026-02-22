# Replace filesystem cache with SQLite via Turso (chunked blobs, S3-authoritative startup)

## Context

The current `DiskCache` stores each 4MB block as a separate file with `.dirty`/`.uploading`/`.pending` marker files. Replace with a single SQLite database using the Turso crate (native async, pure Rust). Blocks are split into 16KB chunks stored as inline blobs (64KB page size). Missing chunks are implicitly zero (sparse). Never construct 4MB on heap.

Important correction: startup block ownership/tombstone truth remains S3-authoritative. We still run `list_blocks()` for the mounted store and its ancestors at load time.

## Schema

```sql
PRAGMA page_size=65536;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

-- Map string volume names to compact integer IDs.
CREATE TABLE IF NOT EXISTS volumes (
    volume_id        INTEGER PRIMARY KEY,
    volume_name      TEXT    NOT NULL UNIQUE,
    parent_volume_id INTEGER REFERENCES volumes(volume_id)
);

-- Map (volume, block_idx) to a compact integer block ID.
CREATE TABLE IF NOT EXISTS blocks (
    block_id         INTEGER PRIMARY KEY,
    volume_id        INTEGER NOT NULL REFERENCES volumes(volume_id),
    block_idx        INTEGER NOT NULL,
    -- Physical bytes stored in chunks table only (NOT logical block length).
    size             INTEGER NOT NULL DEFAULT 0,
    downloaded_thru  INTEGER,   -- NULL = fully authoritative
    dirty_at         INTEGER,   -- NULL = clean
    uploading_at     INTEGER,   -- NULL = not uploading
    UNIQUE (volume_id, block_idx)
);

-- 16KB chunk data keyed by integer block ID + chunk index.
CREATE TABLE IF NOT EXISTS chunks (
    block_id  INTEGER NOT NULL REFERENCES blocks(block_id) ON DELETE CASCADE,
    chunk_idx INTEGER NOT NULL,
    data      BLOB    NOT NULL,
    PRIMARY KEY (block_id, chunk_idx)
) WITHOUT ROWID;

-- Durable queue for zero-block S3 operations (tombstone/delete) so crashes
-- cannot lose a required remote transition.
CREATE TABLE IF NOT EXISTS pending_zero_ops (
    volume_id   INTEGER NOT NULL REFERENCES volumes(volume_id),
    block_idx   INTEGER NOT NULL,
    op          INTEGER NOT NULL, -- 0=tombstone, 1=delete
    enqueued_at INTEGER NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (volume_id, block_idx)
) WITHOUT ROWID;
```

- Hot-path keys are integers.
- `chunks` is sparse: absent row means zeros.
- `blocks.size` is physical cache usage (`non_zero_chunk_count * CHUNK_SIZE`) for LRU accounting only.
- `pending_zero_ops` is crash-recovery critical.
- `parent_volume_id` is optional metadata; if kept, it must be upserted after parent rows exist.

**Constants:** `CHUNK_SIZE = 16384`, chunks per block = `block_size / CHUNK_SIZE` (256 for 4MB blocks).

## Files to modify

### 1. `Cargo.toml`
- Add `turso = "0.5.0-pre.13"`

### 2. `src/cache.rs` — complete rewrite

**Struct:**
```rust
pub struct SqliteCache {
    db: turso::Database,
    volume_ids: DashMap<String, i64>,      // store_name -> volume_id
    lru: Mutex<LruCache<(i64, u64), LruEntry>>,
    used_bytes: AtomicU64,
    max_bytes: u64,
}
```

Turso API (all natively async):
- `turso::Builder::new_local("path").build().await` -> `Database`
- `db.connect()` -> `Connection`
- `conn.execute(sql, params).await` -> `u64`
- `conn.query(sql, params).await` -> `Rows`

**`init(dir: PathBuf, max_bytes: u64)` -> async**
- Create `dir/cache.db`, set PRAGMAs, create tables.

**`init_store(store_name, parent_store_name, block_locks)` -> async**
- Ensure row exists for `store_name`.
- If `parent_store_name` is known and present, upsert `parent_volume_id`.
- Populate `volume_ids` cache and LRU from `blocks`.

**`read(store_name, block_idx, offset, len)` -> async -> `Option<Result<Vec<u8>>>`**
- Same cache-hit/miss contract as now.
- Respect `downloaded_thru` watermark for partial background downloads.
- Missing chunks within authoritative range read as zeros.

**`pwrite(store_name, block_idx, offset, data)` -> async**
- Chunk-level RMW (`INSERT/REPLACE`), delete all-zero chunks, update `blocks.size`.

**`insert_from_stream(store_name, block_idx, block_locks, reader)` -> async -> `Result<u64>`**
- Stream in 16KB chunks, sparse-store non-zero chunks only.
- `downloaded_thru` tracks in-progress fetch; set to `NULL` on completion.

**`ensure_sparse_block(store_name, block_idx, block_size, block_locks)` -> async**
- `INSERT OR IGNORE` block row with `size=0`, no chunks.

**`set_block_len(store_name, block_idx, new_len, block_locks)` -> async**
- Shrink: delete chunks above new length.
- Grow: no chunk writes, no `size` increase (physical size unchanged).

**`has_block(store_name, block_idx)` -> async -> `bool`**

**`try_mark_dirty(store_name, block_idx)` -> async -> `bool`**
- Atomic `dirty_at` transition replacing marker-file `create_new(true)`.

**`prepare_upload(store_name, block_idx, block_size)` -> async -> `Result<PreparedUpload>`**
- Transaction: set `dirty_at=NULL`, `uploading_at=unixepoch()`.
- Return chunk stream + metadata (`non_zero_bytes`, `block_size`) for zero-upload guard.

**`clear_dirty(store_name, block_idx)` -> async**
- Explicit API (used by uploader) to clear stale dirty status.

**`mark_dirty(store_name, block_idx)` -> async**
- Re-mark dirty after failed upload.

**`clear_uploading(store_name, block_idx)` -> async**
- Clear in-flight flag after success/failure handling.

**`remove_block(store_name, block_idx)` -> async**
- Delete block row + chunks, remove from LRU.

**`queue_zero_op(store_name, block_idx, op)` -> async**
- Insert/update `pending_zero_ops` row in same transaction as local zero/unzero transition.

**`cancel_zero_op(store_name, block_idx)` -> async**
- Delete any stale queued zero op when a block becomes non-zero again.

**`recover(store_name)` -> async -> `Result<RecoveryWork>`**
- Delete incomplete downloads (`downloaded_thru IS NOT NULL`).
- Requeue dirty/uploading blocks.
- Return pending zero ops to replay.

**`evict_if_needed()` -> async**
- Same LRU strategy, skip dirty/uploading blocks.

### 3. `src/store.rs` — update cache interactions

**`Store::load`:**
- Keep `list_blocks()` for mounted store and ancestors (S3 truth).
- Build `local_index` and tombstones from S3 exactly as today.
- Reconcile startup in-memory zero state from:
  - S3 tombstones for current store.
  - Pending local zero ops from DB recovery.
- `cache::get().init_store(...).await`, `cache::get().recover(...).await`.
- Do not remove S3 startup indexing.

**`do_zero_block`:**
- Under lock, remove local block from cache, update in-memory `zero_blocks`.
- Queue durable zero op (`tombstone` if ancestor has block, `delete` if local-only).
- Enqueue uploader work item.

**`do_write_block`:**
- Replace path checks with `has_block`.
- Replace dirty marker creation with `try_mark_dirty`.
- On first non-zero write to previously zeroed block:
  - Remove from `zero_blocks`.
  - `cancel_zero_op` for that block (prevents stale zero replay).

**`do_read_block`:**
- Cache read becomes async.
- Behavior unchanged: zeroed blocks return zeros before cache/S3 lookup.

### 4. `src/uploader.rs` — stream uploads and keep safety invariants

- `prepare_upload()` returns `PreparedUpload` stream, not path.
- Zero op replay path processes `pending_zero_ops` durably.
- Replace marker-file cleanup with `clear_dirty`/`clear_uploading`.
- Preserve zero-upload safety assertion:
  - If upload candidate has `non_zero_bytes == 0` and block is not logically zeroed, treat as invariant violation or route to zero-op path explicitly.

### 5. `src/s3.rs` — add streaming upload without losing assertions

Add to `S3Access` trait:
```rust
fn put_stream<'a>(
    &'a self,
    bucket: &'a str,
    key: &'a str,
    body: ByteStream,
) -> impl Future<Output = Result<()>> + Send + 'a;
```

- Real `Client` impl: `put_object().bucket(b).key(k).body(body).send()`.
- `MockS3` impl: collect stream to `Vec<u8>`.
- Keep assertion behavior currently in `upload_block()` by moving it to uploader preflight or a new streaming helper.

### 6. `src/main.rs`
- `cache::init(cache_dir, cache_size)?` -> `cache::init(cache_dir, cache_size).await?`

### 7. `src/store_tests.rs` and `src/cache_tests.rs`
- `ensure_global_cache()` -> async (`tokio::sync::OnceCell`).
- Replace filesystem marker assertions with DB-backed API assertions where needed.
- Add recovery tests for `pending_zero_ops`.

## Key design decisions (corrected)

1. 16KB sparse chunks in SQLite reduce write amplification.
2. Streaming upload/download avoids 4MB heap buffers.
3. Startup ownership and tombstones remain S3-authoritative.
4. Durable `pending_zero_ops` guarantees crash-safe zero-op replay.
5. LRU tracks physical bytes only (`blocks.size`), not logical block length.
6. WAL mode for read/write concurrency.
7. Parent linkage metadata is optional and must not depend on incorrect init order.

## Verification

1. `cargo build`
2. `cargo test`
3. `./test-e2e.sh`
4. New targeted regressions:
   - Crash after local zero persist but before S3 op -> zero op replayed on restart.
   - Stale/missing SQLite zero state on cold start still reads correctly from S3 tombstones.
   - `set_block_len` grow does not increase `used_bytes`.
   - Parent linkage is correct if enabled.
