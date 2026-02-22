# CLAUDE.md

This file provides guidance to coding agents when working with code in this repository.

## What This Is

Loophole is a FUSE filesystem that exposes a single large virtual file (`/volume`) backed by S3, with instant copy-on-write clones. No data is ever copied during snapshot/clone — only metadata (state.json) is written. It layers: FUSE → loop device → ext4, giving you a full filesystem backed by S3.

## Build & Test Commands

```bash
# Build
cargo build
cargo build --release

# Run Rust unit tests (store_tests.rs, cache_tests.rs)
cargo test
cargo test <test_name>           # single test

# E2E tests (Python/pytest, run inside Docker)
./test-e2e.sh                    # all e2e tests
./test-e2e.sh tests/test_store.py -v                # one file
./test-e2e.sh tests/test_clone.py::test_name -v     # one test

# Docker services (RustFS as S3 mock)
docker compose build
docker compose up -d
docker compose exec -w /tests e2e pytest /tests/ -v
```

## Architecture

### Module Layout

- **`src/main.rs`** — CLI (clap derive). Two command levels: high-level (`format`, `mount`, `snapshot`, `clone`) and low-level (`store format`, `store mount`, `store snapshot`, `store clone`). High-level commands manage the full FUSE→loop→ext4 stack.
- **`src/store.rs`** — Core `Store<S: S3Access>` struct. All block I/O, snapshot/clone logic, write-back cache coordination. The `S3Access` trait is generic for testing with `MockS3`.
- **`src/cache.rs`** — `DiskCache` singleton. LRU eviction of clean blocks, manages block files and `.dirty`/`.uploading`/`.pending` markers.
- **`src/fs.rs`** — FUSE implementation. Virtual filesystem with `/volume` (block device file), `/.loophole/rpc` (control endpoint). Splits reads/writes across block boundaries.
- **`src/rpc.rs`** — JSON-based RPC over the FUSE control file for snapshot/clone from inside a mount.
- **`src/s3.rs`** — `S3Access` trait + `Client` impl. State.json management, block index (list of S3 keys), put/get/delete operations.
- **`src/uploader.rs`** — Background uploader. Receives block indices via channel, copies `.dirty` → `.uploading`, streams to S3, retries with backoff (max 5).
- **`src/assert.rs`** — Custom assertions controlled by env var.

### S3 Layout

```
s3://bucket/[prefix/]stores/<store-id>/state.json
s3://bucket/[prefix/]stores/<store-id>/<block-idx-hex>   (16-char zero-padded)
```

### Key Data Flow

**Read path** (`do_read_block`): zero_blocks → local cache → ancestor caches → S3 index (local then ancestors) → range GET + background full-block fetch → zeros if not found.

**Write path** (`do_write_block`): acquire write_lock(shared) → block lock → ensure cache file → create `.dirty` marker (O_CREAT|O_EXCL before pwrite for crash safety) → pwrite + fdatasync → enqueue upload.

**Snapshot/Clone**: acquire write_lock(exclusive) → flush all dirty blocks → write new state.json(s) → set frozen=true.

### Concurrency Model

- `write_lock: RwLock` — shared for writes, exclusive for snapshot/clone (drains in-flight writes)
- `block_locks: DashMap<u64, Arc<Mutex>>` — per-block, prevents concurrent writes/fetches to same block
- `Semaphore` — bounded concurrency for uploads (20) and downloads (200)
- `DashSet` — lock-free sets for inflight_downloads, pending_uploads, zero_blocks

### Cache File Layout

```
<cache-dir>/<store-id>/<block-idx-hex>              # block data
<cache-dir>/<store-id>/<block-idx-hex>.dirty         # not yet uploaded
<cache-dir>/<store-id>/<block-idx-hex>.uploading     # copy being sent to S3
<cache-dir>/<store-id>/<block-idx-hex>.pending       # partial background fetch
```

### Testing

- **Rust unit tests** (`src/store_tests.rs`, `src/cache_tests.rs`): use `MockS3` (in-memory). ~50 tests covering read/write, block boundaries, ancestors, snapshots, clones, zero-blocks, tombstones, crash recovery.
- **Python E2E tests** (`tests/`): pytest with fixtures in `conftest.py`. Three fixture types: `fuse` (raw FUSE mount), `ext4` (loop+ext4 on FUSE volume), `hl_mount` (high-level format+mount). Run inside Docker with RustFS as S3 mock.

### Defaults

- Block size: 4MB
- Volume size: 1TB (configurable via `parse_size()`: "1G", "4M", etc.)
- Upload slots: 20, download slots: 200
