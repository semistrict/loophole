# CLAUDE.md

This file provides guidance to coding agents when working with code in this repository.

## What This Is

Loophole is a FUSE filesystem that exposes a single large virtual file (`/volume`) backed by S3, with instant copy-on-write clones. No data is ever copied during snapshot/clone — only metadata (state.json) is written. On Linux it layers FUSE → loop device → ext4. On macOS it uses an NFS server → lwext4, giving you a full filesystem backed by S3 on both platforms.

## Build & Test Commands

```bash
# Build
cargo build
cargo build --features nfs    # include NFS server (required on macOS)

# Install nextest once (preferred Rust test runner)
cargo install cargo-nextest --locked

# Run Rust tests (default)
cargo nextest run
cargo nextest run <test_name>    # single test filter
cargo nt                          # alias for `cargo nextest run`

# E2E tests (Python/pytest)
cargo e2e                    # all e2e tests
cargo e2e test_store         # filter by name

# macOS one-time setup for e2e (NFS mount needs sudo):
# Add to /etc/sudoers via `sudo visudo`:
#   <username> ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount

# E2E on Linux: runs inside Docker (docker compose build + exec)
# E2E on macOS: runs natively, starts S3 via docker compose, uses uv for Python deps
```

## Dependency Notes

- `ext4-lwext4` and `ext4-lwext4-sys` are vendored under `vendor/ext4-rs/` and overridden via `[patch.crates-io]` in `Cargo.toml`.
- Reason: crates.io `ext4-lwext4` `0.1.1` has a `mkfs` NULL `ext4_fs` bug that can hang forever inside lwext4 assert loops.
- Prefer keeping the vendored override until an upstream crates.io release includes commit `c4503df5c507748c6d037fbfc47f6f9171faa6f4` (or equivalent fix).
- Prefer `cargo nextest run` over `cargo test` for Rust tests. lwext4 has low per-process mount/device limits; `nextest` isolates tests in separate processes and avoids `NoSpace` flakiness from shared in-process globals.
- Python test dependencies (`pytest`, `boto3`) are managed by `uv` via `tests/pyproject.toml`.

## Shell Notes

- In this environment, `rm -r` works but `rm -rf` does not. Prefer `rm -r`.

## Architecture

### Module Layout

- **`src/main.rs`** — CLI (clap derive). Two command levels: high-level (`format`, `mount`, `snapshot`, `clone`) and low-level (`store format`, `store mount`, `store snapshot`, `store clone`). High-level commands manage the full stack (FUSE+loop+ext4 on Linux, NFS+lwext4 on macOS).
- **`src/store.rs`** — Core `Store<S: S3Access>` struct. All block I/O, snapshot/clone logic, write-back cache coordination. The `S3Access` trait is generic for testing with `MockS3`.
- **`src/cache.rs`** — `DiskCache` singleton. LRU eviction of clean blocks, manages block files and `.dirty`/`.uploading`/`.pending` markers.
- **`src/fs.rs`** — Low-level FUSE implementation. Virtual filesystem with `/volume` (block device file), `/.loophole/` (control directory). Splits reads/writes across block boundaries.
- **`src/fs_ext4.rs`** — FUSE+lwext4 backend. Exposes ext4 filesystem directly via FUSE, using lwext4 for ext4 operations.
- **`src/nfs.rs`** — NFS backend. Implements `NFSFileSystem` trait backed by lwext4 `Ext4Fs`. Used on macOS.
- **`src/ctl.rs`** — Virtual `.loophole/` control directory shared by all filesystem backends. Creating files in `.loophole/snapshots/<name>` or `.loophole/clones/<name>` triggers snapshot/clone operations.
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

**Snapshot/Clone**: acquire write_lock(exclusive) → flush all dirty blocks → write new state.json(s) → set frozen=true. Triggered via `.loophole/snapshots/<name>` or `.loophole/clones/<name>`.

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
- **Python E2E tests** (`tests/`): pytest with `uv` for dependency management. Most tests use `hl_mount` (high-level format+mount) and are cross-platform. Linux-only tests (raw FUSE layout, fallocate) are skipped on macOS. Stress tests (`fsx`, `fio`) skip if tools are not installed.
- **E2E runner** (`src/bin/e2e.rs`): On Linux, runs tests in Docker with sharded parallelism. On macOS, runs natively with a local S3 mock (RustFS via docker compose).

### Defaults

- Block size: 4MB
- Volume size: 1TB (configurable via `parse_size()`: "1G", "4M", etc.)
- Upload slots: 20, download slots: 200
