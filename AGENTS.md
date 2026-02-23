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
make e2e                     # all e2e tests (Linux: Docker, macOS: native)
make e2e TEST=test_store     # filter by name

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

### Cargo Features

- `block-fuse` — Raw FUSE block device mode (`src/fs.rs`). Depends on `fuser`.
- `ext4-fuse` — FUSE+lwext4 mode (`src/fs_ext4.rs`). Depends on `fuser` + `lwext4`.
- `lwext4` — Shared lwext4 library dependency (`dep:ext4-lwext4`).
- `nfs` — NFS server mode (`src/nfs.rs`). Depends on `nfsserve` + `lwext4`.
- Default features: `block-fuse`, `ext4-fuse`.

### Module Layout

- **`src/main.rs`** — CLI (clap derive). Two command levels: high-level (`format`, `mount`, `snapshot`, `clone`) and low-level (`store format`, `store mount`, `store snapshot`, `store clone`). High-level commands manage the full stack (FUSE+loop+ext4 on Linux, NFS+lwext4 on macOS).
- **`src/store.rs`** — Core `Store<S: S3Access>` struct. All block I/O, snapshot/clone logic, write-back cache coordination. The `S3Access` trait is generic for testing with `MockS3`.
- **`src/cache.rs`** — `DiskCache` singleton. File-based block cache with LRU eviction of clean blocks. SQLite (`cache_repo.rs`) tracks metadata (populated, dirty, uploading state).
- **`src/cache_repo.rs`** — SQLite repository for cache metadata. Tables: `volumes`, `blocks` (populated/dirty/uploading state), `pending_zero_ops`.
- **`src/fs.rs`** — Low-level FUSE implementation (feature: `block-fuse`). Virtual filesystem with `/volume` (block device file), `/.loophole/` (control directory). Splits reads/writes across block boundaries.
- **`src/fs_ext4.rs`** — FUSE+lwext4 backend (feature: `ext4-fuse`). Exposes ext4 filesystem directly via FUSE, using lwext4 for ext4 operations.
- **`src/nfs.rs`** — NFS backend (feature: `nfs`). Implements `NFSFileSystem` trait backed by lwext4 `Ext4Fs`. Used on macOS.
- **`src/ctl.rs`** — Virtual `.loophole/` control directory shared by all filesystem backends. Creating files in `.loophole/snapshots/<name>` or `.loophole/clones/<name>` triggers snapshot/clone operations.
- **`src/s3.rs`** — `S3Access` trait + `Client` impl. State.json management, block index (list of S3 keys), put/get/delete operations.
- **`src/uploader.rs`** — Background uploader. Batch upload cycle: acquires all dirty/zero block locks, copies data to temp files (or notes zero ops), releases locks, then executes all S3 operations concurrently. No retries — failures propagate to flush() callers; dirty blocks stay dirty for the next cycle.
- **`src/assert.rs`** — Custom assertions controlled by env var.

### S3 Layout

```
s3://bucket/[prefix/]stores/<store-id>/state.json
s3://bucket/[prefix/]stores/<store-id>/<block-idx-hex>   (16-char zero-padded)
```

### Key Data Flow

**Read path** (`do_read_block`): zero_blocks → local cache → ancestor caches → S3 index (local then ancestors) → range GET + background full-block fetch → zeros if not found.

**Write path** (`do_write_block`): acquire write_lock(shared) → block lock → ensure cache file → pwrite → mark dirty in SQLite → release locks.

**Upload cycle** (`uploader.rs`): collect dirty blocks + zero ops → acquire all block locks (sorted) → for each: copy file to temp OR note zero op → release lock → execute all S3 uploads/tombstones/deletes concurrently (no locks held) → update SQLite state. Errors propagate to flush() callers; failed blocks remain dirty.

**Flush** (`store.rs`): bumps `requested_generation` (AtomicU64) + notifies upload loop → waits for `completed_generation` to catch up via watch channel. Multiple concurrent flush() calls coalesce.

**Snapshot/Clone**: acquire write_lock(exclusive) → flush all dirty blocks → write new state.json(s) → set frozen=true. Triggered via `.loophole/snapshots/<name>` or `.loophole/clones/<name>`.

### Concurrency Model

- `write_lock: RwLock` — shared for writes, exclusive for snapshot/clone (drains in-flight writes)
- `block_locks: BlockLockMap` — striped per-block locks (1024 mutexes), prevents concurrent writes/fetches to same block
- `Semaphore` — bounded concurrency for uploads (20) and downloads (200)
- `DashSet` — lock-free sets for inflight_downloads, zero_blocks

### Cache Layout

Block data is stored as one file per block on disk. SQLite tracks metadata only (populated, dirty, uploading state).

```
<cache-dir>/<store-id>/<block-idx-hex>     # block data (up to block_size)
<cache-dir>/cache.db                        # SQLite metadata
```

### Testing

- **Rust unit tests** (`src/store_tests.rs`): use `MockS3` (in-memory). ~57 tests covering read/write, block boundaries, ancestors, snapshots, clones, zero-blocks, tombstones, crash recovery, flush semantics.
- **Python E2E tests** (`tests/`): pytest with `uv` for dependency management. Most tests use `hl_mount` (high-level format+mount) and are cross-platform. Linux-only tests (raw FUSE layout, fallocate) are skipped on macOS. Stress tests (`fsx`, `fio`) skip if tools are not installed.
- **E2E runner**: `make e2e`. On Linux, runs tests in Docker with sharded parallelism. On macOS, runs natively with a local S3 mock (RustFS via docker compose).

### Defaults

- Block size: 4MB
- Volume size: 1TB (configurable via `parse_size()`: "1G", "4M", etc.)
- Upload slots: 20, download slots: 200
