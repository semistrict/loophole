# Plan: Integrate lwext4 as in-process ext4 filesystem

## Context

Currently loophole uses a 4-layer stack: `User → ext4 (kernel) → loop device → FUSE (block device file) → Store → S3`. This requires Linux-only shell commands (`losetup`, `mkfs.ext4`, `mount`) and cannot work on macOS.

By integrating lwext4 via the `ext4-lwext4` crate, we get: `User → FUSE (files/dirs) → lwext4 (in-process) → Store → S3`. This eliminates all shell commands from the data path and should be supported alongside the existing kernel ext4 mode for performance comparison.

We also want a **no-fuse library mode**: `User code → lwext4 API (in-process) → Store → S3`, so projects can embed loophole without mounting FUSE.

## Ground rule

New code goes in new files. Confirm with user before making major modifications to existing files (fs.rs, store.rs, main.rs, etc.). Minor additions like `mod` declarations are fine.

## Step 0: Bump Rust toolchain (already done)

- Dockerfile: `rust:1.93`, local: `rustc 1.93.1`
- `ext4-lwext4` dep in `Cargo.toml`

## Step 0.5: Add Cargo feature flags

```toml
[features]
default = ["kernel", "lwext4"]     # keeps existing CLI behavior
kernel = ["fuse"]                   # existing losetup+mount mode
lwext4 = ["dep:ext4-lwext4"]        # in-process ext4 core
fuse = ["dep:fuser"]                # FUSE frontend (used by kernel and lwext4-fuse)
no-fuse = []                        # library mode marker (no FUSE frontend)

[dependencies]
fuser = { version = "0.17.0", optional = true }
ext4-lwext4 = { version = "0.1.1", optional = true }
```

Build combinations:
- `cargo build` → default CLI modes (`kernel` + `lwext4`, with `fuse` transitively via `kernel`)
- `cargo build --no-default-features --features kernel` → kernel only (current behavior)
- `cargo build --no-default-features --features "lwext4,fuse"` → lwext4 with FUSE mount frontend
- `cargo build --no-default-features --features "lwext4,no-fuse"` → lwext4 library mode, no FUSE

### Static linking (lwext4-only mode)

When building with `lwext4,no-fuse`, there is no libfuse C dependency. That enables static musl builds for library consumers or a separate no-fuse binary target:

```dockerfile
# Static build for lwext4-only mode
FROM rust:1.93 AS builder
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl \
    --no-default-features --features "lwext4,no-fuse"

FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/loophole /loophole
ENTRYPOINT ["/loophole"]
```

Note: lwext4 is a C library compiled via cc crate, so musl cross-compilation needs `musl-tools` installed. Any build that enables `fuse` still depends on platform FUSE support.

Guard code with `#[cfg(feature = "...")]`:
- `src/fs.rs` and its `mod fs` → `#[cfg(all(feature = "kernel", feature = "fuse"))]`
- `src/fs_ext4.rs` and `mod fs_ext4` → `#[cfg(all(feature = "lwext4", feature = "fuse"))]`
- `src/blockdev_adapter.rs` → `#[cfg(feature = "lwext4")]`
- `src/lwext4_api.rs` and `mod lwext4_api` → `#[cfg(feature = "lwext4")]`
- Runtime mode selection in main.rs checks which features are compiled in

## Step 1: Create `src/blockdev_adapter.rs` — Bridge Store's BlockStorage to lwext4's BlockDevice

The core adapter. ext4-lwext4's `BlockDevice` trait is synchronous and uses ext4-sized blocks (4096 bytes). Our `Store` uses async `BlockStorage` with 4MB blocks.

```rust
// Implements ext4_lwext4::BlockDevice for our Store
pub struct StoreBlockDevice {
    store: Arc<Store<S>>,       // the underlying store
    rt: tokio::runtime::Handle, // for block_on
    store_block_size: u64,      // e.g. 4MB
    ext4_block_size: u32,       // e.g. 4096
    ext4_block_count: u64,      // volume_size / ext4_block_size
}
```

Key translation in `read_blocks(block_id, buf)`:
- `store_block_idx = (block_id * ext4_block_size) / store_block_size`
- `offset_within = (block_id * ext4_block_size) % store_block_size`
- Call `rt.block_on(store.read_block(store_block_idx, offset_within, buf.len()))`
- Handle reads/writes that span **any number** of store blocks (loop, not special-case two)

Same logic for `write_blocks`. `flush()` calls `rt.block_on(store.flush())`.

## Step 1.5: Create `src/lwext4_api.rs` — no-fuse library API

Expose a library surface that wraps `Ext4Fs + StoreBlockDevice + Store` without FUSE:
- `Lwext4Volume::open(...)` / `Lwext4Volume::format(...)`
- file/dir operations via underlying lwext4 handles
- `sync_all()` performs `fs.sync()` **then** `store.flush()`
- `snapshot()` / `clone_store()` perform `fs.sync()` + `store.flush()` before metadata transition

This is the primary path for `--features "lwext4,no-fuse"`.

## Step 2: Create `src/fs_ext4.rs` — New FUSE impl using lwext4 (separate from existing fs.rs)

Keep `src/fs.rs` (existing block-device FUSE) completely untouched. Create `src/fs_ext4.rs` as a second, independent FUSE filesystem implementation. At runtime, `main.rs` picks which one to mount based on `LOOPHOLE_MODE`.

This file contains:
- `Ext4Fuse` struct implementing `fuser::Filesystem`
- Wraps `ext4_lwext4::Ext4Fs` with a `Mutex` (lwext4 is not thread-safe, FUSE is multi-threaded)
- Maintains path/inode mappings that support hardlinks and rename updates (not single `ino -> one path`)
- Virtual `.loophole/rpc` overlay using synthetic inodes (`u64::MAX`, `u64::MAX - 1`)
- RPC handling reused from `src/rpc.rs`

### FUSE op to lwext4 mapping

| FUSE op | lwext4 call |
|---------|-------------|
| lookup | `fs.open_dir(parent_path)` then find entry by name, update path/inode maps |
| getattr | `fs.metadata(path)` |
| readdir | `fs.open_dir(path)` then iterate `DirEntry` |
| read | `fs.open(path, READ)` then `file.seek()` + `file.read()` |
| write | `fs.open(path, WRITE)` then `file.seek()` + `file.write()` |
| create | `fs.open(path, CREATE | WRITE)` |
| mkdir | `fs.mkdir(path, mode)` |
| unlink | `fs.remove(path)` |
| rmdir | `fs.rmdir(path)` |
| rename | `fs.rename(from, to)` + update path/inode maps for subtree |
| setattr | `fs.set_permissions()` / `fs.set_owner()` / truncate |
| statfs | `fs.stat()` |
| fsync | `fs.sync()` then `store.flush()` |
| symlink/readlink/link | `fs.symlink()` / `fs.readlink()` / `fs.link()` |
| access | basic mode checks + `EACCES`/`EPERM` mapping |
| xattr | return `ENODATA`/`EOPNOTSUPP` consistently |

## Step 3: Modify `src/main.rs` — Runtime mode switch via env var

Mode selection via `LOOPHOLE_MODE` env var, constrained by compiled features:
- `LOOPHOLE_MODE=lwext4` (or `lwext4-fuse`) → lwext4 FUSE path (requires `lwext4` + `fuse`)
- `LOOPHOLE_MODE=kernel` → losetup+mount path (requires `kernel` + `fuse`)
- If only one FUSE mode is compiled, use it and warn if env var requests unavailable mode
- `no-fuse` builds expose library API and do not include FUSE mount commands
- Default on macOS (when both FUSE modes compiled): `lwext4`
- Default on Linux (when both FUSE modes compiled): `kernel`

## Step 4: Format/mount/snapshot/clone flows in main.rs (lwext4 mode paths)

### Format flow (lwext4 mode)
1. Create/load Store (existing code)
2. Create `StoreBlockDevice` wrapping the Store
3. Call `ext4_lwext4::mkfs(device, &MkfsOptions::default())` (explicit 4096-byte ext4 block size)
4. Flush store

### Mount flow (lwext4 mode)
1. Load Store (existing code)
2. Create `StoreBlockDevice`
3. Mount with `Ext4Fs::mount(device, false)`
4. Create `Ext4Fuse` struct with Ext4Fs + path/inode maps + RPC handling
5. Start `fuser::mount2()` with `Ext4Fuse`

### Snapshot/Clone flow (lwext4 FUSE mode)
1. `syncfs_mount(mountpoint)` to drain kernel buffered writes into FUSE
2. Call RPC directly on that mountpoint (`<mountpoint>/.loophole/rpc`) — no loop-device lookup
3. In RPC handler for snapshot/clone: lock lwext4 FS, run `fs.sync()`, then run `store.flush()`, then perform store snapshot/clone

## Files to modify/create

| File | Action |
|------|--------|
| `Cargo.toml` | **MODIFY** — Add `[features]`, make `fuser` and `ext4-lwext4` optional |
| `src/lib.rs` | **CREATE** — Public library entry points for no-fuse mode |
| `src/blockdev_adapter.rs` | **CREATE** — StoreBlockDevice impl |
| `src/lwext4_api.rs` | **CREATE** — Direct lwext4 API wrapper (no FUSE) |
| `src/fs_ext4.rs` | **CREATE** — New FUSE impl (lwext4-based), separate from fs.rs |
| `src/fs.rs` | **UNCHANGED** — Existing block-device FUSE impl stays as-is |
| `src/main.rs` | **MODIFY** — Add cfg-gated mods, read `LOOPHOLE_MODE`, branch format/mount/snapshot/clone flows |

## Existing code to reuse

- `src/store.rs`: `Store`, `BlockStorage` trait, `load()`, `format()`, `flush()` — unchanged
- `src/s3.rs`: `S3Access` trait, all helpers — unchanged
- `src/cache.rs`: LRU cache — unchanged
- `src/rpc.rs`: RPC types and dispatch — reuse in new fs_ext4.rs
- `src/lru.rs`: LRU impl — unchanged

## Implementation order

1. `src/blockdev_adapter.rs` — can be tested independently
2. `src/lwext4_api.rs` + `src/lib.rs` — no-fuse library API
3. `src/fs_ext4.rs` — new FUSE impl with lwext4 + Mutex + path/inode maps + RPC overlay
4. `src/main.rs` — add cfg-gated mods, runtime mode selection, lwext4 snapshot/clone path
5. Test end-to-end: format + mount + read/write files

## Verification

1. `cargo build` — compiles on macOS
2. `cargo test` — existing store tests still pass (they don't touch fs.rs)
3. Manual test on macOS:
   - Start local S3 (minio)
   - `LOOPHOLE_MODE=lwext4 loophole --bucket test --endpoint-url http://127.0.0.1:9000 format --store s1 --block-size 4M --volume-size 100M`
   - `LOOPHOLE_MODE=lwext4 loophole --bucket test --endpoint-url http://127.0.0.1:9000 mount --store s1 --cache-dir /tmp/loophole-cache /tmp/mnt`
   - `echo hello > /tmp/mnt/test.txt && cat /tmp/mnt/test.txt`
   - `ls -la /tmp/mnt/`
   - `LOOPHOLE_MODE=lwext4 loophole --bucket test --endpoint-url http://127.0.0.1:9000 snapshot /tmp/mnt --new-store s2`
4. Docker test on Linux:
   - Same as above with `LOOPHOLE_MODE=lwext4`
   - Also test without env var (defaults to kernel mode, existing behavior)
   - Performance comparison between modes
5. no-fuse library build checks:
   - `cargo build --no-default-features --features "lwext4,no-fuse"`
   - `cargo test --no-default-features --features "lwext4,no-fuse"`
