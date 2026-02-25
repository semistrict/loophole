# TODO

## Bugs

- [x] **Double-close of immutable cache block files**: Tests that unmount+remount or snapshot/clone produce `WARN close failed: file already closed` for immutable cache files. Two layers sharing the same cached block file both try to close the same file handle. Seen on blocks 0x0, 0x4, 0x5, 0x6, 0x7 (ext4 superblock/metadata blocks).

- [ ] **`flush_bytes_total` metric never incremented**: `FlushBytes` counter is defined in `metrics/metrics.go` but never called in `flusher.go:flushOne` — always reads 0 regardless of actual uploads.

## Performance

- [ ] **E2E test suite slow (~5s per test) due to per-test S3 overhead**: Each test creates its own backend, S3 store, lease, volume, and formats ext4. ~10-12 S3 round trips of setup/teardown per test at ~200ms each. Consider sharing a backend across tests and/or `t.Parallel()`.

- [ ] **`layer.openBlocks` map grows unbounded**: Every touched 4MB block keeps an open FD for the layer lifetime. Not critical after `lazy_itable_init=1,nodiscard` mkfs fix (blocks: 25,600 → ~198), but latent issue for large volumes with heavy writes. Add an open FD limit with LRU eviction — close least-recently-used block file handles when approaching the limit.

- [ ] **Cache miss reads block entire 4MB block synchronously**: On a cache miss, the read path fetches the full 4MB block from S3 before returning any data. Instead, issue a range GET for just the requested byte range (fast path), return immediately, and kick off a background GET for the full block to populate the cache.

- [ ] **Block compression**: Compress blocks (e.g. zstd) before uploading to S3. Reduces storage costs, transfer time, and S3 PUT latency. Especially effective for ext4 metadata blocks which are highly compressible. Decompress on read. Store compression type in block key or metadata.

- [ ] **Block cache has no eviction or disk space limits**: The immutable block cache (`BlockCache`) grows without bound on disk. Need LRU eviction with a configurable max disk usage so the cache doesn't fill the disk on long-running instances.

## Missing Features (from design.md)

- [ ] **Crash recovery for interrupted snapshots**: design.md describes a two-phase snapshot with `snapshot_pending` field on the volume ref, and recovery logic on mount. Not yet implemented — a crash mid-snapshot could leave the volume in an inconsistent state.

- [ ] **Volume lease**: design.md describes a lease on the volume ref itself (separate from the layer lease) to protect snapshot/clone from races between processes. Currently only layer leases are implemented.

- [x] **Flush coalescing via generation counter** (correctness bug): If Flush A swaps the dirty set and starts uploading, a concurrent Flush B sees an empty dirty set and returns immediately — before Flush A's uploads complete. An fsync that triggered Flush B returns "success" while its data is still in-flight. Fix: use a generation counter so Flush B waits for the in-progress Flush A to complete.

- [ ] **Volume size as mount-time option (`--size`)**: design.md specifies a `--size` flag (default 1TB) for configuring volume size at mount time. Currently hardcoded (100GB for FUSE blockdev, 1GB for lwext4 tests).

- [ ] **Auto-start daemon from `loophole mount`**: design.md says `mount` should auto-start the daemon if no socket is found, and auto-stop when no volumes remain. Not yet implemented.

- [ ] **Mountpoint symlinks for socket discovery**: design.md describes `~/.loophole/mounts/<hash>.sock` symlinks so post-mount commands can find the daemon from a mountpoint path instead of requiring the S3 URL.

# FUSE Performance TODO

## fuseblockdev

- [ ] Set `NegativeTimeout: 1s` — cache ENOENT lookups for non-existent volume names
- [ ] Increase `AttrTimeout` / `EntryTimeout` to 5s — device file attributes are stable
- [ ] Set `MaxBackground: 128` — better concurrent I/O
- [ ] Set `DirectMount: true` — saves a fork

## Future (blocked on go-fuse / kernel support)

- [ ] FUSE over io_uring (Linux 6.14+) — ~25% read improvement, 50% CPU reduction. go-fuse doesn't support yet
- [ ] Multiple /dev/fuse FDs per mount (Linux 6.x) — better multi-core scaling. go-fuse issue #384
