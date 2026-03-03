# LSM TODO

## Critical bugs

- ~~**`loadLayerMap` fallback missing image layers**~~ — done. Fallback now lists both `deltas/` and `images/`, parsing image keys with `parseImageKey`.
- ~~**`createChild` doesn't use CAS**~~ — done. Uses `ModifyJSON` (CAS with retry) to update parent `meta.json`.
- ~~**Race on `tl.memLayer` pointer**~~ — done. Capture `ml := tl.memLayer` under `mu.RLock()`, use `ml.size.Load()` after unlock.
- ~~**`freezeMemLayer` panics on disk full**~~ — done. Returns error instead of panicking. New memLayer is created before freezing the old one so failure is safe.
- ~~**`Manager.Close` dead code**~~ — done. Removed dead `firstErr` pattern, directly return `pageCache.Close()` error.
- ~~**`loadLayerMap` swallows corrupt JSON**~~ — done. `ErrNotFound` sentinel distinguishes 404 from parse errors. Only falls back on not-found; corrupt JSON is a hard error.

## Correctness gaps

- ~~**`MaxFrozenLayers` unused**~~ — done. `Write` checks frozen layer count before each page put; if at capacity, flushes first to provide backpressure.
- ~~**No concurrent compaction guard**~~ — done. `compactMu` serializes in-process. Cross-process guarded by `LeaseToken` in `TimelineMeta` (acquired on open, released on close).
- ~~**`readPage` seq comparison naming**~~ — done. Renamed `maxSeq` to `beforeSeq` in `readPage`, `readFromDeltaLayer`, `DebugPage`, and `findPage` to clarify the exclusive `<` semantics.

## Performance

- ~~**mmap page cache**~~ — done. File-backed mmap with random eviction in `pagecache.go`.
- ~~**Pool zstd encoder/decoder**~~ — done. `sync.Pool` of `zstd.Decoder` in `lsm.go`; `findPage` uses `getZstdDecoder`/`putZstdDecoder`.
- ~~**Singleflight for layer downloads**~~ — done. `singleflight.Group` on Timeline deduplicates concurrent `getDeltaLayer`/`getImageLayer` calls.
- ~~**Singleflight for OpenVolume**~~ — done. `singleflight.Group` on Manager deduplicates concurrent `OpenVolume` calls.
- ~~**Shared zero page**~~ — done. Package-level `var zeroPage [PageSize]byte`; tombstones and never-written pages return `zeroPage[:]`.
- ~~**Context cancellation in tight loops**~~ — done. `Read`, `Write`, `PunchHole`, `Compact` check `ctx.Err()` each iteration.
- ~~**Unbounded in-memory layer caches**~~ — done. `MaxLayerCacheEntries` config (default 64); random eviction when exceeded.
- ~~**Full layer download vs range reads**~~ — done. `ObjectStore.GetRange` added for bounded range reads. `parseDeltaIndex`/`parseImageIndex` download only header+index (2 range reads). `findPage` fetches individual compressed pages via `GetRange` on demand. Disk cache still uses full blobs when available. Benchmark: **1,062,502 → 26,782 s3-rx-bytes/op** (~40x reduction) for reading 4 pages from a 1024-page layer.

## Missing features (per design doc)

- ~~**Leases**~~ — done. `Manager` accepts `*LeaseManager`; on `openVolume`, acquires lease in `meta.json` via CAS; on close/destroy, releases it. Background renewal handled by `LeaseManager`.
- **GC** — `DeleteVolume` has `// TODO: background GC`. Deleted volumes leave timeline data permanently on S3. Need timeline-level GC: delete orphaned timelines, clean up branch points from deleted children.
- ~~**On-disk page cache**~~ — done. `pages.cache` mmap'd file, deleted+recreated on startup.
