# FUSE Performance TODO

## fuseblockdev

- [ ] Set `NegativeTimeout: 1s` — cache ENOENT lookups for non-existent volume names
- [ ] Increase `AttrTimeout` / `EntryTimeout` to 5s — device file attributes are stable
- [ ] Set `MaxBackground: 128` — better concurrent I/O
- [ ] Set `DirectMount: true` — saves a fork

## Future (blocked on go-fuse / kernel support)

- [ ] FUSE over io_uring (Linux 6.14+) — ~25% read improvement, 50% CPU reduction. go-fuse doesn't support yet
- [ ] Multiple /dev/fuse FDs per mount (Linux 6.x) — better multi-core scaling. go-fuse issue #384
