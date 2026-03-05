NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo.

## Building and testing

- **ALWAYS use `make` targets** — they build the lwext4 C static library and set CGO_LDFLAGS automatically. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make clean-lwext4` — when cgo C source changes, clean to avoid stale builds
- E2E tests require Linux (FUSE). Run in Docker: `docker compose run --rm go bash -c 'make clean-lwext4 && make e2e-lwext4fuse'`
- `make e2e-juicefs [RUN=TestName]` — JuiceFS in-process e2e tests (macOS/Linux, no FUSE)
- `make e2e-juicefsfuse [RUN=TestName]` — JuiceFS FUSE e2e tests (Linux only)
- Set `LOG_LEVEL=debug` to enable slog debug output in e2e tests

## Utilities

- Use `util.SafeClose(c, msg)` from `internal/util` for `defer` closing `io.Closer` values instead of `defer func() { _ = c.Close() }()`

