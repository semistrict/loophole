NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo. A `go.work` workspace file includes them as local modules — imports like `github.com/klauspost/compress` resolve to the local `third_party/compress` copy, not the upstream module.

## Building and testing

- **ALWAYS use `make` targets** — they build the lwext4 C static library and set CGO_LDFLAGS automatically. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make e2e-inprocess` — run e2e tests in-process (no FUSE/NBD/root required, works on macOS)
- `make e2e-inprocess RUN=TestName` — run a specific e2e test
- E2E tests in Docker: `docker compose run --rm go bash -c 'make e2e-lwext4fuse'`
- Set `LOG_LEVEL=debug` to enable slog debug output in e2e tests

## Utilities

- Use `util.SafeClose(c, msg)` from `internal/util` for `defer` closing `io.Closer` values instead of `defer func() { _ = c.Close() }()`
