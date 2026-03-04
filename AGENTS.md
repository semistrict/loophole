NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo.

## Building and testing

- **ALWAYS use `make` targets** — they build the lwext4 C static library and set CGO_LDFLAGS automatically. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make test-lwext4` — lwext4 unit tests (run locally on macOS, no Docker needed)
- `make test-lwext4 RUN=TestName` — run a specific lwext4 test
- `make clean-lwext4 && make test-lwext4` — when cgo C source changes, clean and rebuild to avoid stale builds
- E2E tests require Linux (FUSE). Run in Docker: `docker compose run --rm go bash -c 'make clean-lwext4 && make e2e-lwext4fuse'`

