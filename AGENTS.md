NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

## Third-party dependencies (`deps.sh`)

Third-party C/Go deps live in `third_party/` (gitignored). Patches live in `patches/`.

- `./deps.sh setup` — clone deps at pinned SHAs and apply patches (run this first)
- `./deps.sh genpatch <dep>` — regenerate a patch from your working tree changes (e.g. `./deps.sh genpatch lwext4`)
- `./deps.sh reset` — reset all patched deps to upstream state
- `./deps.sh repatch` — reset + re-apply patches

lwext4 uses `git diff`/`git apply` for patching (respects .gitignore). Other deps use `diff -ruN`/`patch -p2`.

To modify a third-party dep: edit files in `third_party/<dep>/`, then run `./deps.sh genpatch <dep>` to update the patch file.

## Building and testing

- **ALWAYS use `make` targets** — they build the lwext4 C static library and set CGO_LDFLAGS automatically. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make test-lwext4` — lwext4 unit tests (run locally on macOS, no Docker needed)
- `make test-lwext4 RUN=TestName` — run a specific lwext4 test
- `make clean-lwext4 && make test-lwext4` — when cgo C source changes, clean and rebuild to avoid stale builds
- E2E tests require Linux (FUSE). Run in Docker: `docker compose run --rm go bash -c 'make clean-lwext4 && make e2e-lwext4fuse'`

