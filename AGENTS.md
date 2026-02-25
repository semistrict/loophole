NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

## Third-party dependencies (`deps.sh`)

Third-party C/Go deps live in `third_party/` (gitignored). Patches live in `patches/`.

- `./deps.sh setup` — clone deps at pinned SHAs and apply patches (run this first)
- `./deps.sh genpatch <dep>` — regenerate a patch from your working tree changes (e.g. `./deps.sh genpatch lwext4`)
- `./deps.sh reset` — reset all patched deps to upstream state
- `./deps.sh repatch` — reset + re-apply patches

lwext4 uses `git diff`/`git apply` for patching (respects .gitignore). Other deps use `diff -ruN`/`patch -p2`.

To modify a third-party dep: edit files in `third_party/<dep>/`, then run `./deps.sh genpatch <dep>` to update the patch file.

## Running tests

- `go test ./lwext4/` — lwext4 unit tests (run locally on macOS, no Docker needed)
- E2E tests require Linux (FUSE). Run in Docker: `docker compose run --rm go bash -c 'go clean -cache && make e2e-lwext4fuse'`
- When cgo C source changes, run `go clean -cache` before testing (both locally and in Docker) to avoid stale builds

