NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

**NEVER use `npm` or `npx`. Always use `pnpm` and `pnpm exec` (or `pnpm dlx`) instead.**

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo. A `go.work` workspace file includes them as local modules — imports like `github.com/klauspost/compress` resolve to the local `third_party/compress` copy, not the upstream module.

## Building and testing

- **ALWAYS use `make` targets** — they build the lwext4 C static library and set CGO_LDFLAGS automatically. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make e2e-inprocess` — run e2e tests in-process (no FUSE/NBD/root required, works on macOS)
- `make e2e-inprocess RUN=TestName` — run a specific e2e test
- E2E tests in Docker: `docker compose run --rm go bash -c 'make e2e-fuse'`
- Set `LOG_LEVEL=debug` to enable slog debug output in e2e tests

## Utilities

- Use `util.SafeClose(c, msg)` from `internal/util` for `defer` closing `io.Closer` values instead of `defer func() { _ = c.Close() }()`

## Running locally

Config lives in `~/.loophole/config.toml`. Each `[profiles.<name>]` section defines an S3/R2 backend. Use `-p <profile>` to select one (e.g. `bin/loophole-darwin-arm64 -p r2 ...`).

- `make loophole` — build macOS binary to `bin/loophole-darwin-arm64`
- The daemon auto-starts when any command needs it. No need to run `start` explicitly.
- `bin/loophole-darwin-arm64 -p r2 status` — check daemon status
- `bin/loophole-darwin-arm64 -p r2 stop` — stop daemon

### Daemon log file

Logs go to `~/.loophole/<profile>.log`. The auto-start daemon (via go-daemon) redirects stdout and stderr to this file, so C library output, Go panics, and `net/http` panic recovery messages all appear there. Go's `log` package is also redirected to this file.

Set `log_level = "debug"` in the profile config for debug output.

### Debugging with goroutine dumps

The daemon exposes pprof endpoints on its Unix socket:
```
curl --unix-socket ~/.loophole/<profile>.sock 'http://localhost/debug/pprof/goroutine?debug=2'
```
Use this to diagnose stuck operations (e.g. freeze blocked on S3 upload).

## Creating a zygote volume

A zygote is a frozen volume with a rootfs that other volumes clone from.

1. Export an Ubuntu rootfs tarball:
   ```
   CID=$(docker create --platform linux/amd64 ubuntu:24.04 /bin/true)
   docker export "$CID" > /tmp/ubuntu-2404.tar
   docker rm "$CID"
   ```

2. Create, populate, and freeze:
   ```
   bin/loophole-darwin-arm64 -p r2 create --size 16GB zygote-ubuntu-2404
   bin/loophole-darwin-arm64 -p r2 file tar -xv -C zygote-ubuntu-2404:/ -f /tmp/ubuntu-2404.tar
   bin/loophole-darwin-arm64 -p r2 freeze zygote-ubuntu-2404
   ```

## Deploying to Cloudflare

- `make cf-demo-bin` — cross-compile linux/amd64 binary (nosqlite nolwext4) to `cf-demo/bin/loophole`
- `cd cf-demo && pnpm exec wrangler deploy` — deploy the worker + container
- Deployed URL: `https://cf-demo.ramon3525.workers.dev`

### CF debug endpoints

The `/c/<container-id>/debug/` routes proxy to the container's loophole daemon. The default container ID is `cf-singleton-container`.

**TanStack route handlers** (in `cf-demo/src/routes/c/$id/debug/`):
- `GET /debug/state` — container state (`{"status":"healthy"}`)
- `GET /debug/env` — R2 credentials summary (redacted)
- `GET /debug/test` — container state + daemon status fetch
- `POST /debug/start` — start the container
- `POST /debug/boot` — boot sequence (browser-only, returns HTML)
- `POST /debug/destroy` — destroy the container

**Passthrough proxy** (`GET|POST /debug/$splat` → daemon HTTP API):
- `POST /debug/mount` — mount a volume: `{"volume":"...","mountpoint":"..."}`
- `POST /debug/clone` — clone a volume: `{"mountpoint":"<src>","clone":"<name>","clone_mountpoint":"<mp>"}`
- `POST /debug/sandbox/exec?volume=<vol>&cmd=<cmd>` — run a command in the sandbox
- `GET /debug/status` — daemon status JSON
- `GET /debug/volumes` — list loaded volumes

**Example: clone zygote and run a command:**
```
BASE=https://cf-demo.ramon3525.workers.dev/c/cf-singleton-container/debug
curl -X POST "$BASE/start"
curl -X POST "$BASE/mount" -H 'Content-Type: application/json' -d '{"volume":"zygote-ubuntu-2404","mountpoint":"zygote-ubuntu-2404"}'
curl -X POST "$BASE/clone" -H 'Content-Type: application/json' -d '{"mountpoint":"zygote-ubuntu-2404","clone":"sandbox-1","clone_mountpoint":"sandbox-1"}'
curl -X POST "$BASE/sandbox/exec?volume=sandbox-1&cmd=cat+/etc/os-release"
```
