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

### Debugging on remote Linux (Fly)

When debugging the daemon on a remote Linux machine (e.g. Fly), these techniques work well:

**Grep structured logs from the log file.** The daemon writes JSON-structured logs to `~/.loophole/<profile>.log`. Use `grep "snapshotter:"` or similar to filter for specific subsystems. This is much more reliable than trying to read interleaved console output.

**Use `fly ssh console -C` for clean output.** When the daemon runs in the foreground with debug logging, its output floods the terminal. Use a separate SSH connection to run commands with clean stdout:
```
fly ssh console -a loophole-test -C 'ctr run --snapshotter loophole --rm docker.io/library/alpine:latest test /bin/echo hello'
```

**Goroutine dumps for stuck operations.** When something hangs, grab a goroutine dump via pprof to see exactly where it's blocked:
```
fly ssh console -a loophole-test -C 'curl --unix-socket /root/.loophole/tigris.sock "http://localhost/debug/pprof/goroutine?debug=2"'
```
Grep for your subsystem (e.g. `WaitClosed`, `snapshotter`, `grpc`) to find the stuck goroutine. This is how the FUSE Lookup ref leak was diagnosed — the dump showed `WaitClosed` blocked in `sync.Cond.Wait` because `OnForget` (kernel inode eviction) was indefinitely delayed.

**Watch goroutine count in heartbeat.** The daemon logs `goroutines=N` every 5 seconds. A steadily climbing count indicates a goroutine leak. A stable but unexpectedly high count may indicate a stuck operation.

**Note:** Ubuntu 24.04+ has `fusermount3` not `fusermount`. Stale FUSE mounts from a crashed daemon need `umount -l <mountpoint>` followed by `rm -rf <mountpoint>` before restarting.

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
- **NEVER run `pnpm exec wrangler deploy` directly** — it skips the build and deploys stale artifacts. Always use `cd cf-demo && pnpm run deploy` which runs the full build first.
- Deployed URL: `https://cf-demo.ramon3525.workers.dev`
- **Container rollout is not instant.** After `wrangler deploy`, CF containers use a rolling deploy strategy. A `destroy()` + `start()` cycle does NOT guarantee the new image — the rollout may still be in progress. Give it a minute or two after deploy before destroying/restarting. Verify the new binary is running by checking `md5sum /usr/local/bin/loophole` via the host exec endpoint (no volume param): `POST /debug/sandbox/exec?cmd=md5sum+/usr/local/bin/loophole`

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

### CF container logs (Axiom)

The CF container daemon sends structured logs to Axiom. Dataset: `deepagent`. Use the `axiom` CLI to query.

**Common fields:** `_time`, `level` (INFO/WARN/ERROR), `msg`, `volume`, `mountpoint`, `clone`, `zygote`, `err`, `error`, `method`, `path`, `status`, `dur` (nanoseconds).

**Examples:**
```bash
# Recent logs (last 30 min), sorted chronologically
axiom query "['deepagent'] | where _time > now(-30m) | sort by _time asc | project _time, level, msg, volume, mountpoint, err, error, clone" -f json

# Errors and warnings only
axiom query "['deepagent'] | where _time > now(-1h) | where level == 'ERROR' or level == 'WARN' | sort by _time asc | project _time, level, msg, err, error" -f json

# HTTP requests with latency
axiom query "['deepagent'] | where _time > now(-30m) | where msg == 'http' | sort by _time asc | project _time, method, path, status, dur" -f json

# Filter by volume
axiom query "['deepagent'] | where _time > now(-1h) | where volume == 'sandbox-1' | sort by _time asc" -f json
```

**Notes:**
- Time filters use `now(-30m)`, `now(-1h)`, etc. Do NOT use string timestamps (causes type error).
- Use `-f json` for machine-readable output.
- The `dur` field is in nanoseconds (e.g. 1924547003 = ~1.9s).

**Example: clone zygote and run a command:**
```
BASE=https://cf-demo.ramon3525.workers.dev/c/cf-singleton-container/debug
curl -X POST "$BASE/start"
curl -X POST "$BASE/mount" -H 'Content-Type: application/json' -d '{"volume":"zygote-ubuntu-2404","mountpoint":"zygote-ubuntu-2404"}'
curl -X POST "$BASE/clone" -H 'Content-Type: application/json' -d '{"mountpoint":"zygote-ubuntu-2404","clone":"sandbox-1","clone_mountpoint":"sandbox-1"}'
curl -X POST "$BASE/sandbox/exec?volume=sandbox-1&cmd=cat+/etc/os-release"
```
