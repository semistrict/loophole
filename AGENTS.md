NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

When choosing between a quick patch and the intended architecture, always do the right architectural fix. Do not preserve or reintroduce obsolete behavior just to get tests passing faster.

**NEVER use `npm` or `npx`. Always use `pnpm` and `pnpm exec` (or `pnpm dlx`) instead.**

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo. A `go.work` workspace file includes them as local modules — imports like `github.com/klauspost/compress` resolve to the local `third_party/compress` copy, not the upstream module.

## Building and testing

- **ALWAYS use `make` targets** — they capture the supported build and test entrypoints for this repo. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- `make e2e` — run the Linux kernel ext4-over-FUSE e2e tests
- `make e2e RUN=TestName` — run a specific e2e test
- E2E tests in Docker: `docker compose run --rm go bash -c 'make e2e'`
- Set `LOG_LEVEL=debug` to enable slog debug output in e2e tests

## Utilities

- **Always use `util.SafeClose(c, msg)`** from `internal/util` when discarding a `Close()` error — both in `defer` and in error-cleanup paths. Never write bare `c.Close()` or `_ = c.Close()`. Example: `defer util.SafeClose(f, "close file")` or in an error path: `util.SafeClose(db, "close db on init failure")`

## Running locally

Config lives in `~/.loophole/config.toml`. Each `[profiles.<name>]` section defines an S3/R2 backend. Use `-p <profile>` to select one (e.g. `bin/loophole-darwin-arm64 -p r2 ...`).

- `make loophole` — build macOS binary to `bin/loophole-darwin-arm64`
- `bin/loophole-darwin-arm64 -p r2 create myvol` — create, mount, and keep the owner process in the foreground
- `bin/loophole-darwin-arm64 -p r2 status myvol` — check the owner process for a mounted/attached volume
- `bin/loophole-darwin-arm64 -p r2 shutdown myvol` — gracefully stop the owner process

### Daemon log file

Logs go to `~/.loophole/<profile>.log` for the foreground owner process. C library output, Go panics, and `net/http` panic recovery messages all appear there. Go's `log` package is also redirected to this file.

Set `log_level = "debug"` in the profile config for debug output.

### Debugging with goroutine dumps

The daemon exposes pprof endpoints on its Unix socket:
```
curl --unix-socket ~/.loophole/<profile>.sock 'http://localhost/debug/pprof/goroutine?debug=2'
```
Use this to diagnose stuck operations (e.g. freeze blocked on S3 upload).

### Debugging on remote Linux (Fly)

When debugging the daemon on a remote Linux machine (e.g. Fly), these techniques work well:

**Grep structured logs from the log file.** The daemon writes JSON-structured logs to `~/.loophole/<profile>.log`. Use `grep "checkpoint"` or other subsystem strings to filter for specific operations. This is much more reliable than trying to read interleaved console output.

**Use `fly ssh console -C` for clean output.** When the daemon runs in the foreground with debug logging, its output floods the terminal. Use a separate SSH connection to run commands with clean stdout:
```
fly ssh console -a loophole-test -C 'bin/loophole-linux-amd64 -p tigris status'
```

**Goroutine dumps for stuck operations.** When something hangs, grab a goroutine dump via pprof to see exactly where it's blocked:
```
fly ssh console -a loophole-test -C 'curl --unix-socket /root/.loophole/tigris.sock "http://localhost/debug/pprof/goroutine?debug=2"'
```
Grep for your subsystem (e.g. `WaitClosed`, `checkpoint`, `grpc`) to find the stuck goroutine. This is how the FUSE Lookup ref leak was diagnosed — the dump showed `WaitClosed` blocked in `sync.Cond.Wait` because `OnForget` (kernel inode eviction) was indefinitely delayed.

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
- `make cf-demo-dev-lima` — run `cf-demo` locally inside the `fc` Lima VM with `wrangler dev`
- `make cf-demo-smoke-local` — hit the local Worker at `CF_DEMO_BASE_URL` and verify sandbox create/files/exec/session end to end
- `make cf-demo-smoke-lima` — hit the local `/debug` endpoints in Lima and verify Firecracker `exec` with `echo hello` and `/etc/os-release`
- **NEVER run `pnpm exec wrangler deploy` directly** — it skips the build and deploys stale artifacts. Always use `cd cf-demo && pnpm run deploy` which runs the full build first.
- Deployed URL: `https://cf-demo.ramon3525.workers.dev`
- **Container rollout is not instant.** After `wrangler deploy`, CF containers use a rolling deploy strategy. Give it a minute or two after deploy. Verify the new binary by SSH-ing in and running `md5sum /usr/bin/loophole`.

### Local CF workflow in Lima

Use this loop instead of redeploying to Cloudflare while iterating on the container/daemon path:

1. Start the local Worker + container runtime:
   ```
   make cf-demo-dev-lima
   ```
2. In another shell, run the smoke test:
   ```
   make cf-demo-smoke-lima
   ```

Overrides:
- `LIMA_VM=<name>` — use a different Lima VM than `fc`
- `CF_DEMO_BASE_URL=http://127.0.0.1:7935` — point smoke tests at a different local port
- `CF_DEMO_SMOKE_VOLUME=<volume>` — use a different sandbox volume

### CF architecture

The app uses two Durable Objects:
- **Scheduler** (`cf-demo/src/scheduler.ts`) — singleton that keeps the sandbox catalog and routes `/api/*`, `/toolbox/*`, and `/debug/*` to the chosen container.
- **SandboxContainer** (`cf-demo/src/container.ts`) — wraps the CF Container runtime. Starts/stops containers and notifies Scheduler on stop.

API requests go: Worker → Scheduler DO → Container (`container-control` → `loophole`/`sandboxd`).

### CF debug endpoints

The Scheduler forwards `/debug/*` requests to the container daemon (stripping the `/debug/` prefix):
- `POST /debug/stop-all` — stop all containers (handled by Scheduler, use after deploy to force new image)
- `POST /debug/mount` — mount a volume: `{"volume":"...","mountpoint":"..."}`
- `POST /debug/clone` — clone a volume: `{"mountpoint":"<src>","clone":"<name>","clone_mountpoint":"<mp>"}`
- `POST /debug/checkpoint` — create a checkpoint for a mounted volume: `{"mountpoint":"..."}`
- `POST /debug/sandbox/exec?volume=<vol>&cmd=<cmd>` — run a command in the sandbox
- `GET /debug/status` — daemon status JSON
- `GET /debug/volumes` — list loaded volumes

### SSH into CF container

```
bin/loophole-darwin-arm64 ssh --url https://cf-demo.ramon3525.workers.dev --volume <volume-name>
```

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

**Example: clone zygote and run a command via debug endpoints:**
```
BASE=https://cf-demo.ramon3525.workers.dev/debug
curl -X POST "$BASE/mount" -H 'Content-Type: application/json' -d '{"volume":"zygote-ubuntu-2404","mountpoint":"zygote-ubuntu-2404"}'
curl -X POST "$BASE/clone" -H 'Content-Type: application/json' -d '{"mountpoint":"zygote-ubuntu-2404","clone":"sandbox-1","clone_mountpoint":"sandbox-1"}'
curl -X POST "$BASE/sandbox/exec?volume=sandbox-1&cmd=cat+/etc/os-release"
```
