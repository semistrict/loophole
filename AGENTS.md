NB: this is a completely new system with no users. Don't worry about backwards compatibility ever.

When choosing between a quick patch and the intended architecture, always do the right architectural fix. Do not preserve or reintroduce obsolete behavior just to get tests passing faster.

## Communication

- Answer behavior-first in plain language by default. Start with the shortest direct explanation of what happens.
- Do not lead with implementation details, control-flow narration, or internal terminology unless the user asks for internals.
- Do not include file paths or line numbers in normal explanations. Only include code references when the user explicitly asks for them.

## Package manager -- CRITICAL

**NEVER use `npm`, `npx`, or `node_modules/.bin/*` directly. This project uses pnpm exclusively.**

| Instead of | Use |
|---|---|
| `npm install` | `pnpm install` |
| `npm run <script>` | `pnpm run <script>` |
| `npx <pkg>` | `pnpm dlx <pkg>` |
| `npx <local-bin>` | `pnpm exec <local-bin>` |

## Logging

**Always use `log/slog`** for logging. Never use `fmt.Fprintf(os.Stderr, ...)`, `log.Printf`, or `fmt.Println` for diagnostic output. Use structured fields: `slog.Info("msg", "key", value)`.

## Third-party dependencies

Third-party C/Go deps live in `third_party/` and are committed directly to the repo. A `go.work` workspace file includes them as local modules — imports like `github.com/klauspost/compress` resolve to the local `third_party/compress` copy, not the upstream module.

## Building and testing

- **ALWAYS use `make` targets** — they capture the supported build and test entrypoints for this repo. Never run `go build` or `go test` directly.
- `make build` — build all packages
- `make test` — run all unit tests
- `make test RUN=TestName` — run a specific test across all packages
- **E2E tests MUST run in Docker** (they need Linux root + FUSE): `docker compose run --rm go bash -c 'make e2e'`
- Run a specific e2e test: `docker compose run --rm go bash -c 'make e2e RUN=TestName'`
- Do NOT run `make e2e` directly on macOS — tests will all skip.
- Set `LOG_LEVEL=debug` to enable slog debug output in e2e tests

## Utilities

- **Always use `util.SafeClose(c, msg)`** from `internal/util` when discarding a `Close()` error — both in `defer` and in error-cleanup paths. Never write bare `c.Close()` or `_ = c.Close()`. Example: `defer util.SafeClose(f, "close file")` or in an error path: `util.SafeClose(db, "close db on init failure")`

## Running locally

Pass the backing store as a positional URL argument on commands that need one, for example `https://storage.googleapis.com/<bucket>/<prefix>`. Authentication comes from the platform SDK defaults and standard env vars. Use `--log-level` or `LOOPHOLE_LOG_LEVEL` for daemon verbosity.

- `make loophole` — build macOS binary to `bin/loophole-darwin-arm64`
- `bin/loophole-darwin-arm64 create <store-url> myvol` — create, mount, and keep the owner process in the foreground
- `bin/loophole-darwin-arm64 status <store-url> myvol` — check the owner process for a mounted/attached volume
- `bin/loophole-darwin-arm64 shutdown <store-url> myvol` — gracefully stop the owner process

### Daemon log file

Each per-volume owner process logs to `~/.loophole/volumes/<hash>.log` (the hash is derived from the volset UUID plus volume name, matching the socket path pattern). C library output, Go panics, and `net/http` panic recovery messages all appear there. Go's `log` package is also redirected to this file. The exact log path is shown in the daemon's `/status` response.

Set `LOOPHOLE_LOG_LEVEL=debug` or pass `--log-level debug` for debug output.

### Debugging with goroutine dumps

The daemon exposes pprof endpoints on its Unix socket:
```
curl --unix-socket <owner-socket> 'http://localhost/debug/pprof/goroutine?debug=2'
```
Use this to diagnose stuck operations (e.g. freeze blocked on S3 upload).

### Debugging on remote Linux (Fly)

When debugging the daemon on a remote Linux machine (e.g. Fly), these techniques work well:

**Grep structured logs from the owner log file.** The daemon writes JSON-structured logs to the per-volume log path from `/status`. Use `grep "checkpoint"` or other subsystem strings to filter for specific operations. This is much more reliable than trying to read interleaved console output.

**Use `fly ssh console -C` for clean output.** When the daemon runs in the foreground with debug logging, its output floods the terminal. Use a separate SSH connection to run commands with clean stdout:
```
fly ssh console -a loophole-test -C 'bin/loophole-linux-amd64 status https://storage.googleapis.com/<bucket>/<prefix> <volume>'
```

**Goroutine dumps for stuck operations.** When something hangs, grab a goroutine dump via pprof to see exactly where it's blocked:
```
fly ssh console -a loophole-test -C 'curl --unix-socket <owner-socket> "http://localhost/debug/pprof/goroutine?debug=2"'
```
Grep for your subsystem (e.g. `WaitClosed`, `checkpoint`, `grpc`) to find the stuck goroutine. This is how the FUSE Lookup ref leak was diagnosed — the dump showed `WaitClosed` blocked in `sync.Cond.Wait` because `OnForget` (kernel inode eviction) was indefinitely delayed.

**Watch goroutine count in heartbeat.** The daemon logs `goroutines=N` every 5 seconds. A steadily climbing count indicates a goroutine leak. A stable but unexpectedly high count may indicate a stuck operation.

**Note:** Ubuntu 24.04+ has `fusermount3` not `fusermount`. Stale FUSE mounts from a crashed daemon need `umount -l <mountpoint>` followed by `rm -rf <mountpoint>` before restarting.

## Deploying to Cloudflare

- `make cf-demo-bin` — cross-compile linux/amd64 binary (nosqlite nolwext4) to `cf-demo/bin/loophole`
- `make cf-demo-dev-lima` — run `cf-demo` locally inside the `fc` Lima VM with `wrangler dev`
- `make cf-demo-smoke-local` — hit the local Worker at `CF_DEMO_BASE_URL` and verify sandbox create/files/exec/session end to end
- `make cf-demo-smoke-lima` — hit the local `/debug` endpoints in Lima and verify Firecracker `exec` with `echo hello` and `/etc/os-release`
- **NEVER run `pnpm exec wrangler deploy` directly** — it skips the build and deploys stale artifacts. Always use `cd cf-demo && pnpm run deploy` which runs the full build first.
- Deployed URL: `$CF_DEMO_URL`
- **Container rollout is not instant.** After `wrangler deploy`, CF containers use a rolling deploy strategy. Give it a minute or two after deploy. After deploy, use `POST /debug/stop-all` to force new containers.

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

### CF client script

Use `cf-demo/scripts/client.ts` for authenticated requests to the CF worker. It reads `CONTROL_SECRET` from `.dev.vars` automatically:

```bash
cd cf-demo
pnpm dlx tsx scripts/client.ts GET /debug/sandboxd/v1/status --url $CF_DEMO_URL
pnpm dlx tsx scripts/client.ts POST /debug/control/<ctr>/exec --url $CF_DEMO_URL --json '{"cmd":"uname -a"}'
```

### SSH into CF container

```
export SECRET="$(cut -d= -f2- < <(rg '^CONTROL_SECRET=' cf-demo/.dev.vars))"
bin/loophole-darwin-arm64 ssh --url $CF_DEMO_URL --secret "$SECRET" --volume <volume-name>
```

### Hot-patching binaries on live CF containers

Upload and replace binaries without redeploying. Useful for iterating on runsc/loophole/sandboxd:

```bash
export SECRET="$(cut -d= -f2- < <(rg '^CONTROL_SECRET=' cf-demo/.dev.vars))"
export BASE="$CF_DEMO_URL"
export CTR="$(curl -sS -H "X-Control-Secret: $SECRET" "$BASE/debug/container" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')"

# Upload a binary
gzip -1c cf-demo/bin/runsc > /tmp/runsc-hotpatch.gz
curl -sS -X PUT -H "X-Control-Secret: $SECRET" --data-binary @/tmp/runsc-hotpatch.gz "$BASE/debug/control/$CTR/upload?path=/tmp/runsc-hotpatch.gz&mode=0644"

# Execute commands inside the container
curl -sS -X POST -G -H "X-Control-Secret: $SECRET" --data-urlencode 'cmd=gzip -dc /tmp/runsc-hotpatch.gz > /usr/local/bin/runsc; chmod 0755 /usr/local/bin/runsc' "$BASE/debug/control/$CTR/exec"
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

### CF container gVisor/runsc details

- **Platform:** systrap (default) or kvm. Set via `LOOPHOLE_SANDBOXD_RUNSC_PLATFORM` env var in entrypoint.
- **KVM works** on Cloudflare containers — needs `--rootless` flag. Stock upstream runsc with `--rootless --platform=kvm --network=host --ignore-cgroups` works out of the box.
- **Network:** currently `--network=host`. Switching to `--network=sandbox` (isolated netstack) requires setting up veth pair + SNAT ourselves since Cloudflare's firecracker kernel lacks `xt_MASQUERADE` and `xt_comment` modules. Use `-j SNAT --to-source <host-ip>` instead of `-j MASQUERADE`.
- **Rootless mode:** gVisor auto-detects rootless when `euid != 0`. The `--rootless` flag just controls cgroup error tolerance. Our `TestOnlyAllowRunAsCurrentUserWithoutChroot` patches in the gvisor submodule are for running as root without full capabilities (the Cloudflare container case).
- **Zygote naming:** bump the version in `LOOPHOLE_DEFAULT_ZYGOTE` (entrypoint.sh) to force re-bootstrap from the bundled rootfs tar. Old zygotes persist in R2 and may have wrong-arch binaries.
- **Owner socket bind-mount:** the per-volume loophole daemon socket (`Dir.VolumeSocket`) is bind-mounted into the sandbox at `/.loophole/api.sock`. Processes inside the sandbox can `curl --unix-socket /.loophole/api.sock http://localhost/status` (or `/flush`, `/checkpoint`, `/clone`).
