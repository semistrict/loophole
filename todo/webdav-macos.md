# WebDAV Endpoint for macOS lwext4 Backend

## Context

On macOS, the daemon can't compile because `daemon/daemon.go` has `//go:build linux` (imports `fuseblockdev`). The lwext4 backend works cross-platform, and on macOS `DefaultMode()` already returns `ModeLwext4FUSE`.

The approach: make the daemon cross-platform by splitting out the linux-specific `fuseblockdev` import. On macOS, use the pure `Lwext4Driver` (in-process lwext4, no kernel mount) and expose files via a per-volume WebDAV HTTP server. macOS users can then access volumes via `mount_webdav` or Finder's Connect to Server.

## Plan

### 1. Split daemon backend construction by platform

**`daemon/daemon.go`** — Remove `//go:build linux`, remove `fuseblockdev` import. Replace the inline backend switch with a call to `newBackend(mode, vm, dir, inst, debug)`.

**New: `daemon/backend_linux.go`** (`//go:build linux`)
- Import `fuseblockdev`, `fsbackend`
- `newBackend()` handles: `ModeFUSE` (fuseblockdev), `ModeNBD`, `ModeTestNBDTCP`, `ModeLwext4FUSE` (fsbackend.NewLwext4FUSE which uses fuselwext4+FUSE)

**New: `daemon/backend_other.go`** (`//go:build !linux`)
- `newBackend()` handles: `ModeLwext4FUSE` → `fsbackend.NewLwext4()` (pure in-process, no FUSE mount)
- Returns error for unsupported modes

### 2. Create WebDAV filesystem adapter

**New: `fsbackend/webdav.go`** (no build tag)

Bridge `fsbackend.FS` → `golang.org/x/net/webdav.FileSystem`.

The `webdav.File` interface requires Read/Write/Seek/Close/Stat/Readdir. The current `fsbackend.File` only has Read/Write/Close.

Rather than extending FS/File interfaces (which would touch all backends), the adapter wraps types:
- `WebDAVFS` struct adapts `fsbackend.FS` → `webdav.FileSystem`
- `webdavFile` wraps `fsbackend.File`, adds:
  - `Seek` — type-assert to `io.Seeker` (lwext4.File and os.File both have it)
  - `Stat` — calls back to `FS.Stat(name)`
  - `Readdir` — returns error (not a directory)
- `webdavDir` — open directory handle, implements `Readdir` via `FS.ReadDir` + `FS.Stat`
- `Rename` — `FS` doesn't have it → read+write+delete for files
- `RemoveAll` — recursive `ReadDir` + `Remove`
- `NewWebDAVHandler(fs FS) http.Handler` — creates `webdav.Handler` with `NewMemLS()`

### 3. WebDAV server manager in daemon

**New: `daemon/webdav.go`** (no build tag)

Per-mountpoint WebDAV HTTP server lifecycle:
- `webdavManager` struct with `map[string]*webdavServer`
- `Start(mountpoint string, fs fsbackend.FS) (string, error)` — listen on `127.0.0.1:0`, serve WebDAV, return URL
- `Stop(mountpoint string)` — graceful shutdown
- `URLs() map[string]string` — snapshot of mountpoint→URL
- `Close()` — shutdown all servers

### 4. Integrate WebDAV into daemon handlers

**`daemon/daemon.go`**:
- Add `webdav *webdavManager` field to `Daemon`
- Initialize in `Start()` (always created — it's lightweight and nil-safe)
- `handleMount`: after `backend.Mount()`, try `backend.FS(mp)` → if that works, `webdav.Start(mp, fs)` → include `webdav_url` in JSON response
- `handleUnmount`: `webdav.Stop(mp)` before `backend.Unmount()`
- `handleClone`: same for clone mountpoint
- `handleStatus`: include `"webdav": webdav.URLs()` in status response
- `Serve` shutdown: `webdav.Close()`

### 5. Move shared CLI commands to cross-platform file

**New: `cmd/loophole/commands.go`** (no build tag) — Move from `commands_linux.go`:
- All command funcs: `stopCmd`, `createCmd`, `lsCmd`, `mountCmd`, `unmountCmd`, `snapshotCmd`, `cloneCmd`, `statusCmd`, `deviceCmd` + device sub-commands
- All helpers: `resolveClientArgs`, `socketFromMountpoint`, `detectMountpoint`

**`cmd/loophole/commands_linux.go`** — Keep only:
- `addPlatformCommands` (adds device subcommand + the standard commands)
- `startDaemon` (calls `daemon.Start`)
- `startDaemonBackground` (calls `client.EnsureDaemon`)

**`cmd/loophole/commands_darwin.go`** — Replace stubs:
- `addPlatformCommands` — add same commands as linux (stop, create, ls, mount, unmount, snapshot, clone, status) but NOT device subcommand
- `startDaemon` — call `daemon.Start` (now works since daemon is cross-platform)
- `startDaemonBackground` — call `client.EnsureDaemon` with `Sudo: false`

### 6. Update mount command to show WebDAV URL

**`cmd/loophole/commands.go`** (the shared `mountCmd`):
- After mount succeeds, parse response for `webdav_url`
- Print: `mounted vol1 — WebDAV: http://localhost:12345/`

**`client/client.go`**:
- Change `Mount` to return `(*MountResponse, error)` with `WebDAVURL string` field
- Or simpler: keep `Mount` returning error, add separate `MountResult` method

### 7. Update `device_darwin.go`

Remove macFUSE requirement:
```go
func DefaultMode() Mode { return ModeLwext4FUSE }
```

### 8. `go.mod`

Move `golang.org/x/net` from indirect to direct (we now import `golang.org/x/net/webdav`).

## Files to modify
- `daemon/daemon.go` — remove linux build tag, extract backend init to `newBackend()`, add webdav field + lifecycle
- `cmd/loophole/commands_linux.go` — keep only platform-specific functions
- `cmd/loophole/commands_darwin.go` — real implementations
- `client/client.go` — return webdav URL from mount
- `device_darwin.go` — remove macFUSE check
- `go.mod` — promote golang.org/x/net

## New files
- `daemon/backend_linux.go` — linux backend factory
- `daemon/backend_other.go` — non-linux backend factory
- `daemon/webdav.go` — per-volume WebDAV server manager
- `fsbackend/webdav.go` — WebDAV ↔ FS adapter
- `cmd/loophole/commands.go` — shared CLI commands (extracted from commands_linux.go)

## Verification
1. `go build ./cmd/loophole` on macOS — should compile
2. `go build ./...` on Linux (in Docker) — should still compile
3. On macOS with minio running: `loophole start s3://bucket/prefix`
4. `loophole create vol1 && loophole mount vol1 /vol1` → prints WebDAV URL
5. `curl -X PROPFIND http://localhost:PORT/` → returns directory listing XML
6. E2E tests: `go test ./e2e/...` still pass in Docker
