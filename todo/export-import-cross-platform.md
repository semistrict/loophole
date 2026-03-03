# Plan: `loophole export` and `loophole import` + cross-platform daemon

## Context
Add export/import commands and make the daemon cross-platform (macOS + Linux). The daemon is currently gated behind `//go:build linux` but most of it is already portable. The lwext4 and lwext4fuse backends are cross-platform. Only FUSE blockdev and NBD backends are Linux-only.

## Part 1: Make daemon cross-platform

### Problem
`daemon/daemon.go` has `//go:build linux` and imports Linux-only `fuseblockdev`. The CLI `commands_linux.go` has all daemon commands gated behind `//go:build linux`.

### Approach
Extract the backend-selection switch (which references Linux-only packages) into platform-specific files. Everything else in daemon.go is already portable.

### Files

| File | Action |
|------|--------|
| `daemon/daemon.go` | Remove `//go:build linux`. Replace backend switch with call to `initBackend(mode, dir, vm, debug)`. Remove `fuseblockdev` import. |
| `daemon/backend_linux.go` | **New** `//go:build linux` — `initBackend()` handles all modes: FUSE, NBD, TestNBDTCP, Lwext4FUSE, Lwext4 (default: FUSE). Imports `fuseblockdev`. |
| `daemon/backend_other.go` | **New** `//go:build !linux` — `initBackend()` handles Lwext4FUSE and Lwext4 only. Returns error for FUSE/NBD modes. |
| `daemon/tune_other.go` | **New** `//go:build !linux` — no-op `tuneProcess(log)` |
| `cmd/loophole/commands.go` | **Rename** from `commands_linux.go`, remove `//go:build linux` tag |
| `cmd/loophole/commands_darwin.go` | **Delete** |
| `cmd/loophole/commands_stub.go` | **Delete** |

### `initBackend` signature
```go
func initBackend(mode loophole.Mode, dir loophole.Dir, inst loophole.Instance, vm *loophole.VolumeManager, debug bool) (fsbackend.Service, error)
```

## Part 2: Export/Import

### Design
- Always tgz over the wire between client and daemon
- Pure Go `archive/tar` + `compress/gzip`
- Archive operations go through `fsbackend.FS` interface (works for all backends: lwext4, lwext4fuse, FUSE, NBD)
- CLI detects tgz vs dir from path suffix, handles conversion locally

### Files

| File | Action |
|------|--------|
| `archive/archive.go` | **New** — `WriteTarGz(fsys fsbackend.FS, w io.Writer)` and `ExtractTarGz(r io.Reader, fsys fsbackend.FS)` |
| `client/client.go` | Add `Export(ctx, mp, w)` and `Import(ctx, mp, r)` streaming methods |
| `daemon/daemon.go` | Add `GET /export` and `POST /import` routes + handlers |
| `cmd/loophole/commands.go` | Add `exportCmd()` and `importCmd()` to `addPlatformCommands` |

### `archive/archive.go`
- `WriteTarGz(fsys fsbackend.FS, w io.Writer) error` — recursive walk via `fsys.ReadDir`/`fsys.Stat`/`fsys.Open`. Gzip writer -> tar writer. Relative paths. Handles regular files and directories. Preserves permissions from `fs.FileInfo`.
- `ExtractTarGz(r io.Reader, fsys fsbackend.FS) error` — gzip reader -> tar reader. Iterate headers. `fsys.MkdirAll` for dirs, `fsys.Create` for files. Path traversal protection (reject `..` and absolute paths).
- No symlink support (not in `fsbackend.FS` interface — fine for now).

### `client/client.go`
- `Export(ctx, mountpoint, w io.Writer)` — `GET /export?mountpoint=<url.QueryEscape>`, check status code, `io.Copy(w, resp.Body)`. Add `"net/url"` import.
- `Import(ctx, mountpoint, r io.Reader)` — `POST /import?mountpoint=<url.QueryEscape>`, body=r, `Content-Type: application/gzip`. Read JSON response for error check.
- Both bypass `c.rpc()` for binary streaming.

### `daemon/daemon.go` handlers
- Register `mux.HandleFunc("GET /export", d.handleExport)` and `mux.HandleFunc("POST /import", d.handleImport)`
- `handleExport` — validate `IsMounted(mp)`, get `backend.FS(mp)`, set `Content-Type: application/gzip`, call `archive.WriteTarGz(fs, w)`. Mid-stream errors logged server-side.
- `handleImport` — validate `IsMounted(mp)`, get `backend.FS(mp)`, call `archive.ExtractTarGz(r.Body, fs)`, return JSON status.

### CLI commands (`commands.go`)
- `exportCmd`: `loophole export <mountpoint> <path>` (ExactArgs(2), `socketFromMountpoint`)
  - `.tar.gz`/`.tgz`: `os.Create(path)`, `c.Export(ctx, mp, file)`
  - Directory: `os.MkdirAll`, `io.Pipe`, goroutine: `c.Export(ctx, mp, pw)` + `pw.CloseWithError`, main: local `extractTarGzToDir(pr, path)` using `archive/tar`+`compress/gzip`+`os`
- `importCmd`: `loophole import <mountpoint> <path>` (ExactArgs(2), `socketFromMountpoint`)
  - `.tar.gz`/`.tgz`: `os.Open(path)`, `c.Import(ctx, mp, file)`
  - Directory: `io.Pipe`, goroutine: local `createTarGzFromDir(path, pw)` + `pw.CloseWithError`, main: `c.Import(ctx, mp, pr)`
- `isTarGz(path) bool` — `strings.HasSuffix` for `.tar.gz` or `.tgz`
- Local `extractTarGzToDir`/`createTarGzFromDir` helpers use OS paths directly (client-side, always a real local filesystem)

## Verification
- `go build ./...` on macOS — confirms cross-platform compilation
- `go vet ./...`
- Docker e2e: create+mount, write files, export to tgz, export to dir, import tgz, import dir, verify contents
