# Loophole

S3-backed virtual block device with instant copy-on-write snapshots and clones.

## Overview

Loophole exposes volumes as block devices backed by an S3-compatible object store.
Each volume is a sparse 100 GB ext4 filesystem. Snapshots and clones are instant
(copy-on-write at the block level).

On Linux, volumes are served via the kernel NBD driver and appear as `/dev/nbdN`
devices. On macOS (or any platform), volumes can be served over TCP using the NBD
protocol — useful for backing virtual machines.

## Quick start (Linux)

```bash
# Build
./deps.sh setup
go build -o loophole ./cmd/loophole

# Start the daemon
loophole start s3://mybucket

# Create a volume and mount it
loophole create myvolume
loophole mount myvolume /mnt/myvolume

# Use it like a normal filesystem
echo "hello" > /mnt/myvolume/greeting.txt

# Snapshot and clone
loophole snapshot myvolume myvolume-snap
loophole clone myvolume-snap myclone
loophole mount myclone /mnt/myclone
```

## Running a macOS VM with a loophole root disk

On macOS, loophole serves volumes over NBD to back virtual machines using
[vfkit](https://github.com/crc-org/vfkit). The VM's root filesystem lives
entirely in S3 — no local disk images needed.

### 1. Build loophole

```bash
./deps.sh setup
go build -o loophole ./cmd/loophole
```

### 2. Start the loophole NBD server

```bash
loophole start s3://mybucket --nbd 127.0.0.1:10809
```

This serves all volumes in the bucket as named NBD exports on `localhost:10809`.
Each volume is accessible by name (e.g. `nbd://127.0.0.1:10809/myvm`).

### 3. Prepare the VM volume

The volume is a bare ext4 filesystem (no partition table). We use a Docker image
as the rootfs source — this avoids needing debootstrap or a separate Linux machine.

#### Build a Docker image with a kernel

Docker images don't include a kernel (containers share the host kernel), so we
install one explicitly:

```dockerfile
# Dockerfile.vm
FROM debian:bookworm
RUN apt-get update && apt-get install -y \
    linux-image-arm64 \
    systemd-sysv \
    && rm -rf /var/lib/apt/lists/*
```

```bash
docker build -f Dockerfile.vm -t loophole-vm .
```

#### Export the rootfs and extract the kernel

```bash
# Export the Docker image as a rootfs tarball
docker create --name loophole-vm-tmp loophole-vm
docker export loophole-vm-tmp -o rootfs.tar
docker rm loophole-vm-tmp

# Extract kernel and initrd to the macOS host
tar xf rootfs.tar boot/vmlinuz-* boot/initrd.img-*
cp boot/vmlinuz-* vmlinuz
cp boot/initrd.img-* initrd.img
```

#### Write the rootfs into a loophole volume

This step requires Linux (to mount the ext4 volume). Run it inside the Docker
container from `docker-compose.yml`:

```bash
docker compose run --rm go bash -c '
  loophole create myvm
  loophole mount myvm /mnt/myvm
  tar xf /app/rootfs.tar -C /mnt/myvm
  sync
  loophole unmount myvm
'
```

### 4. Start the VM with vfkit

Direct kernel boot — the kernel and initrd live on the macOS host, the root
filesystem is the loophole volume served over NBD:

```bash
vfkit \
  --cpus 4 --memory 4096 \
  --bootloader linux,kernel=vmlinuz,initrd=initrd.img,cmdline="\"console=hvc0 root=/dev/vda rw\"" \
  --device nbd,uri=nbd://127.0.0.1:10809/myvm,deviceId=root \
  --device virtio-net,nat \
  --device virtio-serial,stdio
```

The `root=/dev/vda` kernel parameter tells Linux to use the first virtio block
device as the root filesystem, which is the loophole NBD volume.

### 5. Snapshot a running VM

Since the volume lives in S3, you can snapshot it from any machine with access
to the same bucket:

```bash
# On the Linux admin machine:
loophole snapshot myvm myvm-2025-02-25

# Clone from the snapshot to spin up another VM:
loophole clone myvm-2025-02-25 myvm-dev
```

Then point a second vfkit instance at `nbd://127.0.0.1:10809/myvm-dev`.

## NBD server options

```
--nbd ADDRESS    Serve volumes over NBD on this address

# TCP (default port 10809):
--nbd 127.0.0.1:10809
--nbd 10809              # shorthand for :10809

# Unix socket:
--nbd /tmp/loophole.sock
```

## Development

### Third-party dependencies

```bash
./deps.sh setup            # Clone deps at pinned SHAs and apply patches
./deps.sh genpatch lwext4  # Regenerate a patch after editing third_party/lwext4/
./deps.sh reset            # Reset all patched deps to upstream
./deps.sh repatch          # Reset + re-apply patches
```

### Static checks and formatting

```bash
make check    # Run golangci-lint (vet, errcheck, staticcheck, etc.)
make fmt      # Format all Go source files
```

### Running tests

```bash
# All tests (unit + all e2e modes, requires Docker)
docker compose run --rm go make e2e

# Individual e2e modes
docker compose run --rm go make e2e-fuse         # FUSE + losetup + kernel ext4
docker compose run --rm go make e2e-nbd          # NBD + kernel ext4
docker compose run --rm go make e2e-testnbdtcp   # NBD over TCP + kernel ext4

# Unit tests only
docker compose run --rm go make test
```
