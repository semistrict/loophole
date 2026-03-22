# Loophole

> [!WARNING]
> This software is unstable and not safe for real data. If you use it, assume you will lose data.

S3-backed virtual block device with ext4-over-FUSE mounts, checkpoints, and clones.

## Overview

Loophole exposes sparse ext4 volumes backed by an S3-compatible object store.
On Linux, volumes are mounted through a FUSE block device and then mounted by
the kernel as ext4. Clones are instant copy-on-write branches. Checkpoints
capture immutable restore points that clones can be created from later.

## Quick Start

```bash
# Install
go install ./cmd/loophole

STORE_URL=https://storage.googleapis.com/my-bucket/my-prefix

# Format the backing store once
loophole format "$STORE_URL"

# Create and mount a volume
loophole create "$STORE_URL" myvolume
sudo loophole mount "$STORE_URL" myvolume /mnt/myvolume

# Use it like a normal filesystem
echo "hello" | sudo tee /mnt/myvolume/greeting.txt

# Create a checkpoint
loophole checkpoint /mnt/myvolume

# List checkpoints
loophole checkpoints /mnt/myvolume

# Clone either a live mount or a checkpoint
loophole clone /mnt/myvolume myclone
loophole clone --from-checkpoint <checkpoint_id> "$STORE_URL" myvolume myclone2
sudo loophole mount "$STORE_URL" myclone /mnt/myclone
sudo loophole mount "$STORE_URL" myclone2 /mnt/myclone2
```

## Development

```bash
make build
make test
docker compose run --rm go bash -c 'make e2e'
```

## Notes

- The supported filesystem path is ext4 via kernel mount over the FUSE block device backend.
- Checkpoints replace the old public snapshot command surface.
- `create`, `mount`, and `device attach` run a single-volume owner process in the foreground.
