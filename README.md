# Loophole

S3-backed virtual block device with ext4-over-FUSE mounts, checkpoints, and clones.

## Overview

Loophole exposes sparse ext4 volumes backed by an S3-compatible object store.
On Linux, volumes are mounted through a FUSE block device and then mounted by
the kernel as ext4. Clones are instant copy-on-write branches. Checkpoints
capture immutable restore points that clones can be created from later.

## Quick Start

```bash
# Build
make loophole

# Create and mount a volume
bin/loophole-darwin-arm64 -p r2 create myvolume
sudo bin/loophole-darwin-arm64 -p r2 mount myvolume /mnt/myvolume

# Use it like a normal filesystem
echo "hello" | sudo tee /mnt/myvolume/greeting.txt

# Create a checkpoint
bin/loophole-darwin-arm64 -p r2 checkpoint /mnt/myvolume

# List checkpoints
bin/loophole-darwin-arm64 -p r2 checkpoints myvolume

# Clone either a live mount or a checkpoint
bin/loophole-darwin-arm64 -p r2 clone /mnt/myvolume myclone /mnt/myclone
bin/loophole-darwin-arm64 -p r2 clone --from-checkpoint <checkpoint_id> myvolume myclone2 /mnt/myclone2
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
