#!/usr/bin/env bash
set -euo pipefail

APP="loophole-e2e"
REGION="sin"

echo "=== Launching test machine in $REGION ==="
fly machine run . \
    -a "$APP" \
    -r "$REGION" \
    --dockerfile Dockerfile.test \
    --vm-cpus 4 \
    --vm-memory 8192 \
    --rootfs-size 20 \
    --rm \
    -e S3_ENDPOINT=https://fly.storage.tigris.dev \
    -e LOOPHOLE_S3_URL=s3://loophole-e2e
