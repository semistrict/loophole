#!/usr/bin/env bash
set -euo pipefail

# Build linux/amd64 binaries via Docker, then push to Fly machine via sftp.
# Usage: ./push-fly.sh [binary...]
# Default: pushes both loophole and e2e.test

BUILDER_IMAGE="loophole-builder"
APP="loophole-e2e"
REMOTE_DIR="/usr/local/bin"

echo "=== Building linux/amd64 binaries ==="
docker build --platform linux/amd64 -t "$BUILDER_IMAGE" -f Dockerfile.test --target builder .

echo "=== Extracting binaries ==="
CONTAINER=$(docker create --platform linux/amd64 "$BUILDER_IMAGE")
trap "docker rm $CONTAINER >/dev/null" EXIT

mkdir -p .tmp-fly-bin
docker cp "$CONTAINER:/app/loophole" .tmp-fly-bin/loophole
docker cp "$CONTAINER:/app/e2e.test" .tmp-fly-bin/e2e.test

echo "=== Uploading to Fly ($APP) ==="
fly ssh console -a "$APP" -C "rm -f $REMOTE_DIR/loophole $REMOTE_DIR/e2e.test"
echo "put .tmp-fly-bin/loophole $REMOTE_DIR/loophole
put .tmp-fly-bin/e2e.test $REMOTE_DIR/e2e.test" | fly ssh sftp shell -a "$APP"
fly ssh console -a "$APP" -C "chmod +x $REMOTE_DIR/loophole $REMOTE_DIR/e2e.test"

rm -rf .tmp-fly-bin

echo "=== Done ==="
