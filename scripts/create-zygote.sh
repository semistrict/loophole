#!/usr/bin/env bash
set -euo pipefail

# Creates a zygote volume in R2 by exporting an Ubuntu 24.04 rootfs
# and extracting it into a loophole volume via docker compose.
#
# Usage: scripts/create-zygote.sh [zygote-name]
# Default zygote name: ubuntu-2404-v4

ZYGOTE_NAME="${1:-ubuntu-2404-v4}"
VOLUME="zygote-${ZYGOTE_NAME}"
PROFILE="${LOOPHOLE_PROFILE:-r2}"
PLATFORM="${PLATFORM:-linux/amd64}"
MOUNTPOINT="/tmp/zygote-bootstrap"

echo "==> Creating Ubuntu 24.04 container (${PLATFORM})..."
CID=$(docker create --platform "${PLATFORM}" ubuntu:24.04 /bin/true)
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT

echo "==> Exporting rootfs and creating zygote volume '${VOLUME}' in profile '${PROFILE}'..."
docker export "$CID" | docker compose run --rm -i -T go bash -c '
set -euo pipefail

BIN=bin/loophole-linux-$(uname -m | sed "s/x86_64/amd64/" | sed "s/aarch64/arm64/")
PROFILE='"${PROFILE}"'
VOLUME='"${VOLUME}"'
MOUNTPOINT='"${MOUNTPOINT}"'

# Save stdin (the tar stream) since the daemon needs stdin.
TARFILE=$(mktemp /tmp/rootfs-XXXXXX.tar)
trap "rm -f $TARFILE" EXIT
cat > "$TARFILE"
echo "==> Rootfs tar saved ($(du -sh "$TARFILE" | cut -f1))"

# Delete volume if it already exists (idempotent).
"$BIN" -p "$PROFILE" delete -y "$VOLUME" 2>/dev/null || true

# Clean up stale sockets.
rm -f /root/.loophole/volumes/*.sock

# Create volume — daemon runs in background.
mkdir -p "$MOUNTPOINT"
"$BIN" -p "$PROFILE" create --mount "$MOUNTPOINT" "$VOLUME" &
DAEMON_PID=$!

# Wait for daemon to be ready.
for i in $(seq 1 60); do
    if "$BIN" -p "$PROFILE" status "$VOLUME" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

if ! "$BIN" -p "$PROFILE" status "$VOLUME" >/dev/null 2>&1; then
    echo "ERROR: daemon did not become ready"
    exit 1
fi
echo "==> Daemon ready, extracting rootfs..."

# Extract rootfs.
tar -xf "$TARFILE" -C "$MOUNTPOINT"
rm -f "$TARFILE"

# Prepare standard directories.
for d in proc sys dev run tmp var/tmp; do
    mkdir -p "$MOUNTPOINT/$d"
done
chmod 1777 "$MOUNTPOINT/tmp" "$MOUNTPOINT/var/tmp"

echo "==> Rootfs extracted, creating checkpoint..."

# Checkpoint and shutdown.
"$BIN" -p "$PROFILE" checkpoint "$MOUNTPOINT"
"$BIN" -p "$PROFILE" shutdown "$VOLUME"
wait $DAEMON_PID 2>/dev/null || true

echo "==> Zygote created successfully."
'
