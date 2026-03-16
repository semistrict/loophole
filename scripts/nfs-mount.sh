#!/bin/bash
set -euo pipefail

VOLUME="${1:-nfsvol}"
SIZE="${2:-1GB}"
MOUNTPOINT="/tmp/loophole-nfs"

echo "==> Starting containers"
docker compose up -d s3 go

echo "==> Building loophole in container"
docker compose exec -T go make loophole GOOS=linux 2>&1 | tail -1

echo "==> Mounting volume '$VOLUME' (create if needed, size=${SIZE})"
# Try create first; if it already exists, use mount instead.
docker compose exec -T -d go bash -c "
    bin/loophole-linux-* create --size $SIZE $VOLUME 2>&1 || \
    bin/loophole-linux-* mount $VOLUME 2>&1
"

echo "    waiting for mount..."
for i in $(seq 1 30); do
    if docker compose exec -T go mount 2>/dev/null | grep -q "$VOLUME"; then
        echo "    mounted after ${i}s"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "error: volume not mounted after 30s" >&2
        docker compose exec -T go cat /root/.loophole/default.log 2>/dev/null | tail -5
        exit 1
    fi
    sleep 1
done

echo "==> Starting NFS server (mountd on port 20048)"
docker compose exec -T go bash -c '
echo "/app/'"$VOLUME"' *(rw,sync,no_subtree_check,no_root_squash,insecure,fsid=0)" > /etc/exports
rpcbind 2>/dev/null || true
rpc.mountd --port 20048 2>/dev/null || true
rpc.nfsd 1 2>/dev/null || true
exportfs -ra
'

echo "==> Waiting for NFS grace period (5s)"
sleep 5

echo "==> Mounting on macOS at $MOUNTPOINT"
mkdir -p "$MOUNTPOINT"
sudo mount_nfs -o nolocks,vers=3,tcp,port=2049,mountport=20048 localhost:/app/"$VOLUME" "$MOUNTPOINT"

echo "==> Done! Mounted at $MOUNTPOINT"
ls "$MOUNTPOINT"
