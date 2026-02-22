#!/usr/bin/env bash
set -euo pipefail

LOCKFILE=".test-e2e.lock"

# Check for an existing run.
if [ -f "$LOCKFILE" ]; then
    existing_pid=$(cat "$LOCKFILE")
    if kill -0 "$existing_pid" 2>/dev/null; then
        echo "Another run is already in progress (pid $existing_pid)"
        exit 1
    fi
    echo "Stale lock file (pid $existing_pid not running), continuing..."
    rm -f "$LOCKFILE"
fi

echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

echo "=== Building Docker image ==="
docker compose build

echo "=== Starting services ==="
docker compose up -d

# Slow test files get their own shard; fast ones are grouped together.
slow=(tests/test_stress.py tests/test_fallocate.py)
fast=()
for f in tests/test_*.py; do
    skip=false
    for s in "${slow[@]}"; do
        [[ "$f" == "$s" ]] && skip=true
    done
    $skip || fast+=("$f")
done

# Each slow file = its own shard, all fast files = one shard.
declare -a groups
for s in "${slow[@]}"; do
    groups+=("$s")
done
groups+=("${fast[*]}")

SHARDS=${#groups[@]}

# Clean up stale mounts, loop devices, and cache dirs before launching shards.
docker compose exec e2e bash -c '
    umount /mnt/ext4* /mnt/loophole* 2>/dev/null
    losetup -D 2>/dev/null
    rm -rf /tmp/loophole-cache*
    true
'

echo "=== Running tests across $SHARDS shards (same container, separate buckets) ==="
for i in "${!groups[@]}"; do
    echo "  shard $i: ${groups[$i]}"
done

# Launch each shard as a background pytest inside the single e2e container.
pids=()
for i in $(seq 0 $((SHARDS - 1))); do
    bucket="testbucket-${i}"
    files=(${groups[$i]})

    (
        docker compose exec -e "BUCKET=$bucket" -e "SHARD=-$i" -w /tests e2e \
            pytest "${files[@]/#//}" -v -x 2>&1 \
            | sed "s/^/[shard $i] /"
    ) &
    pids+=($!)
done

# Wait for all shards and collect exit codes.
failed=0
for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
        failed=1
    fi
done

if [ "$failed" -ne 0 ]; then
    echo "=== SOME SHARDS FAILED ==="
    exit 1
fi

echo "=== ALL SHARDS PASSED ==="
