#!/bin/bash
set -euo pipefail

export PATH=/app/bin:$PATH

# Setup
mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
mkdir -p /etc/containers /usr/libexec/podman
echo '{"default":[{"type":"insecureAcceptAnything"}]}' > /etc/containers/policy.json
ln -sf /bin/true /usr/libexec/podman/netavark

PODMAN="podman --storage-driver loophole --storage-opt loophole.url=s3://testbucket"
RUN_OPTS="--rm --network host --security-opt seccomp=unconfined --cgroups=disabled" # needed to run in Docker

echo "=== Pull ubuntu:24.04 ==="
$PODMAN pull docker.io/library/ubuntu:24.04

echo "=== Run echo ==="
$PODMAN run $RUN_OPTS docker.io/library/ubuntu:24.04 echo "hello from loophole"

echo "=== Run cat /etc/os-release ==="
$PODMAN run $RUN_OPTS docker.io/library/ubuntu:24.04 cat /etc/os-release

echo "=== Run write + read ==="
$PODMAN run $RUN_OPTS docker.io/library/ubuntu:24.04 bash -c 'echo "test data" > /tmp/test.txt && cat /tmp/test.txt'

echo "=== Run ls / ==="
$PODMAN run $RUN_OPTS docker.io/library/ubuntu:24.04 ls /

echo "=== List images ==="
$PODMAN images

echo "=== Image layer contents ==="
while IFS=' ' read -r image mountpoint; do
    echo "--- $image ---"
    echo "mountpoint: $mountpoint"
    ls "$mountpoint"
    echo ""
    echo "usr/bin (first 20):"
    ls "$mountpoint/usr/bin" | head -20
    echo "..."
    echo ""
done < <($PODMAN image mount -a)

echo "=== All tests passed ==="
