#!/bin/bash
set -e

echo "Starting containerd..."
containerd &

echo "Waiting for containerd socket..."
while [ ! -S /run/containerd/containerd.sock ]; do
    sleep 0.1
done
echo "containerd is ready."

echo "Waiting for loophole daemon (start it manually)..."
echo ""
echo "=== Setup instructions ==="
echo "1. Upload the binary:  fly sftp shell -a loophole-test"
echo "   put bin/loophole-linux-amd64 /usr/local/bin/loophole"
echo ""
echo "2. SSH in:  fly ssh console -a loophole-test"
echo ""
echo "3. Configure loophole:  mkdir -p ~/.loophole && cat > ~/.loophole/config.toml"
echo ""
echo "4. Start loophole daemon:"
echo "   loophole -p <profile> start --snapshotter-socket /run/containerd/loophole.sock"
echo ""
echo "5. Test with ctr:"
echo "   ctr images pull --snapshotter loophole docker.io/library/alpine:latest"
echo "   ctr run --snapshotter loophole --rm docker.io/library/alpine:latest test /bin/echo hello"
echo "=========================="

# Keep alive
exec sleep infinity
