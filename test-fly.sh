#!/usr/bin/env bash
set -euo pipefail

IMAGE="registry.fly.io/loophole-e2e:test"

echo "=== Building test image ==="
docker build --platform linux/amd64 -t "$IMAGE" -f Dockerfile.test .

echo "=== Pushing test image ==="
docker push "$IMAGE"

echo "=== Deploying to Fly ==="
fly deploy -c fly.test.toml --image "$IMAGE"
