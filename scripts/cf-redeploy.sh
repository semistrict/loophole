#!/bin/bash
# cf-redeploy.sh — Destroy the current CF worker and redeploy with a new name.
#
# This forces a completely fresh deployment, clearing all Durable Objects,
# containers, and stale state. Useful when containers are orphaned or the
# image won't roll out.
#
# Steps:
#   1. Delete the current worker
#   2. Bump the name suffix in wrangler.jsonc
#   3. Deploy the new worker
#   4. Push secrets from .dev.vars

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CF_DIR="${SCRIPT_DIR}/../cf-demo"

cd "$CF_DIR"

WRANGLER="wrangler.jsonc"

# Read current worker name.
CURRENT_NAME=$(python3 -c "
import json, re
with open('$WRANGLER') as f:
    text = re.sub(r'//.*', '', f.read())  # strip comments
print(json.loads(text)['name'])
")
echo "Current worker: ${CURRENT_NAME}"

# Compute new name: bump numeric suffix.
BASE=$(echo "$CURRENT_NAME" | sed -E 's/-[0-9]+$//')
NUM=$(echo "$CURRENT_NAME" | sed -E 's/.*-([0-9]+)$/\1/')
if [ "$NUM" = "$CURRENT_NAME" ]; then
    # No numeric suffix — start at 2.
    NUM=1
fi
NEW_NUM=$((NUM + 1))
NEW_NAME="${BASE}-${NEW_NUM}"
echo "New worker: ${NEW_NAME}"

# Step 1: Delete current worker.
echo "=== Deleting worker ${CURRENT_NAME} ==="
pnpm exec wrangler delete --name "${CURRENT_NAME}" --force || true

# Step 2: Update wrangler.jsonc.
echo "=== Updating ${WRANGLER}: ${CURRENT_NAME} → ${NEW_NAME} ==="
python3 -c "
import re
with open('$WRANGLER') as f:
    text = f.read()
text = text.replace('\"${CURRENT_NAME}\"', '\"${NEW_NAME}\"', 1)
with open('$WRANGLER', 'w') as f:
    f.write(text)
"

# Step 3: Deploy.
echo "=== Deploying ${NEW_NAME} ==="
pnpm run deploy

# Step 4: Push secrets from .dev.vars.
echo "=== Pushing secrets ==="
if [ ! -f .dev.vars ]; then
    echo "ERROR: .dev.vars not found" >&2
    exit 1
fi

# Parse KEY=VALUE lines (skip comments and blank lines), push each as a secret.
while IFS='=' read -r key value; do
    # Skip empty lines and comments.
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    # Strip surrounding whitespace.
    key=$(echo "$key" | xargs)
    value=$(echo "$value" | xargs)
    [ -z "$value" ] && continue
    echo "  ${key}"
    echo "$value" | pnpm exec wrangler secret put "$key" --name "${NEW_NAME}" 2>&1 | tail -1
done < .dev.vars

echo ""
echo "=== Done ==="
echo "Deployed: https://${NEW_NAME}.<your-subdomain>.workers.dev"
echo ""
echo "Update BASE_URL in boot/stage scripts if needed."
